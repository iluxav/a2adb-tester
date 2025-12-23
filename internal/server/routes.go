package server

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/redis/go-redis/v9"
)

//go:embed static/index.html
var staticFS embed.FS

func (s *FiberServer) RegisterFiberRoutes() {
	// Apply CORS middleware
	s.App.Use(cors.New(cors.Config{
		AllowOrigins:     "*",
		AllowMethods:     "GET,POST,PUT,DELETE,OPTIONS,PATCH",
		AllowHeaders:     "Accept,Authorization,Content-Type,X-DB,X-DB-All",
		AllowCredentials: false, // credentials require explicit origins
		MaxAge:           300,
	}))

	s.App.Get("/", s.indexHandler)
	s.App.Get("/health", s.healthHandler)
	s.App.Get("/api/keys", s.ListKeysHandler)
	s.App.Get("/api/keys/all", s.ListKeysAllHandler)
	s.App.Get("/api/key/:key/all", s.GetKeyForAllDbHandler)
	s.App.Delete("/api/key/:key", s.DeleteKeyHandler)
	s.App.Post("/api/set", s.SetKeyHandler)
	s.App.Get("/api/stats", s.StatsHandler)
	s.App.Get("/api/metrics", s.MetricsHandler)
	s.App.Post("/api/loadtest", s.LoadTestHandler)
	s.App.Get("/:operation/:key/:number", s.OperationHandler)
	s.App.Get("/:key", s.GetKeyHandler)
}

func (s *FiberServer) indexHandler(c *fiber.Ctx) error {
	content, err := staticFS.ReadFile("static/index.html")
	if err != nil {
		return c.Status(500).SendString("Failed to load page")
	}
	c.Set("Content-Type", "text/html")
	return c.Send(content)
}

func (s *FiberServer) GetKeyForAllDbHandler(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	key := c.Params("key")

	type result struct {
		idx   int
		addr  string
		value string
		err   error
	}

	resultsChan := make(chan result, len(s.dbs))

	for i, client := range s.dbs {
		go func(idx int, cli *redis.Client) {
			val, err := cli.Get(ctx, key).Result()
			resultsChan <- result{idx: idx, addr: cli.Options().Addr, value: val, err: err}
		}(i, client)
	}

	results := make([]fiber.Map, len(s.dbs))
	for range s.dbs {
		r := <-resultsChan
		if r.err == redis.Nil {
			results[r.idx] = fiber.Map{"db_index": r.idx, "address": r.addr, "error": "key not found"}
		} else if r.err != nil {
			results[r.idx] = fiber.Map{"db_index": r.idx, "address": r.addr, "error": r.err.Error()}
		} else {
			results[r.idx] = fiber.Map{"db_index": r.idx, "address": r.addr, "value": r.value}
		}
	}

	return c.JSON(fiber.Map{"key": key, "results": results})
}

type SetKeyRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (s *FiberServer) SetKeyHandler(c *fiber.Ctx) error {
	var req SetKeyRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid request body"})
	}

	if req.Key == "" {
		return c.Status(400).JSON(fiber.Map{"error": "key is required"})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Check for X-DB-All header
	if c.Get("X-DB-All") != "" {
		type result struct {
			idx  int
			addr string
			err  error
		}

		resultsChan := make(chan result, len(s.dbs))

		for i, client := range s.dbs {
			go func(idx int, cli *redis.Client) {
				start := time.Now()
				_, err := cli.Set(ctx, req.Key, req.Value, 0).Result()
				RecordRedisCommand(idx, cli.Options().Addr, "set", time.Since(start), err)
				resultsChan <- result{idx: idx, addr: cli.Options().Addr, err: err}
			}(i, client)
		}

		results := make([]fiber.Map, len(s.dbs))
		for range s.dbs {
			r := <-resultsChan
			if r.err != nil {
				results[r.idx] = fiber.Map{"db_index": r.idx, "address": r.addr, "error": r.err.Error()}
			} else {
				results[r.idx] = fiber.Map{"db_index": r.idx, "address": r.addr, "result": "OK"}
			}
		}

		return c.JSON(fiber.Map{"key": req.Key, "operation": "set", "results": results})
	}

	client, idx := s.getDB(c)
	start := time.Now()
	_, err := client.Set(ctx, req.Key, req.Value, 0).Result()
	RecordRedisCommand(idx, client.Options().Addr, "set", time.Since(start), err)

	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{"key": req.Key, "operation": "set", "result": "OK", "db_index": idx})
}

func (s *FiberServer) DeleteKeyHandler(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	key := c.Params("key")

	type result struct {
		idx     int
		addr    string
		deleted int64
		err     error
	}

	resultsChan := make(chan result, len(s.dbs))

	for i, client := range s.dbs {
		go func(idx int, cli *redis.Client) {
			start := time.Now()
			deleted, err := cli.Del(ctx, key).Result()
			RecordRedisCommand(idx, cli.Options().Addr, "del", time.Since(start), err)
			resultsChan <- result{idx: idx, addr: cli.Options().Addr, deleted: deleted, err: err}
		}(i, client)
	}

	results := make([]fiber.Map, len(s.dbs))
	for range s.dbs {
		r := <-resultsChan
		if r.err != nil {
			results[r.idx] = fiber.Map{"db_index": r.idx, "address": r.addr, "error": r.err.Error()}
		} else {
			results[r.idx] = fiber.Map{"db_index": r.idx, "address": r.addr, "deleted": r.deleted}
		}
	}

	return c.JSON(fiber.Map{"key": key, "results": results})
}

// getDB returns a Redis client based on X-DB header or random selection.
// Returns the client and its index.
func (s *FiberServer) getDB(c *fiber.Ctx) (*redis.Client, int) {
	if header := c.Get("X-DB"); header != "" {
		if idx, err := strconv.Atoi(header); err == nil && idx >= 0 && idx < len(s.dbs) {
			return s.dbs[idx], idx
		}
	}
	// Random selection
	idx := rand.Intn(len(s.dbs))
	return s.dbs[idx], idx
}

func (s *FiberServer) healthHandler(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	results := make([]fiber.Map, len(s.dbs))
	for i, client := range s.dbs {
		status := "up"
		pong, err := client.Ping(ctx).Result()
		if err != nil {
			status = "down"
			pong = err.Error()
		}
		results[i] = fiber.Map{
			"index":   i,
			"address": client.Options().Addr,
			"status":  status,
			"ping":    pong,
		}
	}
	return c.JSON(fiber.Map{"databases": results})
}

// DBStats represents the stats from a Redis Prometheus endpoint
type DBStats struct {
	UptimeSeconds int `json:"uptime_seconds"`
	Connections   struct {
		Active int `json:"active"`
		Total  int `json:"total"`
	} `json:"connections"`
	Commands struct {
		Incr   int `json:"incr"`
		Decr   int `json:"decr"`
		Get    int `json:"get"`
		Set    int `json:"set"`
		Mget   int `json:"mget"`
		Expire int `json:"expire"`
		TTL    int `json:"ttl"`
		Keys   int `json:"keys"`
		Ping   int `json:"ping"`
		Info   int `json:"info"`
		Other  int `json:"other"`
	} `json:"commands"`
	LatencyAvgUs struct {
		Incr int `json:"incr"`
		Get  int `json:"get"`
		Set  int `json:"set"`
	} `json:"latency_avg_us"`
	Replication struct {
		DeltasSent     int `json:"deltas_sent"`
		DeltasReceived int `json:"deltas_received"`
		SendErrors     int `json:"send_errors"`
		LatencyAvgMs   int `json:"latency_avg_ms"`
		LatencyMinMs   int `json:"latency_min_ms"`
		LatencyMaxMs   int `json:"latency_max_ms"`
	} `json:"replication"`
	Store struct {
		Keys int `json:"keys"`
	} `json:"store"`
}

func (s *FiberServer) MetricsHandler(c *fiber.Ctx) error {
	addresses := make([]string, len(s.dbs))
	for i, client := range s.dbs {
		addresses[i] = client.Options().Addr
	}
	metrics := GetMetrics(len(s.dbs), addresses)
	return c.JSON(metrics)
}

func (s *FiberServer) StatsHandler(c *fiber.Ctx) error {
	type statsResult struct {
		Index   int      `json:"index"`
		Address string   `json:"address"`
		URL     string   `json:"url"`
		Stats   *DBStats `json:"stats,omitempty"`
		Error   string   `json:"error,omitempty"`
	}

	var wg sync.WaitGroup
	results := make([]statsResult, len(s.statsURLs))
	httpClient := &http.Client{Timeout: 10 * time.Second}

	for i, url := range s.statsURLs {
		wg.Add(1)
		go func(idx int, statsURL string) {
			defer wg.Done()

			result := statsResult{
				Index: idx,
				URL:   statsURL,
			}
			if idx < len(s.dbs) {
				result.Address = s.dbs[idx].Options().Addr
			}

			resp, err := httpClient.Get(statsURL)
			if err != nil {
				result.Error = err.Error()
				results[idx] = result
				return
			}
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				result.Error = err.Error()
				results[idx] = result
				return
			}

			var stats DBStats
			if err := json.Unmarshal(body, &stats); err != nil {
				result.Error = err.Error()
				results[idx] = result
				return
			}

			result.Stats = &stats
			results[idx] = result
		}(i, url)
	}

	wg.Wait()
	return c.JSON(fiber.Map{"stats": results})
}

func (s *FiberServer) ListKeysHandler(c *fiber.Ctx) error {
	ctx := context.Background()
	client, idx := s.getDB(c)

	start := time.Now()
	keys, err := client.Keys(ctx, "*").Result()
	RecordRedisCommand(idx, client.Options().Addr, "keys", time.Since(start), err)

	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"keys": keys, "db_index": idx})
}

func (s *FiberServer) ListKeysAllHandler(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	type dbKeys struct {
		idx  int
		addr string
		keys []string
		err  error
	}

	resultsChan := make(chan dbKeys, len(s.dbs))

	// Fetch keys from all databases in parallel
	for i, client := range s.dbs {
		go func(idx int, cli *redis.Client) {
			keys, err := cli.Keys(ctx, "*").Result()
			resultsChan <- dbKeys{idx: idx, addr: cli.Options().Addr, keys: keys, err: err}
		}(i, client)
	}

	// Collect results and build a map of key -> list of db indices
	keyMap := make(map[string][]int)
	dbAddresses := make(map[int]string)

	for range s.dbs {
		r := <-resultsChan
		dbAddresses[r.idx] = r.addr
		if r.err == nil {
			for _, key := range r.keys {
				keyMap[key] = append(keyMap[key], r.idx)
			}
		}
	}

	// Convert to response format
	type keyInfo struct {
		Key       string `json:"key"`
		Databases []int  `json:"databases"`
	}

	var keys []keyInfo
	for key, dbs := range keyMap {
		keys = append(keys, keyInfo{Key: key, Databases: dbs})
	}

	// Sort keys alphabetically
	for i := 0; i < len(keys)-1; i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i].Key > keys[j].Key {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}

	return c.JSON(fiber.Map{
		"keys":      keys,
		"databases": dbAddresses,
	})
}

func (s *FiberServer) OperationHandler(c *fiber.Ctx) error {
	ctx := context.Background()
	operation := c.Params("operation")
	key := c.Params("key")
	valueStr := c.Params("number") // Can be number or string value

	// URL-decode the value for SET operations
	if decodedKey, err := url.QueryUnescape(key); err == nil {
		key = decodedKey
	}
	if decodedValue, err := url.QueryUnescape(valueStr); err == nil {
		valueStr = decodedValue
	}

	// Validate operation
	validOps := map[string]bool{"incrby": true, "decrby": true, "expire": true, "pexpire": true, "set": true}
	opLower := strings.ToLower(operation)
	if !validOps[opLower] {
		return c.Status(400).JSON(fiber.Map{"error": "invalid operation. Use: incrby, decrby, expire, pexpire, set"})
	}

	// For SET, use string value; for others, parse as int64
	var number int64
	var err error
	if opLower != "set" {
		number, err = strconv.ParseInt(valueStr, 10, 64)
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "invalid number"})
		}
	}

	// Check for X-DB-All header to update all databases in parallel
	if c.Get("X-DB-All") != "" {
		return s.operationAll(c, ctx, opLower, key, number, valueStr)
	}

	client, idx := s.getDB(c)
	result, err := s.executeOperation(ctx, client, opLower, key, number, valueStr)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{"key": key, "operation": opLower, "result": result, "db_index": idx})
}

func (s *FiberServer) executeOperation(ctx context.Context, client *redis.Client, op, key string, number int64, strValue string) (any, error) {
	return s.executeOperationWithMetrics(ctx, client, -1, op, key, number, strValue)
}

func (s *FiberServer) executeOperationWithMetrics(ctx context.Context, client *redis.Client, dbIndex int, op, key string, number int64, strValue string) (any, error) {
	// Find db index if not provided
	if dbIndex < 0 {
		for i, c := range s.dbs {
			if c == client {
				dbIndex = i
				break
			}
		}
	}

	start := time.Now()
	var result any
	var err error

	switch op {
	case "incrby":
		result, err = client.IncrBy(ctx, key, number).Result()
	case "decrby":
		result, err = client.DecrBy(ctx, key, number).Result()
	case "expire":
		result, err = client.Expire(ctx, key, time.Duration(number)*time.Second).Result()
	case "pexpire":
		result, err = client.PExpire(ctx, key, time.Duration(number)*time.Millisecond).Result()
	case "set":
		result, err = client.Set(ctx, key, strValue, 0).Result()
	default:
		return nil, fmt.Errorf("unknown operation: %s", op)
	}

	// Record metrics
	RecordRedisCommand(dbIndex, client.Options().Addr, op, time.Since(start), err)

	return result, err
}

func (s *FiberServer) operationAll(c *fiber.Ctx, ctx context.Context, op, key string, number int64, strValue string) error {
	type result struct {
		idx   int
		addr  string
		value any
		err   error
	}

	resultsChan := make(chan result, len(s.dbs))

	for i, client := range s.dbs {
		go func(idx int, cli *redis.Client) {
			val, err := s.executeOperation(ctx, cli, op, key, number, strValue)
			resultsChan <- result{idx: idx, addr: cli.Options().Addr, value: val, err: err}
		}(i, client)
	}

	results := make([]fiber.Map, len(s.dbs))
	for range s.dbs {
		r := <-resultsChan
		if r.err != nil {
			results[r.idx] = fiber.Map{"db_index": r.idx, "address": r.addr, "error": r.err.Error()}
		} else {
			results[r.idx] = fiber.Map{"db_index": r.idx, "address": r.addr, "result": r.value}
		}
	}

	return c.JSON(fiber.Map{"key": key, "operation": op, "results": results})
}

type LoadTestRequest struct {
	Operation   string `json:"operation"`
	Key         string `json:"key"`
	Value       int64  `json:"value"`
	StringValue string `json:"string_value"`
	Workers     int    `json:"workers"`
	Duration    int    `json:"duration"` // seconds
	DbIndex     int    `json:"db_index"`
	AllDbs      bool   `json:"all_dbs"`
}

type LoadTestResult struct {
	TotalRequests int64   `json:"total_requests"`
	Errors        int64   `json:"errors"`
	Duration      float64 `json:"duration"`
	RPS           float64 `json:"rps"`
	AvgLatency    float64 `json:"avg_latency_ms"`
	MinLatency    float64 `json:"min_latency_ms"`
	MaxLatency    float64 `json:"max_latency_ms"`
}

func (s *FiberServer) LoadTestHandler(c *fiber.Ctx) error {
	var req LoadTestRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid request body"})
	}

	// Validate
	if req.Key == "" {
		return c.Status(400).JSON(fiber.Map{"error": "key is required"})
	}
	if req.Workers < 1 {
		req.Workers = 10
	}
	if req.Workers > 1000 {
		req.Workers = 1000
	}
	if req.Duration < 1 {
		req.Duration = 10
	}
	if req.Duration > 300 {
		req.Duration = 300
	}

	validOps := map[string]bool{"incrby": true, "decrby": true, "expire": true, "pexpire": true}
	if !validOps[req.Operation] {
		req.Operation = "incrby"
	}

	// Run load test
	IncrementActiveLoadTests()
	result := s.runLoadTest(req)
	DecrementActiveLoadTests()

	return c.JSON(result)
}

func (s *FiberServer) runLoadTest(req LoadTestRequest) LoadTestResult {
	var totalRequests, errors int64
	var totalLatency, minLatency, maxLatency float64
	minLatency = float64(^uint64(0) >> 1) // max float

	var mu sync.Mutex
	var wg sync.WaitGroup

	ctx := context.Background()
	startTime := time.Now()
	endTime := startTime.Add(time.Duration(req.Duration) * time.Second)

	// Worker function
	worker := func() {
		defer wg.Done()

		for time.Now().Before(endTime) {
			reqStart := time.Now()

			var err error
			if req.AllDbs {
				// Execute on all DBs
				for _, client := range s.dbs {
					_, e := s.executeOperation(ctx, client, req.Operation, req.Key, req.Value, req.StringValue)
					if e != nil {
						err = e
					}
				}
			} else {
				// Execute on specific DB
				dbIdx := req.DbIndex
				if dbIdx < 0 || dbIdx >= len(s.dbs) {
					dbIdx = 0
				}
				_, err = s.executeOperation(ctx, s.dbs[dbIdx], req.Operation, req.Key, req.Value, req.StringValue)
			}

			latency := float64(time.Since(reqStart).Microseconds()) / 1000.0 // ms

			mu.Lock()
			totalRequests++
			totalLatency += latency
			if latency < minLatency {
				minLatency = latency
			}
			if latency > maxLatency {
				maxLatency = latency
			}
			if err != nil {
				errors++
			}
			mu.Unlock()
		}
	}

	// Start workers
	wg.Add(req.Workers)
	for i := 0; i < req.Workers; i++ {
		go worker()
	}

	wg.Wait()
	duration := time.Since(startTime).Seconds()

	avgLatency := 0.0
	if totalRequests > 0 {
		avgLatency = totalLatency / float64(totalRequests)
	}
	if minLatency == float64(^uint64(0)>>1) {
		minLatency = 0
	}

	return LoadTestResult{
		TotalRequests: totalRequests,
		Errors:        errors,
		Duration:      duration,
		RPS:           float64(totalRequests) / duration,
		AvgLatency:    avgLatency,
		MinLatency:    minLatency,
		MaxLatency:    maxLatency,
	}
}

func (s *FiberServer) GetKeyHandler(c *fiber.Ctx) error {
	ctx := context.Background()
	client, idx := s.getDB(c)
	key := c.Params("key")

	start := time.Now()
	val, err := client.Get(ctx, key).Result()
	RecordRedisCommand(idx, client.Options().Addr, "get", time.Since(start), err)

	if err == redis.Nil {
		return c.Status(404).JSON(fiber.Map{"error": "key not found", "db_index": idx})
	}
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{"key": key, "value": val, "db_index": idx})
}
