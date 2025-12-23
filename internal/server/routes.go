package server

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
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
	s.App.Get("/api/key/:key/all", s.GetKeyForAllDbHandler)
	s.App.Get("/api/stats", s.StatsHandler)
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
	httpClient := &http.Client{Timeout: 2 * time.Second}

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
	keys, err := client.Keys(ctx, "*").Result()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"keys": keys, "db_index": idx})
}

func (s *FiberServer) OperationHandler(c *fiber.Ctx) error {
	ctx := context.Background()
	operation := c.Params("operation")
	key := c.Params("key")
	numberStr := c.Params("number")

	number, err := strconv.ParseInt(numberStr, 10, 64)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid number"})
	}

	// Validate operation
	validOps := map[string]bool{"incrby": true, "decrby": true, "expire": true, "pexpire": true}
	opLower := strings.ToLower(operation)
	if !validOps[opLower] {
		return c.Status(400).JSON(fiber.Map{"error": "invalid operation. Use: incrby, decrby, expire, pexpire"})
	}

	// Check for X-DB-All header to update all databases in parallel
	if c.Get("X-DB-All") != "" {
		return s.operationAll(c, ctx, opLower, key, number)
	}

	client, idx := s.getDB(c)
	result, err := s.executeOperation(ctx, client, opLower, key, number)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{"key": key, "operation": opLower, "result": result, "db_index": idx})
}

func (s *FiberServer) executeOperation(ctx context.Context, client *redis.Client, op, key string, number int64) (interface{}, error) {
	switch op {
	case "incrby":
		return client.IncrBy(ctx, key, number).Result()
	case "decrby":
		return client.DecrBy(ctx, key, number).Result()
	case "expire":
		return client.Expire(ctx, key, time.Duration(number)*time.Second).Result()
	case "pexpire":
		return client.PExpire(ctx, key, time.Duration(number)*time.Millisecond).Result()
	default:
		return nil, fmt.Errorf("unknown operation: %s", op)
	}
}

func (s *FiberServer) operationAll(c *fiber.Ctx, ctx context.Context, op, key string, number int64) error {
	type result struct {
		idx   int
		addr  string
		value interface{}
		err   error
	}

	resultsChan := make(chan result, len(s.dbs))

	for i, client := range s.dbs {
		go func(idx int, cli *redis.Client) {
			val, err := s.executeOperation(ctx, cli, op, key, number)
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
	Operation string `json:"operation"`
	Key       string `json:"key"`
	Value     int64  `json:"value"`
	Workers   int    `json:"workers"`
	Duration  int    `json:"duration"` // seconds
	DbIndex   int    `json:"db_index"`
	AllDbs    bool   `json:"all_dbs"`
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
	result := s.runLoadTest(req)

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
					_, e := s.executeOperation(ctx, client, req.Operation, req.Key, req.Value)
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
				_, err = s.executeOperation(ctx, s.dbs[dbIdx], req.Operation, req.Key, req.Value)
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

	val, err := client.Get(ctx, key).Result()
	if err == redis.Nil {
		return c.Status(404).JSON(fiber.Map{"error": "key not found", "db_index": idx})
	}
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{"key": key, "value": val, "db_index": idx})
}
