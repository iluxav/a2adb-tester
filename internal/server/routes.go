package server

import (
	"context"
	"embed"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"a2adb-tester/internal/config"

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
	s.App.Get("/api/key/*/all", s.GetKeyForAllDbHandler)
	s.App.Delete("/api/key/*", s.DeleteKeyHandler)
	s.App.Post("/api/set", s.SetKeyHandler)
	s.App.Get("/api/stats", s.StatsHandler)
	s.App.Get("/api/cluster", s.ClusterHandler)
	s.App.Get("/api/metrics", s.MetricsHandler)
	s.App.Post("/api/loadtest", s.LoadTestHandler)
	s.App.Get("/api/config", s.GetConfigHandler)
	s.App.Post("/api/config", s.SaveConfigHandler)
	s.App.Post("/api/config/address", s.AddAddressHandler)
	s.App.Delete("/api/config/address/*", s.RemoveAddressHandler)
	s.App.Post("/api/reconnect", s.ReconnectHandler)
	s.App.Get("/api/op", s.OperationHandler)
	s.App.Get("/api/get", s.GetKeyHandler)
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

	key := c.Params("*")
	if decoded, err := url.QueryUnescape(key); err == nil {
		key = decoded
	}
	// Remove "/all" suffix if present
	key = strings.TrimSuffix(key, "/all")

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

	key := c.Params("*")
	if decoded, err := url.QueryUnescape(key); err == nil {
		key = decoded
	}

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
		Total  int `json:"total"`
		Incr   int `json:"incr"`
		Decr   int `json:"decr"`
		Get    int `json:"get"`
		Set    int `json:"set"`
		Ping   int `json:"ping"`
		Info   int `json:"info"`
		Quota  int `json:"quota"`
		Other  int `json:"other"`
	} `json:"commands"`
	Errors struct {
		Parse      int `json:"parse"`
		UnknownCmd int `json:"unknown_cmd"`
	} `json:"errors"`
	LatencyP50Us struct {
		Incr int `json:"incr"`
		Get  int `json:"get"`
		Set  int `json:"set"`
		Decr int `json:"decr"`
	} `json:"latency_p50_us"`
	LatencyP99Us struct {
		Incr int `json:"incr"`
		Get  int `json:"get"`
		Set  int `json:"set"`
		Decr int `json:"decr"`
	} `json:"latency_p99_us"`
	Replication struct {
		DeltasSent     int `json:"deltas_sent"`
		DeltasReceived int `json:"deltas_received"`
		BytesSent      int `json:"bytes_sent"`
		BytesReceived  int `json:"bytes_received"`
		Dropped        int `json:"dropped"`
		LagMax         int `json:"lag_max"`
		Peers          int `json:"peers"`
		RttP50Us       int `json:"rtt_p50_us"`
		RttP99Us       int `json:"rtt_p99_us"`
	} `json:"replication"`
	WAL struct {
		Writes       int `json:"writes"`
		BytesWritten int `json:"bytes_written"`
		Dropped      int `json:"dropped"`
		SyncFailures int `json:"sync_failures"`
	} `json:"wal"`
	Snapshots struct {
		Created int `json:"created"`
	} `json:"snapshots"`
	BufferPool struct {
		Size   int `json:"size"`
		Hits   int `json:"hits"`
		Misses int `json:"misses"`
	} `json:"buffer_pool"`
}

// parsePrometheusMetrics parses Prometheus text format into DBStats
func parsePrometheusMetrics(body string) *DBStats {
	stats := &DBStats{}
	lines := strings.Split(body, "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Parse metric line: metric_name{labels} value or metric_name value
		var metricName, labels string
		var value float64

		if idx := strings.Index(line, "{"); idx != -1 {
			metricName = line[:idx]
			endIdx := strings.Index(line, "}")
			if endIdx == -1 {
				continue
			}
			labels = line[idx+1 : endIdx]
			valueStr := strings.TrimSpace(line[endIdx+1:])
			if v, err := strconv.ParseFloat(valueStr, 64); err == nil {
				value = v
			}
		} else {
			parts := strings.Fields(line)
			if len(parts) < 2 {
				continue
			}
			metricName = parts[0]
			if v, err := strconv.ParseFloat(parts[1], 64); err == nil {
				value = v
			}
		}

		intValue := int(value)

		switch metricName {
		case "quotadb_uptime_seconds":
			stats.UptimeSeconds = intValue
		case "quotadb_commands_total":
			stats.Commands.Total = intValue
		case "quotadb_commands":
			if strings.Contains(labels, `type="incr"`) {
				stats.Commands.Incr = intValue
			} else if strings.Contains(labels, `type="decr"`) {
				stats.Commands.Decr = intValue
			} else if strings.Contains(labels, `type="get"`) {
				stats.Commands.Get = intValue
			} else if strings.Contains(labels, `type="set"`) {
				stats.Commands.Set = intValue
			} else if strings.Contains(labels, `type="ping"`) {
				stats.Commands.Ping = intValue
			} else if strings.Contains(labels, `type="info"`) {
				stats.Commands.Info = intValue
			} else if strings.Contains(labels, `type="quota"`) {
				stats.Commands.Quota = intValue
			} else if strings.Contains(labels, `type="other"`) {
				stats.Commands.Other = intValue
			}
		case "quotadb_errors_total":
			if strings.Contains(labels, `type="parse"`) {
				stats.Errors.Parse = intValue
			} else if strings.Contains(labels, `type="unknown_cmd"`) {
				stats.Errors.UnknownCmd = intValue
			}
		case "quotadb_connections_total":
			stats.Connections.Total = intValue
		case "quotadb_connections_active":
			stats.Connections.Active = intValue
		case "quotadb_command_duration_microseconds":
			if strings.Contains(labels, `quantile="0.5"`) {
				if strings.Contains(labels, `cmd="incr"`) {
					stats.LatencyP50Us.Incr = intValue
				} else if strings.Contains(labels, `cmd="get"`) {
					stats.LatencyP50Us.Get = intValue
				} else if strings.Contains(labels, `cmd="set"`) {
					stats.LatencyP50Us.Set = intValue
				} else if strings.Contains(labels, `cmd="decr"`) {
					stats.LatencyP50Us.Decr = intValue
				}
			} else if strings.Contains(labels, `quantile="0.99"`) {
				if strings.Contains(labels, `cmd="incr"`) {
					stats.LatencyP99Us.Incr = intValue
				} else if strings.Contains(labels, `cmd="get"`) {
					stats.LatencyP99Us.Get = intValue
				} else if strings.Contains(labels, `cmd="set"`) {
					stats.LatencyP99Us.Set = intValue
				} else if strings.Contains(labels, `cmd="decr"`) {
					stats.LatencyP99Us.Decr = intValue
				}
			}
		case "quotadb_replication_deltas_sent":
			stats.Replication.DeltasSent = intValue
		case "quotadb_replication_deltas_received":
			stats.Replication.DeltasReceived = intValue
		case "quotadb_replication_bytes_sent":
			stats.Replication.BytesSent = intValue
		case "quotadb_replication_bytes_received":
			stats.Replication.BytesReceived = intValue
		case "quotadb_replication_dropped":
			stats.Replication.Dropped = intValue
		case "quotadb_replication_lag_max":
			stats.Replication.LagMax = intValue
		case "quotadb_replication_peers":
			stats.Replication.Peers = intValue
		case "quotadb_replication_rtt_microseconds":
			if strings.Contains(labels, `quantile="0.5"`) {
				stats.Replication.RttP50Us = intValue
			} else if strings.Contains(labels, `quantile="0.99"`) {
				stats.Replication.RttP99Us = intValue
			}
		case "quotadb_wal_writes":
			stats.WAL.Writes = intValue
		case "quotadb_wal_bytes_written":
			stats.WAL.BytesWritten = intValue
		case "quotadb_wal_dropped":
			stats.WAL.Dropped = intValue
		case "quotadb_wal_sync_failures":
			stats.WAL.SyncFailures = intValue
		case "quotadb_snapshots_created":
			stats.Snapshots.Created = intValue
		case "quotadb_buffer_pool_size":
			stats.BufferPool.Size = intValue
		case "quotadb_buffer_pool_hits":
			stats.BufferPool.Hits = intValue
		case "quotadb_buffer_pool_misses":
			stats.BufferPool.Misses = intValue
		}
	}

	return stats
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

			// Parse Prometheus text format
			stats := parsePrometheusMetrics(string(body))
			result.Stats = stats
			results[idx] = result
		}(i, url)
	}

	wg.Wait()
	return c.JSON(fiber.Map{"stats": results})
}

// ClusterInfo represents cluster/replication information for a database
type ClusterInfo struct {
	Version           string `json:"version"`
	UptimeSeconds     int    `json:"uptime_seconds"`
	ConnectedClients  int    `json:"connected_clients"`
	TotalConnections  int    `json:"total_connections"`
	ReplicationPeers  int    `json:"replication_peers"`
	DeltasSent        int    `json:"deltas_sent"`
	DeltasReceived    int    `json:"deltas_received"`
	ReplicationLagMax int    `json:"replication_lag_max"`
}

func (s *FiberServer) ClusterHandler(c *fiber.Ctx) error {
	type clusterResult struct {
		Index   int          `json:"index"`
		Address string       `json:"address"`
		Cluster *ClusterInfo `json:"cluster,omitempty"`
		Error   string       `json:"error,omitempty"`
	}

	ctx := context.Background()
	var wg sync.WaitGroup
	results := make([]clusterResult, len(s.dbs))

	for i, client := range s.dbs {
		wg.Add(1)
		go func(idx int, cli *redis.Client) {
			defer wg.Done()

			result := clusterResult{
				Index:   idx,
				Address: cli.Options().Addr,
			}

			cluster := &ClusterInfo{}

			// Execute INFO command (replaces CLUSTER INFO)
			infoResult, err := cli.Do(ctx, "INFO").Result()
			if err != nil {
				log.Printf("[ERROR] INFO failed on DB %d (%s): %v", idx, cli.Options().Addr, err)
				result.Error = err.Error()
				results[idx] = result
				return
			}

			// Parse INFO response (key:value format, with # Section headers)
			if infoStr, ok := infoResult.(string); ok {
				lines := strings.Split(infoStr, "\n")
				for _, line := range lines {
					line = strings.TrimSpace(line)
					if line == "" || strings.HasPrefix(line, "#") {
						continue
					}
					if parts := strings.SplitN(line, ":", 2); len(parts) == 2 {
						key := parts[0]
						val := strings.TrimSpace(parts[1])
						switch key {
						case "quota_db_version":
							cluster.Version = val
						case "uptime_in_seconds":
							if n, err := strconv.Atoi(val); err == nil {
								cluster.UptimeSeconds = n
							}
						case "connected_clients":
							if n, err := strconv.Atoi(val); err == nil {
								cluster.ConnectedClients = n
							}
						case "total_connections_received":
							if n, err := strconv.Atoi(val); err == nil {
								cluster.TotalConnections = n
							}
						case "replication_peers":
							if n, err := strconv.Atoi(val); err == nil {
								cluster.ReplicationPeers = n
							}
						case "deltas_sent":
							if n, err := strconv.Atoi(val); err == nil {
								cluster.DeltasSent = n
							}
						case "deltas_received":
							if n, err := strconv.Atoi(val); err == nil {
								cluster.DeltasReceived = n
							}
						case "replication_lag_max":
							if n, err := strconv.Atoi(val); err == nil {
								cluster.ReplicationLagMax = n
							}
						}
					}
				}
			}

			result.Cluster = cluster
			results[idx] = result
		}(i, client)
	}

	wg.Wait()
	return c.JSON(fiber.Map{"clusters": results})
}

func (s *FiberServer) ListKeysHandler(c *fiber.Ctx) error {
	ctx := context.Background()
	client, idx := s.getDB(c)

	// Get limit from query parameter, default to 100
	limit := c.QueryInt("limit", 100)
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	start := time.Now()
	var keys []string

	// Use single SCAN call with limit (not iterator which auto-paginates)
	scanKeys, _, err := client.Scan(ctx, 0, "*", int64(limit)).Result()
	if err != nil {
		log.Printf("[ERROR] SCAN failed on DB %d (%s): %v", idx, client.Options().Addr, err)
	} else {
		keys = scanKeys
	}
	RecordRedisCommand(idx, client.Options().Addr, "scan", time.Since(start), err)

	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"keys": keys, "db_index": idx})
}

func (s *FiberServer) ListKeysAllHandler(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Get pattern from query parameter, default to "*"
	pattern := c.Query("pattern", "*")
	if pattern == "" {
		pattern = "*"
	}

	// Get limit from query parameter, default to 100 per DB
	limit := c.QueryInt("limit", 100)
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	type dbKeys struct {
		idx    int
		addr   string
		keys   []string
		dbsize int64
		err    error
	}

	resultsChan := make(chan dbKeys, len(s.dbs))

	// Fetch keys and DBSIZE from all databases in parallel using single SCAN call
	for i, client := range s.dbs {
		go func(idx int, cli *redis.Client, p string, lim int64) {
			var keys []string
			var dbsize int64
			var err error

			// Get DBSIZE first
			dbsize, err = cli.DBSize(ctx).Result()
			if err != nil {
				log.Printf("[ERROR] DBSIZE failed on DB %d (%s): %v", idx, cli.Options().Addr, err)
				dbsize = -1
				err = nil // Continue with key fetch
			}

			// Use single SCAN call with limit (not iterator which auto-paginates)
			scanKeys, _, scanErr := cli.Scan(ctx, 0, p, lim).Result()
			if scanErr != nil {
				log.Printf("[ERROR] SCAN failed on DB %d (%s): %v", idx, cli.Options().Addr, scanErr)
				err = scanErr
			} else {
				keys = scanKeys
			}

			resultsChan <- dbKeys{idx: idx, addr: cli.Options().Addr, keys: keys, dbsize: dbsize, err: err}
		}(i, client, pattern, int64(limit))
	}

	// Collect results and build a map of key -> list of db indices
	keyMap := make(map[string][]int)
	dbAddresses := make(map[int]string)
	dbSizes := make(map[int]int64)
	var totalDbSize int64

	for range s.dbs {
		r := <-resultsChan
		dbAddresses[r.idx] = r.addr
		dbSizes[r.idx] = r.dbsize
		if r.dbsize > 0 {
			totalDbSize += r.dbsize
		}
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
		"keys":         keys,
		"databases":    dbAddresses,
		"db_sizes":     dbSizes,
		"total_dbsize": totalDbSize,
	})
}

func (s *FiberServer) OperationHandler(c *fiber.Ctx) error {
	ctx := context.Background()
	operation := c.Query("op")
	key := c.Query("key")
	valueStr := c.Query("value")

	if operation == "" || key == "" {
		return c.Status(400).JSON(fiber.Map{"error": "op and key query parameters are required"})
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

	if err != nil {
		log.Printf("[ERROR] Redis %s failed on DB %d (%s) key=%s: %v", op, dbIndex, client.Options().Addr, key, err)
	}

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
	Operation     string `json:"operation"`
	Key           string `json:"key"`
	Value         int64  `json:"value"`
	StringValue   string `json:"string_value"`
	Workers       int    `json:"workers"`
	Duration      int    `json:"duration"` // seconds
	DbIndex       int    `json:"db_index"`
	AllDbs        bool   `json:"all_dbs"`
	RandomDb      bool   `json:"random_db"`
	RandomizeKeys bool   `json:"randomize_keys"`
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

	validOps := map[string]bool{"incrby": true, "decrby": true, "expire": true, "pexpire": true, "set": true}
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

			// Generate key with optional UUID suffix
			key := req.Key
			if req.RandomizeKeys {
				key = fmt.Sprintf("%s:%016x%016x", req.Key, rand.Int63(), rand.Int63())
			}

			var err error
			if req.AllDbs {
				// Execute on all DBs
				for _, client := range s.dbs {
					_, e := s.executeOperation(ctx, client, req.Operation, key, req.Value, req.StringValue)
					if e != nil {
						err = e
					}
				}
			} else if req.RandomDb && len(s.dbs) > 0 {
				// Execute on random DB
				dbIdx := rand.Intn(len(s.dbs))
				_, err = s.executeOperation(ctx, s.dbs[dbIdx], req.Operation, key, req.Value, req.StringValue)
			} else {
				// Execute on specific DB
				dbIdx := req.DbIndex
				if dbIdx < 0 || dbIdx >= len(s.dbs) {
					dbIdx = 0
				}
				_, err = s.executeOperation(ctx, s.dbs[dbIdx], req.Operation, key, req.Value, req.StringValue)
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
	key := c.Query("key")
	if key == "" {
		return c.Status(400).JSON(fiber.Map{"error": "key query parameter is required"})
	}

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

// Config handlers
func (s *FiberServer) GetConfigHandler(c *fiber.Ctx) error {
	cfg, err := config.Load()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{
		"config":      cfg,
		"config_path": config.GetConfigPath(),
	})
}

func (s *FiberServer) SaveConfigHandler(c *fiber.Ctx) error {
	var cfg config.Config
	if err := c.BodyParser(&cfg); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid request body"})
	}

	if err := config.Save(&cfg); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{"success": true, "message": "Config saved. Click 'Reconnect' to apply changes."})
}

type AddAddressRequest struct {
	Address string `json:"address"`
}

func (s *FiberServer) AddAddressHandler(c *fiber.Ctx) error {
	var req AddAddressRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid request body"})
	}

	if req.Address == "" {
		return c.Status(400).JSON(fiber.Map{"error": "address is required"})
	}

	if err := config.AddAddress(req.Address); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{"success": true, "message": "Address added. Click 'Reconnect' to apply changes."})
}

func (s *FiberServer) RemoveAddressHandler(c *fiber.Ctx) error {
	address := c.Params("*")
	if decoded, err := url.QueryUnescape(address); err == nil {
		address = decoded
	}
	if address == "" {
		return c.Status(400).JSON(fiber.Map{"error": "address is required"})
	}

	if err := config.RemoveAddress(address); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{"success": true, "message": "Address removed. Click 'Reconnect' to apply changes."})
}

func (s *FiberServer) ReconnectHandler(c *fiber.Ctx) error {
	cfg, err := config.Load()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	if len(cfg.RedisAddresses) == 0 {
		return c.Status(400).JSON(fiber.Map{"error": "No Redis addresses configured"})
	}

	// Close existing connections
	for _, client := range s.dbs {
		client.Close()
	}

	// Create new connections
	poolSize := cfg.PoolSize
	if poolSize <= 0 {
		poolSize = 500
	}

	var clients []*redis.Client
	var statsURLs []string

	for _, addr := range cfg.RedisAddresses {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		client := redis.NewClient(&redis.Options{
			Addr:            addr,
			Password:        cfg.RedisPassword,
			DB:              cfg.RedisDB,
			PoolSize:        poolSize,
			MinIdleConns:    poolSize / 10,
			PoolTimeout:     5 * time.Second,
			ConnMaxIdleTime: 5 * time.Minute,
			DialTimeout:     5 * time.Second,
			ReadTimeout:     3 * time.Second,
			WriteTimeout:    3 * time.Second,
		})
		clients = append(clients, client)

		// Derive stats URL
		host := strings.Split(addr, ":")[0]
		statsURLs = append(statsURLs, fmt.Sprintf("http://%s:9190/metrics", host))
	}

	s.dbs = clients
	s.statsURLs = statsURLs

	return c.JSON(fiber.Map{
		"success":   true,
		"message":   "Reconnected to Redis",
		"databases": len(clients),
	})
}
