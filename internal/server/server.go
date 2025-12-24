package server

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"a2adb-tester/internal/config"

	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"

	_ "github.com/joho/godotenv/autoload"
)

type FiberServer struct {
	*fiber.App

	dbs      []*redis.Client
	statsURLs []string // Prometheus stats endpoints for each DB
}

// New creates a new FiberServer with multiple Redis connections.
// First tries to load from config file (~/.a2adb-tester/config.json)
// Falls back to REDIS_ADDRESSES env var (comma-separated), e.g.: "localhost:6379,localhost:6380"
// Optional: REDIS_PASSWORD and REDIS_DB (applied to all connections)
func New() *FiberServer {
	var addresses []string
	var password string
	var dbNum int
	var poolSize int

	// Try loading from config file first
	cfg, err := config.Load()
	if err == nil && len(cfg.RedisAddresses) > 0 {
		log.Printf("Loading config from: %s", config.GetConfigPath())
		addresses = cfg.RedisAddresses
		password = cfg.RedisPassword
		dbNum = cfg.RedisDB
		poolSize = cfg.PoolSize
	} else {
		// Fallback to environment variables
		log.Printf("No config file found, using environment variables")
		addressesStr := os.Getenv("REDIS_ADDRESSES")
		if addressesStr == "" {
			// Fallback to single address from old env vars
			addr := os.Getenv("BLUEPRINT_DB_ADDRESS")
			port := os.Getenv("BLUEPRINT_DB_PORT")
			if addr != "" && port != "" {
				addressesStr = addr + ":" + port
			} else {
				addressesStr = ""
			}
		}

		if addressesStr != "" {
			addresses = strings.Split(addressesStr, ",")
		}

		password = os.Getenv("REDIS_PASSWORD")
		if password == "" {
			password = os.Getenv("BLUEPRINT_DB_PASSWORD")
		}

		if dbStr := os.Getenv("REDIS_DB"); dbStr != "" {
			if n, err := strconv.Atoi(dbStr); err == nil {
				dbNum = n
			}
		} else if dbStr := os.Getenv("BLUEPRINT_DB_DATABASE"); dbStr != "" {
			if n, err := strconv.Atoi(dbStr); err == nil {
				dbNum = n
			}
		}

		if ps := os.Getenv("REDIS_POOL_SIZE"); ps != "" {
			if n, err := strconv.Atoi(ps); err == nil && n > 0 {
				poolSize = n
			}
		}
	}

	// Default pool size
	if poolSize <= 0 {
		poolSize = 500
	}

	var clients []*redis.Client
	for _, addr := range addresses {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		client := redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: password,
			DB:       dbNum,

			// Connection pool settings for high throughput
			PoolSize:        poolSize,              // Max connections per Redis server
			MinIdleConns:    poolSize / 10,         // Keep 10% idle connections ready
			PoolTimeout:     5 * time.Second,       // Wait time if pool is exhausted
			ConnMaxIdleTime: 5 * time.Minute,       // Close idle connections after this

			// Timeouts for remote connections
			DialTimeout:  5 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
		})
		clients = append(clients, client)
		log.Printf("Connected to Redis: %s (pool size: %d)", addr, poolSize)
	}

	if len(clients) == 0 {
		log.Printf("No Redis addresses configured. Use the UI to add addresses.")
	}

	// Stats URLs configuration
	// Can be set via REDIS_STATS_URLS (comma-separated) or derived from Redis addresses with port 9090
	var statsURLs []string
	if statsStr := os.Getenv("REDIS_STATS_URLS"); statsStr != "" {
		statsURLs = strings.Split(statsStr, ",")
		for i := range statsURLs {
			statsURLs[i] = strings.TrimSpace(statsURLs[i])
		}
	} else {
		// Derive from Redis addresses - replace port with 9090
		for _, addr := range addresses {
			addr = strings.TrimSpace(addr)
			if addr == "" {
				continue
			}
			host := strings.Split(addr, ":")[0]
			statsURLs = append(statsURLs, fmt.Sprintf("http://%s:9090", host))
		}
	}

	for i, url := range statsURLs {
		log.Printf("Stats endpoint %d: %s", i, url)
	}

	server := &FiberServer{
		App: fiber.New(fiber.Config{
			ServerHeader: "a2adb-tester",
			AppName:      "a2adb-tester",
		}),
		dbs:       clients,
		statsURLs: statsURLs,
	}

	// Start Prometheus metrics server on port 9090
	StartMetricsServer()
	log.Printf("Prometheus metrics available at :9090/metrics")

	// Start pool stats collector
	go server.collectPoolStats()

	return server
}

// collectPoolStats periodically updates Redis pool metrics
func (s *FiberServer) collectPoolStats() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		for i, client := range s.dbs {
			stats := client.PoolStats()
			UpdatePoolStats(i, client.Options().Addr, int(stats.TotalConns), int(stats.IdleConns), int(stats.TotalConns-stats.IdleConns))
		}
	}
}
