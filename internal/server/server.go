package server

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

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
// Configure via REDIS_ADDRESSES env var (comma-separated), e.g.: "localhost:6379,localhost:6380"
// Optional: REDIS_PASSWORD and REDIS_DB (applied to all connections)
func New() *FiberServer {
	addressesStr := os.Getenv("REDIS_ADDRESSES")
	if addressesStr == "" {
		// Fallback to single address from old env vars
		addr := os.Getenv("BLUEPRINT_DB_ADDRESS")
		port := os.Getenv("BLUEPRINT_DB_PORT")
		if addr != "" && port != "" {
			addressesStr = addr + ":" + port
		} else {
			addressesStr = "localhost:6379"
		}
	}

	addresses := strings.Split(addressesStr, ",")
	password := os.Getenv("REDIS_PASSWORD")
	if password == "" {
		password = os.Getenv("BLUEPRINT_DB_PASSWORD")
	}

	dbNum := 0
	if dbStr := os.Getenv("REDIS_DB"); dbStr != "" {
		if n, err := strconv.Atoi(dbStr); err == nil {
			dbNum = n
		}
	} else if dbStr := os.Getenv("BLUEPRINT_DB_DATABASE"); dbStr != "" {
		if n, err := strconv.Atoi(dbStr); err == nil {
			dbNum = n
		}
	}

	// Pool size configuration (default 500, increase via REDIS_POOL_SIZE for high concurrency)
	poolSize := 500
	if ps := os.Getenv("REDIS_POOL_SIZE"); ps != "" {
		if n, err := strconv.Atoi(ps); err == nil && n > 0 {
			poolSize = n
		}
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
		log.Fatal("No Redis addresses configured")
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

	return server
}
