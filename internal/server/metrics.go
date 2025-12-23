package server

import (
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// Redis command duration histogram
	redisCommandDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "redis_command_duration_seconds",
			Help:    "Duration of Redis commands in seconds",
			Buckets: []float64{.0001, .0005, .001, .005, .01, .025, .05, .1, .25, .5, 1},
		},
		[]string{"db_index", "db_address", "command"},
	)

	// Redis command total counter
	redisCommandTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "redis_command_total",
			Help: "Total number of Redis commands executed",
		},
		[]string{"db_index", "db_address", "command", "status"},
	)

	// Redis command errors counter
	redisCommandErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "redis_command_errors_total",
			Help: "Total number of Redis command errors",
		},
		[]string{"db_index", "db_address", "command"},
	)

	// HTTP request duration histogram
	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Duration of HTTP requests in seconds",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		},
		[]string{"method", "path", "status"},
	)

	// HTTP requests total counter
	httpRequestTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "path", "status"},
	)

	// Active load tests gauge
	activeLoadTestsGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "load_tests_active",
			Help: "Number of currently running load tests",
		},
	)

	// Load test requests counter
	loadTestRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "load_test_requests_total",
			Help: "Total requests executed during load tests",
		},
		[]string{"db_index", "command", "status"},
	)

	// Redis connection pool stats
	redisPoolSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redis_pool_size",
			Help: "Current size of the Redis connection pool",
		},
		[]string{"db_index", "db_address"},
	)

	redisPoolIdleConns = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redis_pool_idle_connections",
			Help: "Number of idle connections in the Redis pool",
		},
		[]string{"db_index", "db_address"},
	)

	redisPoolActiveConns = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redis_pool_active_connections",
			Help: "Number of active connections in the Redis pool",
		},
		[]string{"db_index", "db_address"},
	)

	// Keys count per database
	redisKeysCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "redis_keys_count",
			Help: "Number of keys in each Redis database",
		},
		[]string{"db_index", "db_address"},
	)
)

// Internal metrics tracking for JSON API
type internalCommandMetrics struct {
	totalCalls   atomic.Int64
	errorCount   atomic.Int64
	totalLatency atomic.Int64 // in nanoseconds
	minLatency   atomic.Int64 // in nanoseconds
	maxLatency   atomic.Int64 // in nanoseconds
}

type internalDBMetrics struct {
	commands   map[string]*internalCommandMetrics
	commandsMu sync.RWMutex
	poolSize   atomic.Int64
	poolIdle   atomic.Int64
	poolActive atomic.Int64
}

var (
	internalMetrics   = make(map[int]*internalDBMetrics)
	internalMetricsMu sync.RWMutex
	activeLoadTests   atomic.Int64
)

func getOrCreateDBMetrics(dbIndex int) *internalDBMetrics {
	internalMetricsMu.RLock()
	m, ok := internalMetrics[dbIndex]
	internalMetricsMu.RUnlock()
	if ok {
		return m
	}

	internalMetricsMu.Lock()
	defer internalMetricsMu.Unlock()
	if m, ok = internalMetrics[dbIndex]; ok {
		return m
	}
	m = &internalDBMetrics{
		commands: make(map[string]*internalCommandMetrics),
	}
	internalMetrics[dbIndex] = m
	return m
}

func getOrCreateCommandMetrics(dbMetrics *internalDBMetrics, command string) *internalCommandMetrics {
	dbMetrics.commandsMu.RLock()
	m, ok := dbMetrics.commands[command]
	dbMetrics.commandsMu.RUnlock()
	if ok {
		return m
	}

	dbMetrics.commandsMu.Lock()
	defer dbMetrics.commandsMu.Unlock()
	if m, ok = dbMetrics.commands[command]; ok {
		return m
	}
	m = &internalCommandMetrics{}
	m.minLatency.Store(int64(^uint64(0) >> 1)) // Max int64
	dbMetrics.commands[command] = m
	return m
}

func init() {
	// Register all metrics
	prometheus.MustRegister(
		redisCommandDuration,
		redisCommandTotal,
		redisCommandErrors,
		httpRequestDuration,
		httpRequestTotal,
		activeLoadTestsGauge,
		loadTestRequests,
		redisPoolSize,
		redisPoolIdleConns,
		redisPoolActiveConns,
		redisKeysCount,
	)
}

// StartMetricsServer starts the Prometheus metrics server on port 9090
func StartMetricsServer() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	go func() {
		if err := http.ListenAndServe(":9090", mux); err != nil {
			panic(err)
		}
	}()
}

// RecordRedisCommand records metrics for a Redis command
func RecordRedisCommand(dbIndex int, dbAddress, command string, duration time.Duration, err error) {
	idx := strconv.Itoa(dbIndex)

	// Prometheus metrics
	redisCommandDuration.WithLabelValues(idx, dbAddress, command).Observe(duration.Seconds())

	status := "success"
	if err != nil {
		status = "error"
		redisCommandErrors.WithLabelValues(idx, dbAddress, command).Inc()
	}
	redisCommandTotal.WithLabelValues(idx, dbAddress, command, status).Inc()

	// Internal metrics for JSON API
	dbMetrics := getOrCreateDBMetrics(dbIndex)
	cmdMetrics := getOrCreateCommandMetrics(dbMetrics, command)

	cmdMetrics.totalCalls.Add(1)
	if err != nil {
		cmdMetrics.errorCount.Add(1)
	}

	latencyNs := duration.Nanoseconds()
	cmdMetrics.totalLatency.Add(latencyNs)

	// Update min latency
	for {
		old := cmdMetrics.minLatency.Load()
		if latencyNs >= old {
			break
		}
		if cmdMetrics.minLatency.CompareAndSwap(old, latencyNs) {
			break
		}
	}

	// Update max latency
	for {
		old := cmdMetrics.maxLatency.Load()
		if latencyNs <= old {
			break
		}
		if cmdMetrics.maxLatency.CompareAndSwap(old, latencyNs) {
			break
		}
	}
}

// RecordHTTPRequest records metrics for an HTTP request
func RecordHTTPRequest(method, path string, status int, duration time.Duration) {
	statusStr := strconv.Itoa(status)
	httpRequestDuration.WithLabelValues(method, path, statusStr).Observe(duration.Seconds())
	httpRequestTotal.WithLabelValues(method, path, statusStr).Inc()
}

// RecordLoadTestRequest records metrics for load test requests
func RecordLoadTestRequest(dbIndex int, command string, err error) {
	idx := strconv.Itoa(dbIndex)
	status := "success"
	if err != nil {
		status = "error"
	}
	loadTestRequests.WithLabelValues(idx, command, status).Inc()
}

// UpdatePoolStats updates Redis connection pool metrics
func UpdatePoolStats(dbIndex int, dbAddress string, poolSize, idleConns, activeConns int) {
	idx := strconv.Itoa(dbIndex)
	redisPoolSize.WithLabelValues(idx, dbAddress).Set(float64(poolSize))
	redisPoolIdleConns.WithLabelValues(idx, dbAddress).Set(float64(idleConns))
	redisPoolActiveConns.WithLabelValues(idx, dbAddress).Set(float64(activeConns))

	// Internal metrics
	dbMetrics := getOrCreateDBMetrics(dbIndex)
	dbMetrics.poolSize.Store(int64(poolSize))
	dbMetrics.poolIdle.Store(int64(idleConns))
	dbMetrics.poolActive.Store(int64(activeConns))
}

// UpdateKeysCount updates the keys count metric for a database
func UpdateKeysCount(dbIndex int, dbAddress string, count int) {
	idx := strconv.Itoa(dbIndex)
	redisKeysCount.WithLabelValues(idx, dbAddress).Set(float64(count))
}

// IncrementActiveLoadTests increments the active load tests gauge
func IncrementActiveLoadTests() {
	activeLoadTestsGauge.Inc()
	activeLoadTests.Add(1)
}

// DecrementActiveLoadTests decrements the active load tests gauge
func DecrementActiveLoadTests() {
	activeLoadTestsGauge.Dec()
	activeLoadTests.Add(-1)
}

// CommandMetrics holds metrics for a single command type (JSON response)
type CommandMetrics struct {
	Command      string  `json:"command"`
	TotalCalls   int64   `json:"total_calls"`
	ErrorCount   int64   `json:"error_count"`
	AvgLatencyUs float64 `json:"avg_latency_us"`
	MinLatencyUs float64 `json:"min_latency_us"`
	MaxLatencyUs float64 `json:"max_latency_us"`
}

// DBMetrics holds metrics for a single database (JSON response)
type DBMetrics struct {
	DBIndex     int               `json:"db_index"`
	Address     string            `json:"address"`
	PoolSize    int64             `json:"pool_size"`
	PoolIdle    int64             `json:"pool_idle"`
	PoolActive  int64             `json:"pool_active"`
	Commands    []*CommandMetrics `json:"commands"`
	TotalCalls  int64             `json:"total_calls"`
	TotalErrors int64             `json:"total_errors"`
}

// AppMetrics holds all application metrics (JSON response)
type AppMetrics struct {
	Databases       []*DBMetrics `json:"databases"`
	ActiveLoadTests int64        `json:"active_load_tests"`
}

// GetMetrics collects and returns current metrics as JSON-friendly struct
func GetMetrics(dbCount int, dbAddresses []string) *AppMetrics {
	metrics := &AppMetrics{
		Databases:       make([]*DBMetrics, dbCount),
		ActiveLoadTests: activeLoadTests.Load(),
	}

	for i := 0; i < dbCount; i++ {
		addr := ""
		if i < len(dbAddresses) {
			addr = dbAddresses[i]
		}

		dbMetrics := &DBMetrics{
			DBIndex:  i,
			Address:  addr,
			Commands: make([]*CommandMetrics, 0),
		}

		internalMetricsMu.RLock()
		intDB, ok := internalMetrics[i]
		internalMetricsMu.RUnlock()

		if ok {
			dbMetrics.PoolSize = intDB.poolSize.Load()
			dbMetrics.PoolIdle = intDB.poolIdle.Load()
			dbMetrics.PoolActive = intDB.poolActive.Load()

			intDB.commandsMu.RLock()
			for cmd, cmdMetrics := range intDB.commands {
				totalCalls := cmdMetrics.totalCalls.Load()
				if totalCalls > 0 {
					errorCount := cmdMetrics.errorCount.Load()
					totalLatency := cmdMetrics.totalLatency.Load()
					minLatency := cmdMetrics.minLatency.Load()
					maxLatency := cmdMetrics.maxLatency.Load()

					avgLatencyUs := float64(totalLatency) / float64(totalCalls) / 1000.0 // ns to us
					minLatencyUs := float64(minLatency) / 1000.0
					maxLatencyUs := float64(maxLatency) / 1000.0

					if minLatency == int64(^uint64(0)>>1) {
						minLatencyUs = 0
					}

					cm := &CommandMetrics{
						Command:      cmd,
						TotalCalls:   totalCalls,
						ErrorCount:   errorCount,
						AvgLatencyUs: avgLatencyUs,
						MinLatencyUs: minLatencyUs,
						MaxLatencyUs: maxLatencyUs,
					}
					dbMetrics.Commands = append(dbMetrics.Commands, cm)
					dbMetrics.TotalCalls += totalCalls
					dbMetrics.TotalErrors += errorCount
				}
			}
			intDB.commandsMu.RUnlock()
		}

		metrics.Databases[i] = dbMetrics
	}

	return metrics
}
