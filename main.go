package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"

	//	_ "net/http/pprof"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Servers []struct {
		Host      string        `yaml:"host"`
		Port      int           `yaml:"port"`
		User      string        `yaml:"user"`
		Password  string        `yaml:"password"`
		Databases []string      `yaml:"databases"`
		Period    time.Duration `yaml:"period"`
		Timeout   int           `yaml:"timeout"`
		Connect   string        `yaml:"connect"`
	} `yaml:"servers"`
}

type Statistic struct {
	host    string        `json:"host"`
	port    int           `json:"port"`
	db      string        `json:"db"`
	time    time.Duration `json:"time"`
	maxTime float32
	minTime float32
}

type PgMetrics struct {
	connFailCounter      prometheus.Counter
	connFailQueryCounter prometheus.Counter
	connFailCurrent      prometheus.Gauge
	connFailQueryCurrent prometheus.Gauge
	connTimeGauge        prometheus.Gauge
	connAvgTimeGauge     prometheus.Gauge
	connMaxTimeGauge     prometheus.Gauge
	connMinTimeGauge     prometheus.Gauge
}

func ValidateConfigPath(path string) error {
	s, err := os.Stat(path)
	if err != nil {
		return err
	}
	if s.IsDir() {
		return fmt.Errorf("'%s' is a directory, not a normal file", path)
	}
	return nil
}

func ParseFlags() (string, error) {
	var configPath string
	flag.StringVar(&configPath, "config", "./config.yml", "path to config file")
	flag.Parse()
	if err := ValidateConfigPath(configPath); err != nil {
		return "", err
	}
	return configPath, nil
}

func NewConfig(configPath string) (*Config, error) {
	config := &Config{}
	file, err := os.Open(configPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	d := yaml.NewDecoder(file)
	if err := d.Decode(&config); err != nil {
		return nil, err
	}
	return config, nil
}
func checkPg(connString string, period time.Duration, reconnect bool, pgMetrics PgMetrics) {
	var start time.Time
	var column int32
	var err error
	var conn *pgx.Conn
	var ctx context.Context
	var stats Statistic
	var msTime float64
	var lastMinTime float64 = 0
	var lastMaxTime float64 = 0
	var lastAvgTime float64 = 0
	var lastSumTime float64
	var lastTimes []float64
	var countErrors int = 0
	var QueryExecMode pgx.QueryExecMode = pgx.QueryExecModeSimpleProtocol
	pgString, _ := pgx.ParseConfig(connString)
	stats = Statistic{
		host: pgString.Host,
		db:   pgString.Database,
		port: int(pgString.Port),
	}
	log.Printf("# Starting PGCheck: %s:%d, db: %s, period: %s, reconnect: %t\n", stats.host, stats.port, stats.db, period, reconnect)
	ctx = context.Background()
	defer conn.Close(ctx)
	for i := 0; i > -1; i++ {
		start = time.Now()
		if conn == nil || conn.IsClosed() {
			conn, err = pgx.Connect(ctx, connString)
			if err != nil {
				log.Printf("host: %s, port: %d, db: %s, CONNECT ERROR\n", stats.host, stats.port, stats.db)
				pgMetrics.connFailCounter.Inc()
				pgMetrics.connFailCurrent.Inc()
				countErrors++
				fmt.Println(err)
			}
		}
		if conn != nil && !conn.IsClosed() {
			err = conn.QueryRow(ctx, "select 1", QueryExecMode).Scan(&column)
			if err != nil {
				log.Printf("host: %s, port: %d, db: %s, QUERY ERROR\n", stats.host, stats.port, stats.db)
				countErrors++
				log.Println(err)
				pgMetrics.connFailQueryCounter.Inc()
				pgMetrics.connFailQueryCurrent.Inc()
			} else {
				stats.time = time.Since(start)
				msTime = float64(stats.time / time.Millisecond)
				if lastMinTime > msTime || lastMinTime == 0 {
					lastMinTime = msTime
					pgMetrics.connMinTimeGauge.Set(lastMinTime)
				}
				if lastMaxTime < msTime {
					lastMaxTime = msTime
					pgMetrics.connMaxTimeGauge.Set(lastMaxTime)
				}
				lastTimes = append(lastTimes, msTime)
				lastSumTime = 0
				for _, e := range lastTimes {
					lastSumTime = lastSumTime + e
				}
				lastAvgTime = lastSumTime / float64(len(lastTimes))
				pgMetrics.connAvgTimeGauge.Set(lastAvgTime)
				pgMetrics.connTimeGauge.Set(msTime)
			}
			if reconnect == true {
				conn.Close(ctx)
			}
		}
		if i == 30 {
			log.Printf("# Last 30 times for host: %s, port: %d, db: %s\n", stats.host, stats.port, stats.db)
			log.Printf("max: %.2fms, min: %.2fms, avg: %.2fms errors: %d, last: %.2fms\n", lastMaxTime, lastMinTime, lastAvgTime, countErrors, msTime)
			lastMaxTime = 0
			lastMinTime = 0
			lastAvgTime = 0
			lastTimes = nil
			countErrors = 0
			i = 0
			pgMetrics.connFailCurrent.Set(0)
			pgMetrics.connFailQueryCurrent.Set(0)
		}
		time.Sleep(period)
	}
}

func main() {
	var pgMetrics PgMetrics
	cfgPath, err := ParseFlags()
	if err != nil {
		log.Fatal(err)
	}

	cfg, err := NewConfig(cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	for _, srvConfig := range cfg.Servers {
		for _, database := range srvConfig.Databases {
			timeout := srvConfig.Timeout
			reconnect := false
			if srvConfig.Connect == "transaction" {
				reconnect = true
			}
			connString := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?connect_timeout=%d", srvConfig.User, srvConfig.Password, srvConfig.Host, srvConfig.Port, database, timeout)
			promLabels := prometheus.Labels{"host": srvConfig.Host, "db": database, "port": strconv.Itoa(srvConfig.Port)}
			pgMetrics = PgMetrics{
				connFailCurrent: promauto.NewGauge(prometheus.GaugeOpts{
					ConstLabels: promLabels,
					Name:        "pg_fail_connection_current",
					Help:        "Postgres connection fail gauge for the last 30 requests",
				}),
				connFailQueryCurrent: promauto.NewGauge(prometheus.GaugeOpts{
					ConstLabels: promLabels,
					Name:        "pg_fail_query_current",
					Help:        "Postgres query fail gauge for the last 30 requests",
				}),
				connFailCounter: promauto.NewCounter(prometheus.CounterOpts{
					ConstLabels: promLabels,
					Name:        "pg_fail_connection_total",
					Help:        "Total postgres connection fail count",
				}),
				connFailQueryCounter: promauto.NewCounter(prometheus.CounterOpts{
					ConstLabels: promLabels,
					Name:        "pg_fail_query_total",
					Help:        "Total postgres query fail count",
				}),
				connTimeGauge: promauto.NewGauge(prometheus.GaugeOpts{
					ConstLabels: promLabels,
					Name:        "pg_responce_time",
					Help:        "Postgres last response time",
				}),
				connMaxTimeGauge: promauto.NewGauge(prometheus.GaugeOpts{
					ConstLabels: promLabels,
					Name:        "pg_responce_time_max",
					Help:        "Max postgres response time for the last 30 requests",
				}),
				connMinTimeGauge: promauto.NewGauge(prometheus.GaugeOpts{
					ConstLabels: promLabels,
					Name:        "pg_responce_time_min",
					Help:        "Min postgres response time for the last 30 requests",
				}),
				connAvgTimeGauge: promauto.NewGauge(prometheus.GaugeOpts{
					ConstLabels: promLabels,
					Name:        "pg_responce_time_avg",
					Help:        "Average postgres response time for the last 30 requests",
				}),
			}
			go checkPg(connString, srvConfig.Period, reconnect, pgMetrics)
		}
	}
	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":9112", nil)
}
