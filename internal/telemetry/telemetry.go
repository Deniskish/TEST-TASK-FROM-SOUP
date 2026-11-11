package telemetry

import (
	"context"
	"encoding/json"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"net/http"
	"os"
	"strconv"
	"time"
)

var Requests = promauto.NewCounterVec(prometheus.CounterOpts{Name: "soup_http_requests_total", Help: "HTTP responses"}, []string{"status"})
var Latency = promauto.NewHistogram(prometheus.HistogramOpts{Name: "soup_http_duration_seconds", Help: "HTTP duration", Buckets: prometheus.DefBuckets})
var Processing = promauto.NewHistogram(prometheus.HistogramOpts{Name: "soup_processing_seconds", Help: "Attempt duration", Buckets: prometheus.DefBuckets})
var Retries = promauto.NewCounter(prometheus.CounterOpts{Name: "soup_retries_total", Help: "Repeated claims"})
var Expired = promauto.NewCounter(prometheus.CounterOpts{Name: "soup_lease_expirations_total", Help: "Expired leases recovered or exhausted"})
var Stale = promauto.NewCounter(prometheus.CounterOpts{Name: "soup_stale_workers_total", Help: "Rejected lease checks"})

func Start(ctx context.Context, name string) (context.Context, trace.Span) {
	return otel.Tracer("soup").Start(ctx, name)
}
func Carrier(ctx context.Context) propagation.MapCarrier {
	m := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, m)
	return m
}
func CarrierJSON(ctx context.Context) []byte { b, _ := json.Marshal(Carrier(ctx)); return b }
func Extract(ctx context.Context, b []byte) context.Context {
	m := propagation.MapCarrier{}
	_ = json.Unmarshal(b, &m)
	return otel.GetTextMapPropagator().Extract(ctx, m)
}
func Init(ctx context.Context, service string) func() {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return func() {}
	}
	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return func() {}
	}
	tp := sdk.NewTracerProvider(sdk.WithBatcher(exp), sdk.WithResource(resource.NewWithAttributes("", attribute.String("service.name", service))))
	otel.SetTracerProvider(tp)
	return func() {
		c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = tp.Shutdown(c)
	}
}

type response struct {
	http.ResponseWriter
	status int
}

func (w *response) WriteHeader(n int) {
	if w.status == 0 {
		w.status = n
		w.ResponseWriter.WriteHeader(n)
	}
}
func (w *response) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(b)
}
func HTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := Start(ctx, "http.request")
		defer span.End()
		start := time.Now()
		rw := &response{ResponseWriter: w}
		next.ServeHTTP(rw, r.WithContext(ctx))
		if rw.status == 0 {
			rw.status = 200
		}
		Requests.WithLabelValues(strconv.Itoa(rw.status)).Inc()
		Latency.Observe(time.Since(start).Seconds())
	})
}
func Serve(ctx context.Context, pool *pgxpool.Pool, database bool) {
	mux := http.NewServeMux()
	if database {
		prometheus.MustRegister(&dbCollector{pool})
	}
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: ":9090", Handler: mux, ReadHeaderTimeout: 3 * time.Second}
	go func() { _ = srv.ListenAndServe() }()
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(c)
	}()
}

type dbCollector struct{ p *pgxpool.Pool }

func (d *dbCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("soup_postgres_available", "soup_postgres_available", nil, nil)
	ch <- prometheus.NewDesc("soup_jobs", "Job states", []string{"state"}, nil)
	for _, n := range []string{"soup_oldest_job_seconds", "soup_outbox_backlog", "soup_expired_leases"} {
		ch <- prometheus.NewDesc(n, n, nil, nil)
	}
}
func (d *dbCollector) Collect(ch chan<- prometheus.Metric) {
	emit := func(n string, v float64) {
		ch <- prometheus.MustNewConstMetric(prometheus.NewDesc(n, n, nil, nil), prometheus.GaugeValue, v)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rows, err := d.p.Query(ctx, "SELECT status,count(*) FROM jobs GROUP BY status")
	if err != nil {
		emit("soup_postgres_available", 0)
		return
	}
	defer rows.Close()
	counts := map[string]float64{"queued": 0, "running": 0, "succeeded": 0, "failed": 0, "cancelled": 0}
	for rows.Next() {
		var state string
		var n float64
		if rows.Scan(&state, &n) != nil {
			return
		}
		counts[state] = n
	}
	if rows.Err() != nil {
		emit("soup_postgres_available", 0)
		return
	}
	for state, n := range counts {
		ch <- prometheus.MustNewConstMetric(prometheus.NewDesc("soup_jobs", "Job states", []string{"state"}, nil), prometheus.GaugeValue, n, state)
	}
	var oldest, backlog, expired float64
	err = d.p.QueryRow(ctx, `SELECT COALESCE((SELECT extract(epoch FROM clock_timestamp()-min(created_at)) FROM jobs WHERE status IN ('queued','running')),0),(SELECT count(*) FROM outbox WHERE published_at IS NULL),(SELECT count(*) FROM jobs WHERE status='running' AND lease_until<=clock_timestamp())`).Scan(&oldest, &backlog, &expired)
	if err != nil {
		emit("soup_postgres_available", 0)
		return
	}
	emit("soup_postgres_available", 1)
	emit("soup_oldest_job_seconds", oldest)
	emit("soup_outbox_backlog", backlog)
	emit("soup_expired_leases", expired)
}
