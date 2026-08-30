package main

import (
	"context"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/multitracer"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	databaseWorkloadAPI    = "api"
	databaseWorkloadOutbox = "outbox"

	databaseAcquireSuccess = "success"
	databaseAcquireError   = "error"
)

type databaseWorkloadContextKey struct{}
type databaseAcquireContextKey struct{}

type databaseAcquireTrace struct {
	started  time.Time
	workload string
}

type databaseAcquireTracer struct {
	metrics *applicationMetrics
	now     func() time.Time
}

func withDatabaseWorkload(ctx context.Context, workload string) context.Context {
	switch workload {
	case databaseWorkloadAPI, databaseWorkloadOutbox:
		return context.WithValue(ctx, databaseWorkloadContextKey{}, workload)
	default:
		return ctx
	}
}

func databaseWorkloadFromContext(ctx context.Context) (string, bool) {
	workload, ok := ctx.Value(databaseWorkloadContextKey{}).(string)
	if !ok {
		return "", false
	}
	switch workload {
	case databaseWorkloadAPI, databaseWorkloadOutbox:
		return workload, true
	default:
		return "", false
	}
}

func databaseWorkloadHandler(workload string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r.WithContext(withDatabaseWorkload(r.Context(), workload)))
	})
}

func attachDatabaseAcquireTracer(config *pgxpool.Config, metrics *applicationMetrics) {
	tracer := &databaseAcquireTracer{metrics: metrics, now: time.Now}
	if config.ConnConfig.Tracer == nil {
		config.ConnConfig.Tracer = tracer
		return
	}
	config.ConnConfig.Tracer = multitracer.New(config.ConnConfig.Tracer, tracer)
}

func (tracer *databaseAcquireTracer) TraceAcquireStart(
	ctx context.Context,
	_ *pgxpool.Pool,
	_ pgxpool.TraceAcquireStartData,
) context.Context {
	workload, ok := databaseWorkloadFromContext(ctx)
	if !ok || tracer == nil || tracer.metrics == nil {
		return ctx
	}
	return context.WithValue(ctx, databaseAcquireContextKey{}, databaseAcquireTrace{
		started:  tracer.now(),
		workload: workload,
	})
}

func (tracer *databaseAcquireTracer) TraceAcquireEnd(
	ctx context.Context,
	_ *pgxpool.Pool,
	data pgxpool.TraceAcquireEndData,
) {
	if tracer == nil || tracer.metrics == nil {
		return
	}
	acquire, ok := ctx.Value(databaseAcquireContextKey{}).(databaseAcquireTrace)
	if !ok {
		return
	}
	result := databaseAcquireSuccess
	if data.Err != nil {
		result = databaseAcquireError
	}
	tracer.metrics.ObserveDatabaseAcquire(acquire.workload, result, tracer.now().Sub(acquire.started))
}

// pgx stores one tracer value for both connection queries and pool acquisition.
// Query timing is intentionally left to the existing stage metrics; these
// methods make this tracer composable without duplicating query observations.
func (tracer *databaseAcquireTracer) TraceQueryStart(
	ctx context.Context,
	_ *pgx.Conn,
	_ pgx.TraceQueryStartData,
) context.Context {
	return ctx
}

func (tracer *databaseAcquireTracer) TraceQueryEnd(
	_ context.Context,
	_ *pgx.Conn,
	_ pgx.TraceQueryEndData,
) {
}
