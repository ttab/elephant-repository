package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twitchtv/twirp"
)

// The code in this file is adapted from
// https://github.com/navigacontentlab/panurge/blob/main/twirp.go
//
// See twirp_metrics.LICENSE

type TwirpMetricsOptions struct {
	reg             prometheus.Registerer
	testLatency     time.Duration
	contextCustomer func(ctx context.Context) string
}

type TwirpMetricOptionFunc func(opts *TwirpMetricsOptions)

// WithTwirpMetricsRegisterer uses a custom registerer for Twirp metrics.
func WithTwirpMetricsRegisterer(reg prometheus.Registerer) TwirpMetricOptionFunc {
	return func(opts *TwirpMetricsOptions) {
		opts.reg = reg
	}
}

// WithTwirpMetricsStaticTestLatency configures the RPC metrics to report
// a static duration.
func WithTwirpMetricsStaticTestLatency(latency time.Duration) TwirpMetricOptionFunc {
	return func(opts *TwirpMetricsOptions) {
		opts.testLatency = latency
	}
}

// NewTwirpMetricsHooks creates new twirp hooks enabling prometheus metrics.
func NewTwirpMetricsHooks(opts ...TwirpMetricOptionFunc) (*twirp.ServerHooks, error) {
	opt := TwirpMetricsOptions{
		reg: prometheus.DefaultRegisterer,
		contextCustomer: func(ctx context.Context) string {
			return ""
		},
	}

	for i := range opts {
		opts[i](&opt)
	}

	requestsReceived := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rpc_requests_total",
			Help: "Number of RPC requests received.",
		},
		[]string{"service", "method", "customer"},
	)
	if err := opt.reg.Register(requestsReceived); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	duration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "rpc_duration_seconds",
		Help:    "Duration for a rpc call.",
		Buckets: prometheus.ExponentialBuckets(0.005, 1.75, 15),
	}, []string{"service", "method", "customer"})
	if err := opt.reg.Register(duration); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	responsesSent := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rpc_responses_total",
			Help: "Number of RPC responses sent.",
		},
		[]string{"service", "method", "status", "customer"},
	)
	if err := opt.reg.Register(responsesSent); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	var hooks twirp.ServerHooks

	var reqStartTimestampKey = new(int)

	hooks.RequestReceived = func(ctx context.Context) (context.Context, error) {
		return context.WithValue(ctx, reqStartTimestampKey, time.Now()), nil
	}

	hooks.ResponseSent = func(ctx context.Context) {
		serviceName, sOk := twirp.ServiceName(ctx)
		method, mOk := twirp.MethodName(ctx)

		if !mOk || !sOk {
			return
		}

		customer := opt.contextCustomer(ctx)
		status, _ := twirp.StatusCode(ctx)

		responsesSent.WithLabelValues(
			serviceName, method, status, customer,
		).Inc()

		if start, ok := ctx.Value(reqStartTimestampKey).(time.Time); ok {
			dur := time.Since(start).Seconds() // 100ms = 0.1 sek

			if opt.testLatency != 0 {
				dur = opt.testLatency.Seconds()
			}

			duration.WithLabelValues(
				serviceName, method, customer,
			).Observe(dur)
		}
	}

	hooks.RequestRouted = func(ctx context.Context) (context.Context, error) {
		serviceName, sOk := twirp.ServiceName(ctx)
		method, mOk := twirp.MethodName(ctx)

		if !(sOk && mOk) {
			return ctx, nil
		}

		customer := opt.contextCustomer(ctx)

		requestsReceived.WithLabelValues(
			serviceName, method, customer,
		).Inc()

		return ctx, nil
	}

	return &hooks, nil
}
