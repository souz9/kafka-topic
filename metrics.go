package ktopic

import "github.com/prometheus/client_golang/prometheus"

var Metrics = []prometheus.Collector{
	MetricQueued,
	MetricDropped,
	MetricWriteDuration,
}

var MetricQueued = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "ktopic_queued",
}, []string{"topic"})

var MetricDropped = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "ktopic_dropped_total",
}, []string{"topic"})

var MetricWriteDuration = prometheus.NewSummaryVec(prometheus.SummaryOpts{
	Name:       "ktopic_write_duration_seconds",
	Objectives: map[float64]float64{},
}, []string{"topic"})
