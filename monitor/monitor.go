package monitor

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	monitorInstance *benchMonitor
)

type benchMonitor struct {
	timeMetric *prometheus.HistogramVec
}

func Init() {
	go func() {
		monitorInstance = &benchMonitor{}
		monitorInstance.timeMetric = promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "time_metric",
			Help:    "Track the time of read/write from/to page store.",
			Buckets: prometheus.DefBuckets,
		}, []string{"time_metric"})

		http.Handle("/debug/metrics/prometheus", promhttp.Handler())
		http.ListenAndServe(":6060", nil)
	}()
}

func RecordWriteDuration(d time.Duration) {
	if monitorInstance == nil {
		return
	}
	if monitorInstance.timeMetric == nil {
		return
	}
	monitorInstance.timeMetric.WithLabelValues("write").Observe(d.Seconds())
}

func RecordReadDuration(d time.Duration) {
	if monitorInstance == nil {
		return
	}
	if monitorInstance.timeMetric == nil {
		return
	}
	monitorInstance.timeMetric.WithLabelValues("read").Observe(d.Seconds())
}
