package sinker

import "github.com/streamingfast/dmetrics"

func RegisterMetrics() {
	metrics.Register()
}

var metrics = dmetrics.NewSet()

//var FlushedEntriesCount = metrics.NewCounter("substreams_sink_graphcsv_flushed_e", "The number of flushed entries")
