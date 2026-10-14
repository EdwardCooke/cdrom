package metrics

// Histogram bucket presets (F-20). They are shared by every component's
// metric set so the histogram boundaries are consistent across the stack.
//
//   - Request/RPC durations: 5ms … 30s.
//   - Job and run durations: 1s, 5s, 30s, 1m, 5m, 15m, 30m, 1h, 3h.
//   - Queue wait: 1s, 5s, 30s, 1m, 5m, 15m, 1h.
var (
	// RequestBuckets are the histogram buckets for request and RPC durations
	// (HTTP and gRPC).
	RequestBuckets = []float64{
		0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30,
	}
	// JobRunBuckets are the histogram buckets for job and run durations.
	JobRunBuckets = []float64{
		1, 5, 30, 60, 300, 900, 1800, 3600, 10800,
	}
	// QueueWaitBuckets are the histogram buckets for a job's pending→running
	// wait (the queue's wait time).
	QueueWaitBuckets = []float64{
		1, 5, 30, 60, 300, 900, 3600,
	}
)
