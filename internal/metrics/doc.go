// Package metrics is the shared observability plumbing for every cdrom
// component (F-20). It provides:
//
//   - a Collector: a self-contained prometheus.Registry (with the Go runtime
//     and process collectors registered) plus a promhttp /metrics handler and
//     a /healthz handler, and the shared cdrom_up / cdrom_build_info gauges;
//   - StartMetrics: a small HTTP server that serves only /metrics and
//     /healthz on a dedicated port, separate from the component's service
//     ports;
//   - label conventions and histogram bucket presets;
//   - a gRPC server-side stats.Handler (StatsHandler) that records
//     cdrom_grpc_server_started_total / handled_total / handling_seconds on
//     every service server, including streaming RPCs;
//   - one file per component (api.go, scheduler.go, database.go, artifacts.go,
//     idp.go, worker.go) that declares that component's metric set.
//
// Naming: every metric is prefixed cdrom_. Domain labels are low-cardinality
// by construction (status, trigger, environment, group, step_type, method,
// code, route, kind, resource, op, outcome). A metric never labels by job id,
// run id, user, worker name, or secret name — those are unbounded and would
// blow up the series count.
//
// Each binary uses its own prometheus.Registry (via a Collector) so a
// component's metric set is self-contained and tests can use a fresh
// registry. A component constructs its metric set at startup (passing the
// Collector's registry) and records into it at the hot spots. When metrics
// are disabled (config) the metric set is left nil and every recording call
// is a no-op (the methods are nil-safe).
package metrics
