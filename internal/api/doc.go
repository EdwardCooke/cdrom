// Package api implements the API/controller layer: the single control plane.
// It exposes the HTTP endpoints consumed by the UI and a gRPC surface used by
// execution targets (agents, workers) and the scheduler. It routes requests
// to the service layer (database, scheduler, artifacts), holds the agents'
// WatchJobs streams, and proxies artifact traffic. No business logic belongs
// here.
package api
