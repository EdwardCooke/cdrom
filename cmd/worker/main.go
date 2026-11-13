// Command worker is the Cdrom ephemeral worker.
//
// A worker is a SHORT-LIVED PROCESS SPAWNED BY THE SUPERVISORY AGENT (F-30.4)
// to execute a single job, then terminate. It talks only to the API service.
// Must run on both Windows and Linux.
//
// Canonical env contract (the agent sets EXACTLY these; a future Kubernetes
// agent emits the same variables as a Pod spec):
//
//	CDROM_WORKER_JOB_ID      (mandated job)
//	CDROM_WORKER_JOB_TOKEN   (job token minted by the agent's StartJobExecution)
//	CDROM_API_ADDR           (API gRPC address)
//	CDROM_PARENT_GRPC_ADDR   (the agent's loopback supervision channel; F-30.4 slice 2)
//	CDROM_WORKER_ROOT        (staged-work-area base; defaults OS temp)
//	CDROM_TLS_CA_FILE        (verifies the API server cert)
//	CDROM_LOG_*              (inherited logging conventions)
//
// DELIBERATELY ABSENT: the agent credential (the worker never needs it) and
// the payload (claimed from the API with the job token — never through env,
// argv, or any side channel). The mandate rides the ENVIRONMENT ONLY: argv is
// world-readable (process listings, /proc/*/cmdline); env is not.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"cdrom/internal/config"
	apipb "cdrom/internal/gen/cdrom/api/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/grpcutil"
	"cdrom/internal/logging"
	"cdrom/internal/worker"
)

func main() {
	logger := logging.New()

	configFile, err := config.ParseFlags(os.Args[1:])
	if err != nil {
		logger.Error("config: parse flags", "err", err)
		os.Exit(1)
	}
	cfg, err := config.LoadWithFile(configFile)
	if err != nil {
		logger.Error("config: invalid", "err", err)
		os.Exit(1)
	}
	if cfg.WorkerJobID == "" {
		logger.Error("worker: CDROM_WORKER_JOB_ID is required (the agent's mandate)")
		os.Exit(1)
	}
	jobID, err := strconv.ParseInt(cfg.WorkerJobID, 10, 64)
	if err != nil {
		logger.Error("worker: invalid CDROM_WORKER_JOB_ID", "value", cfg.WorkerJobID, "err", err)
		os.Exit(1)
	}
	// F-30.4: the job token is the worker's ONLY credential (delivered in the
	// environment by the spawning agent). Missing = the mandate is broken;
	// refuse to start (there is nothing sensible to do without it).
	jobToken := os.Getenv("CDROM_WORKER_JOB_TOKEN")
	if jobToken == "" {
		logger.Error("worker: CDROM_WORKER_JOB_TOKEN is required (the agent's mandate)")
		os.Exit(1)
	}

	ctx := context.Background()

	// Metrics (F-20 / F-30.4 slice 2): the worker EXPORTS NO /metrics OF ITS
	// OWN — it is ephemeral (its server would vanish on exit) and its default
	// port COLLIDES with the supervising agent's (:9100). Instead it FORWARDS
	// its samples (step outcomes, log bytes, the final job account) to the
	// agent over the loopback supervision channel (CDROM_PARENT_GRPC_ADDR),
	// and the agent folds them into its own exported series. When the channel
	// is absent/down the worker records into a discarded set and the agent
	// infers the outcome from the exit code.

	// F-30.3: the worker connects to the API WITHOUT a client certificate —
	// the target leg is TLS-with-server-verification + the BEARER job token
	// (the worker's identity is the job token the agent handed it, not a mesh
	// cert). Plaintext when the mesh has no CA (local dev).
	apiConn, err := grpcutil.DialNoClientCert(ctx, cfg.APIAddress, cfg.TLS)
	if err != nil {
		logger.Error("dial api", "addr", cfg.APIAddress, "err", err)
		os.Exit(1)
	}
	defer apiConn.Close()

	a := worker.New(cfg.WorkerName, jobID, worker.Dependencies{
		API:            apipb.NewAPIClient(apiConn),
		JobToken:       jobToken,
		AgentRoot:      cfg.AgentRoot,
		ParentGrpcAddr: os.Getenv("CDROM_PARENT_GRPC_ADDR"),
	}, logger)

	logger.Info("cdrom worker starting", "job", jobID, "name", cfg.WorkerName, "api", cfg.APIAddress)
	status, err := a.Run(ctx)
	if err != nil {
		logger.Error("worker: run", "err", err)
		os.Exit(1)
	}
	logger.Info("cdrom worker finished", "job", jobID, "status", fmt.Sprint(status))
	// EXIT CODE ENCODES THE OUTCOME (F-30.4): the supervising agent reads it
	// to classify the job for its own cdrom_agent_jobs_total metric (the
	// worker's own metrics export is retired — the agent rolls the outcome
	// up). 0 succeeded, 1 failed, 3 cancelled, 4 timed_out. Codes 2/-1
	// (Go panic / signal-killed) collapse to failed on the agent side.
	switch status {
	case dbpb.JobStatus_JOB_STATUS_SUCCEEDED:
		os.Exit(0)
	case dbpb.JobStatus_JOB_STATUS_FAILED:
		os.Exit(1)
	case dbpb.JobStatus_JOB_STATUS_TIMED_OUT:
		os.Exit(4)
	case dbpb.JobStatus_JOB_STATUS_CANCELLED:
		os.Exit(3)
	default:
		os.Exit(1)
	}
}
