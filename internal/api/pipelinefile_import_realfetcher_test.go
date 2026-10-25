package api

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pipelinesourcepb "cdrom/internal/gen/cdrom/pipelinesource/v1"
	"cdrom/internal/services/pipelinesource"
	"cdrom/internal/sourceref"
)

// writeFileTree lays down a tree of unit files under root, mimicking a
// checked-in shared/ directory (F-21.4). Paths are forward-slash relative.
func writeFileTree(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for rel, content := range files {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", p, err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
	}
}

// TestCompilePipelineFileRealFetcherIntegration runs the full F-21.4 path
// against a REAL source-fetcher (a pipelinesource.Server over a loopback gRPC
// socket, file kind rooted at a temp dir): the API's fetcher dials the
// fetcher, the fetcher reads the files from disk, and the compiler splices the
// imported units in place. This proves the API↔fetcher glue and the
// relative-locator resolution (against the pipeline's file: source) end to
// end.
func TestCompilePipelineFileRealFetcherIntegration(t *testing.T) {
	tmp := t.TempDir()
	fsRoot := filepath.Join(tmp, "fsroot")
	writeFileTree(t, fsRoot, map[string]string{
		"pipelines/webapp.yaml": `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: webapp
spec:
  steps:
    build:
      shell: make build
    unit-test:
      importFrom: ../shared/steps/unit-test.yaml
  variableSets:
    dev:
      import:
        - from: ../variablesets/dev.yaml
  jobs:
    build:
      steps:
        b:
          shell: make build
    deploy-dev:
      importFrom: ../jobs/deploy-dev.yaml
      environment: staging
`,
		"shared/steps/unit-test.yaml": `
apiVersion: cdrom/alphav1
kind: Step
metadata:
  name: unit-test
spec:
  shell:
    command: make test
  workdir: /src
`,
		"variablesets/dev.yaml": `
apiVersion: cdrom/alphav1
kind: VariableSet
metadata:
  name: dev
spec:
  variables:
    region: eu-west
`,
		"jobs/deploy-dev.yaml": `
apiVersion: cdrom/alphav1
kind: Job
metadata:
  name: deploy-dev
spec:
  steps:
    step1:
      shell: echo "real fetcher step1"
  variableSets:
    - dev
`,
	})

	// Boot the real source-fetcher (file kind rooted at fsRoot) on a loopback
	// gRPC socket.
	fetcherServer, err := pipelinesource.NewServer(filepath.Join(tmp, "repos"), fsRoot, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pipelinesourcepb.RegisterPipelineSourceServer(grpcServer, fetcherServer)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	// NewClient connects lazily; the first FetchSource initiates the dial.
	conn, err := grpc.NewClient("passthrough:"+lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial fetcher: %v", err)
	}
	defer conn.Close()

	// The API with the real fetcher wired in (no database: a public source
	// needs no credential).
	srv := &Server{clients: Clients{PipelineSource: pipelinesourcepb.NewPipelineSourceClient(conn)}}

	// The main file's source is a file: reference rooted at fsRoot; feed the
	// compiler the main file's contents straight from disk (as the sync
	// routine would after a FetchSource of the pipeline's source).
	rootRef, err := sourceref.ParseReference("file:/pipelines/webapp.yaml")
	if err != nil {
		t.Fatalf("parse root: %v", err)
	}
	mainData, err := os.ReadFile(filepath.Join(fsRoot, "pipelines", "webapp.yaml"))
	if err != nil {
		t.Fatalf("read main file: %v", err)
	}
	req, variableSets, _, err := srv.compilePipelineFile(context.Background(), "webapp.yaml", mainData, rootRef)
	if err != nil {
		t.Fatalf("compilePipelineFile(real fetcher): %v", err)
	}

	// The imported variable set's variables landed in the compiled set.
	var devRegion string
	for _, s := range variableSets {
		if s.GetName() != "dev" {
			continue
		}
		for _, v := range s.GetVariables() {
			if v.GetName() == "region" {
				devRegion = v.GetPlain()
			}
		}
	}
	if devRegion != "eu-west" {
		t.Errorf("dev.region = %q, want the imported 'eu-west' (fetched from disk by the real fetcher)", devRegion)
	}

	// The imported job is spliced in with its local environment override and
	// its imported step.
	var deployDev *pipelineJobRequest
	for i := range req.Jobs {
		if req.Jobs[i].Key == "deploy-dev" {
			deployDev = &req.Jobs[i]
		}
	}
	if deployDev == nil {
		t.Fatal("no deploy-dev job compiled from the real fetcher")
	}
	if deployDev.Environment != "staging" {
		t.Errorf("deploy-dev.environment = %q, want the local override 'staging'", deployDev.Environment)
	}
	stepNames := map[string]bool{}
	for _, st := range deployDev.Spec.Steps {
		stepNames[st.Name] = true
	}
	if !stepNames["step1"] {
		t.Errorf("deploy-dev steps lack the imported step1: %v", stepNames)
	}
}
