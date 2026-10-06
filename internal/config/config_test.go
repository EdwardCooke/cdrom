package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.DBAddress != DefaultDBAddress {
		t.Errorf("DBAddress = %q, want %q", cfg.DBAddress, DefaultDBAddress)
	}
	if cfg.SchedulerAddress != DefaultSchedulerAddress {
		t.Errorf("SchedulerAddress = %q, want %q", cfg.SchedulerAddress, DefaultSchedulerAddress)
	}
	if cfg.ArtifactsAddress != DefaultArtifactsAddress {
		t.Errorf("ArtifactsAddress = %q, want %q", cfg.ArtifactsAddress, DefaultArtifactsAddress)
	}
	if cfg.APIAddress != DefaultAPIAddress {
		t.Errorf("APIAddress = %q, want %q", cfg.APIAddress, DefaultAPIAddress)
	}
	if cfg.APIHTTPAddress != DefaultAPIHTTPAddress {
		t.Errorf("APIHTTPAddress = %q, want %q", cfg.APIHTTPAddress, DefaultAPIHTTPAddress)
	}
	if cfg.ListenAddress != "" {
		t.Errorf("ListenAddress = %q, want empty", cfg.ListenAddress)
	}
	if cfg.ArtifactsRoot != "artifacts" {
		t.Errorf("ArtifactsRoot = %q, want %q", cfg.ArtifactsRoot, "artifacts")
	}
	if cfg.WorkerName != "worker-1" {
		t.Errorf("WorkerName = %q, want %q", cfg.WorkerName, "worker-1")
	}
	if cfg.WorkerGroup != "default" {
		t.Errorf("WorkerGroup = %q, want %q", cfg.WorkerGroup, "default")
	}
	if cfg.AgentJobID != "" {
		t.Errorf("AgentJobID = %q, want empty", cfg.AgentJobID)
	}
	if cfg.DB.Backend != "sqlite" {
		t.Errorf("DB.Backend = %q, want sqlite", cfg.DB.Backend)
	}
	if cfg.DB.SQLitePath != "cdrom.db" {
		t.Errorf("DB.SQLitePath = %q, want cdrom.db", cfg.DB.SQLitePath)
	}
}

func TestLoadWithFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	content := `
listen_address: 127.0.0.1:9999
db_address: 127.0.0.1:7201
artifacts_root: /var/cdrom/artifacts
worker_name: worker-9
worker_group: batch
agent_job_id: "42"
db:
  backend: postgres
  postgres_dsn: "host=db user=u dbname=d"
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadWithFile(path)
	if err != nil {
		t.Fatalf("LoadWithFile: %v", err)
	}
	if cfg.ListenAddress != "127.0.0.1:9999" {
		t.Errorf("ListenAddress = %q, want 127.0.0.1:9999", cfg.ListenAddress)
	}
	if cfg.DBAddress != "127.0.0.1:7201" {
		t.Errorf("DBAddress = %q, want 127.0.0.1:7201", cfg.DBAddress)
	}
	// Keys absent from the file keep their defaults.
	if cfg.SchedulerAddress != DefaultSchedulerAddress {
		t.Errorf("SchedulerAddress = %q, want %q", cfg.SchedulerAddress, DefaultSchedulerAddress)
	}
	if cfg.APIAddress != DefaultAPIAddress {
		t.Errorf("APIAddress = %q, want %q", cfg.APIAddress, DefaultAPIAddress)
	}
	if cfg.ArtifactsRoot != "/var/cdrom/artifacts" {
		t.Errorf("ArtifactsRoot = %q, want /var/cdrom/artifacts", cfg.ArtifactsRoot)
	}
	if cfg.WorkerName != "worker-9" {
		t.Errorf("WorkerName = %q, want worker-9", cfg.WorkerName)
	}
	if cfg.WorkerGroup != "batch" {
		t.Errorf("WorkerGroup = %q, want batch", cfg.WorkerGroup)
	}
	if cfg.AgentJobID != "42" {
		t.Errorf("AgentJobID = %q, want 42", cfg.AgentJobID)
	}
	if cfg.DB.Backend != "postgres" {
		t.Errorf("DB.Backend = %q, want postgres", cfg.DB.Backend)
	}
	if cfg.DB.PostgresDSN != "host=db user=u dbname=d" {
		t.Errorf("DB.PostgresDSN = %q, want host=db user=u dbname=d", cfg.DB.PostgresDSN)
	}
}

func TestEnvOverridesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	content := "worker_name: from-file\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("CDROM_WORKER_NAME", "from-env")

	cfg, err := LoadWithFile(path)
	if err != nil {
		t.Fatalf("LoadWithFile: %v", err)
	}
	if cfg.WorkerName != "from-env" {
		t.Errorf("WorkerName = %q, want from-env (env must override file)", cfg.WorkerName)
	}
}

func TestAgentNameDefaultAndOverride(t *testing.T) {
	// The default agent name is the host name the agent runs on.
	host, err := os.Hostname()
	if err != nil || host == "" {
		t.Skip("hostname unavailable; cannot assert the default agent name")
	}
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.AgentName != host {
		t.Errorf("AgentName = %q, want the host name %q", cfg.AgentName, host)
	}

	// An environment variable overrides the default.
	t.Setenv("CDROM_AGENT_NAME", "agent-from-env")
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.AgentName != "agent-from-env" {
		t.Errorf("AgentName = %q, want agent-from-env (env must override the default)", cfg.AgentName)
	}
}

func TestLoadWithFileMissing(t *testing.T) {
	if _, err := LoadWithFile(filepath.Join(t.TempDir(), "nope.yaml")); err == nil {
		t.Fatal("LoadWithFile: expected error for missing file")
	}
}

func TestLoadWithFileInvalidYAML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("worker_name: [unclosed"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if _, err := LoadWithFile(path); err == nil {
		t.Fatal("LoadWithFile: expected error for invalid YAML")
	}
}

func TestGRPCAuthExchangedTokenLifetime(t *testing.T) {
	// Zero value falls back to the default (15 minutes).
	var zero GRPCAuthConfig
	if got := zero.EffectiveExchangedTokenLifetime(); got != defaultExchangedTokenLifetime {
		t.Errorf("EffectiveExchangedTokenLifetime (unset) = %v, want %v", got, defaultExchangedTokenLifetime)
	}
	// A configured value is used as-is.
	if got := (GRPCAuthConfig{ExchangedTokenLifetime: 30 * time.Minute}).EffectiveExchangedTokenLifetime(); got != 30*time.Minute {
		t.Errorf("EffectiveExchangedTokenLifetime (30m) = %v, want 30m", got)
	}

	// The file value is parsed from a duration string.
	path := filepath.Join(t.TempDir(), "config.yaml")
	content := `
grpc_auth:
  enabled: true
  idp_address: 127.0.0.1:7104
  audiences: [cdrom-api]
  exchanged_token_lifetime: 20m
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := LoadWithFile(path)
	if err != nil {
		t.Fatalf("LoadWithFile: %v", err)
	}
	if cfg.GRPCAuth.ExchangedTokenLifetime != 20*time.Minute {
		t.Errorf("GRPCAuth.ExchangedTokenLifetime = %v, want 20m", cfg.GRPCAuth.ExchangedTokenLifetime)
	}

	// The environment variable overrides the file value.
	t.Setenv("CDROM_GRPC_AUTH_EXCHANGED_TOKEN_LIFETIME", "45m")
	cfg, err = LoadWithFile(path)
	if err != nil {
		t.Fatalf("LoadWithFile: %v", err)
	}
	if cfg.GRPCAuth.ExchangedTokenLifetime != 45*time.Minute {
		t.Errorf("GRPCAuth.ExchangedTokenLifetime = %v, want 45m (env must override file)", cfg.GRPCAuth.ExchangedTokenLifetime)
	}

	// An unparseable duration in the file is a hard error.
	badPath := filepath.Join(t.TempDir(), "bad.yaml")
	if err := os.WriteFile(badPath, []byte("grpc_auth:\n  exchanged_token_lifetime: not-a-duration\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if _, err := LoadWithFile(badPath); err == nil {
		t.Error("LoadWithFile: expected error for invalid exchanged_token_lifetime")
	}
}

func TestParseFlags(t *testing.T) {
	got, err := ParseFlags([]string{"--config-file", "cfg.yaml"})
	if err != nil {
		t.Fatalf("ParseFlags: %v", err)
	}
	if got != "cfg.yaml" {
		t.Errorf("ParseFlags = %q, want cfg.yaml", got)
	}
	got, err = ParseFlags(nil)
	if err != nil {
		t.Fatalf("ParseFlags: %v", err)
	}
	if got != "" {
		t.Errorf("ParseFlags = %q, want empty", got)
	}
	if _, err := ParseFlags([]string{"--bogus"}); err == nil {
		t.Error("ParseFlags: expected error for unknown flag")
	}
}
