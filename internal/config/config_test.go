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
	// Username/password auth is enabled by default for local development.
	if !cfg.Auth.UserPassEnabled {
		t.Errorf("Auth.UserPassEnabled = false, want true (default)")
	}
}

func TestUserPassEnabledConfig(t *testing.T) {
	// Disabled via the config file.
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("auth:\n  userpass_enabled: false\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := LoadWithFile(path)
	if err != nil {
		t.Fatalf("LoadWithFile: %v", err)
	}
	if cfg.Auth.UserPassEnabled {
		t.Errorf("UserPassEnabled = true, want false (from file)")
	}

	// Overridden by the environment.
	t.Setenv("CDROM_AUTH_USERPASS_ENABLED", "true")
	cfg, err = LoadWithFile(path)
	if err != nil {
		t.Fatalf("LoadWithFile: %v", err)
	}
	if !cfg.Auth.UserPassEnabled {
		t.Errorf("UserPassEnabled = false, want true (env must override file)")
	}
}

func TestAPIKeyConfig(t *testing.T) {
	// API-key auth is enabled by default for local development.
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.Auth.APIKey.Enabled {
		t.Errorf("Auth.APIKey.Enabled = false, want true (default)")
	}

	// Set via the config file.
	path := filepath.Join(t.TempDir(), "config.yaml")
	content := `
auth:
  api_key:
    enabled: false
    max_failures: 5
    lockout_duration: 15m
    pepper: s3cret-pepper
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err = LoadWithFile(path)
	if err != nil {
		t.Fatalf("LoadWithFile: %v", err)
	}
	if cfg.Auth.APIKey.Enabled {
		t.Errorf("APIKey.Enabled = true, want false (from file)")
	}
	if cfg.Auth.APIKey.MaxFailures != 5 {
		t.Errorf("APIKey.MaxFailures = %d, want 5 (from file)", cfg.Auth.APIKey.MaxFailures)
	}
	if cfg.Auth.APIKey.LockoutDuration != 15*time.Minute {
		t.Errorf("APIKey.LockoutDuration = %v, want 15m (from file)", cfg.Auth.APIKey.LockoutDuration)
	}
	if cfg.Auth.APIKey.Pepper != "s3cret-pepper" {
		t.Errorf("APIKey.Pepper = %q, want s3cret-pepper (from file)", cfg.Auth.APIKey.Pepper)
	}

	// Overridden by the environment.
	t.Setenv("CDROM_AUTH_APIKEY_ENABLED", "true")
	t.Setenv("CDROM_AUTH_APIKEY_MAX_FAILURES", "3")
	t.Setenv("CDROM_AUTH_APIKEY_LOCKOUT_DURATION", "1h")
	t.Setenv("CDROM_AUTH_APIKEY_PEPPER", "env-pepper")
	cfg, err = LoadWithFile(path)
	if err != nil {
		t.Fatalf("LoadWithFile: %v", err)
	}
	if !cfg.Auth.APIKey.Enabled {
		t.Errorf("APIKey.Enabled = false, want true (env must override file)")
	}
	if cfg.Auth.APIKey.MaxFailures != 3 {
		t.Errorf("APIKey.MaxFailures = %d, want 3 (env must override file)", cfg.Auth.APIKey.MaxFailures)
	}
	if cfg.Auth.APIKey.LockoutDuration != time.Hour {
		t.Errorf("APIKey.LockoutDuration = %v, want 1h (env must override file)", cfg.Auth.APIKey.LockoutDuration)
	}
	if cfg.Auth.APIKey.Pepper != "env-pepper" {
		t.Errorf("APIKey.Pepper = %q, want env-pepper (env must override file)", cfg.Auth.APIKey.Pepper)
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

func TestPipelineSourceConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	content := `
pipeline_source:
  repo_cache_dir: /var/cdrom/repos
  file_root: /shared
  refresh_interval: 90s
  overrides_allowed: false
  max_parallel: 16
  sync_timeout: 7m
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := LoadWithFile(path)
	if err != nil {
		t.Fatalf("LoadWithFile: %v", err)
	}
	if cfg.PipelineSourceRepoCacheDir != "/var/cdrom/repos" {
		t.Errorf("PipelineSourceRepoCacheDir = %q, want /var/cdrom/repos", cfg.PipelineSourceRepoCacheDir)
	}
	if cfg.PipelineSourceFileRoot != "/shared" {
		t.Errorf("PipelineSourceFileRoot = %q, want /shared", cfg.PipelineSourceFileRoot)
	}
	// The address defaults to the pipeline source default.
	if cfg.PipelineSourceAddress != DefaultPipelineSourceAddress {
		t.Errorf("PipelineSourceAddress = %q, want %q", cfg.PipelineSourceAddress, DefaultPipelineSourceAddress)
	}
	// The externally-sourced-pipeline refresh knobs (F-21.5.1 / F-21.5.3).
	if cfg.PipelineSourceRefreshInterval != 90*time.Second {
		t.Errorf("PipelineSourceRefreshInterval = %v, want 90s", cfg.PipelineSourceRefreshInterval)
	}
	if cfg.PipelineSourceOverridesAllowed {
		t.Error("PipelineSourceOverridesAllowed = true, want false (file)")
	}
	if cfg.PipelineSourceMaxParallel != 16 {
		t.Errorf("PipelineSourceMaxParallel = %d, want 16 (file)", cfg.PipelineSourceMaxParallel)
	}
	if cfg.PipelineSourceSyncTimeout != 7*time.Minute {
		t.Errorf("PipelineSourceSyncTimeout = %v, want 7m (file)", cfg.PipelineSourceSyncTimeout)
	}

	// Env overrides the file.
	t.Setenv("CDROM_PIPELINE_SOURCE_ADDR", "127.0.0.1:7999")
	t.Setenv("CDROM_PIPELINE_SOURCE_FILE_ROOT", "/other")
	t.Setenv("CDROM_PIPELINE_SOURCE_REFRESH_INTERVAL", "45s")
	t.Setenv("CDROM_PIPELINE_SOURCE_OVERRIDES_ALLOWED", "true")
	t.Setenv("CDROM_PIPELINE_SOURCE_MAX_PARALLEL", "0")
	t.Setenv("CDROM_PIPELINE_SOURCE_SYNC_TIMEOUT", "3m")
	cfg, err = LoadWithFile(path)
	if err != nil {
		t.Fatalf("LoadWithFile (env): %v", err)
	}
	if cfg.PipelineSourceAddress != "127.0.0.1:7999" {
		t.Errorf("PipelineSourceAddress = %q, want 127.0.0.1:7999", cfg.PipelineSourceAddress)
	}
	if cfg.PipelineSourceFileRoot != "/other" {
		t.Errorf("PipelineSourceFileRoot = %q, want /other", cfg.PipelineSourceFileRoot)
	}
	// The refresh knobs: env overrides the file (F-21.5.1 / F-21.5.3).
	if cfg.PipelineSourceRefreshInterval != 45*time.Second {
		t.Errorf("PipelineSourceRefreshInterval = %v, want 45s (env)", cfg.PipelineSourceRefreshInterval)
	}
	if !cfg.PipelineSourceOverridesAllowed {
		t.Error("PipelineSourceOverridesAllowed = false, want true (env)")
	}
	if cfg.PipelineSourceMaxParallel != 0 {
		t.Errorf("PipelineSourceMaxParallel = %d, want 0 (env)", cfg.PipelineSourceMaxParallel)
	}
	if cfg.PipelineSourceSyncTimeout != 3*time.Minute {
		t.Errorf("PipelineSourceSyncTimeout = %v, want 3m (env)", cfg.PipelineSourceSyncTimeout)
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

func TestWorkRootDefaultAndOverride(t *testing.T) {
	// The default work root is empty (the target uses the OS temp dir).
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.WorkRoot != "" {
		t.Errorf("WorkRoot = %q, want empty (default)", cfg.WorkRoot)
	}

	// An environment variable overrides the default.
	t.Setenv("CDROM_WORK_ROOT", "/scratch/disks")
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.WorkRoot != "/scratch/disks" {
		t.Errorf("WorkRoot = %q, want /scratch/disks (env must override the default)", cfg.WorkRoot)
	}

	// A config file sets it; the env var (higher precedence) still wins.
	path := filepath.Join(t.TempDir(), "cfg.yaml")
	yamlDoc := "work_root: /file/root\n"
	if err := os.WriteFile(path, []byte(yamlDoc), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err = LoadWithFile(path)
	if err != nil {
		t.Fatalf("LoadWithFile: %v", err)
	}
	if cfg.WorkRoot != "/scratch/disks" {
		t.Errorf("WorkRoot = %q, want /scratch/disks (env must override file)", cfg.WorkRoot)
	}
	// Without the env var, the file value applies.
	t.Setenv("CDROM_WORK_ROOT", "")
	cfg, err = LoadWithFile(path)
	if err != nil {
		t.Fatalf("LoadWithFile: %v", err)
	}
	if cfg.WorkRoot != "/file/root" {
		t.Errorf("WorkRoot = %q, want /file/root (file must override the default)", cfg.WorkRoot)
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

func TestMetricsConfig(t *testing.T) {
	// Metrics are enabled by default (F-20): every binary serves /metrics and
	// /healthz on its dedicated metrics port.
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.Metrics.Enabled {
		t.Errorf("Metrics.Enabled = false, want true (default)")
	}

	// Disabled via the environment (CDROM_METRICS_ENABLED=false).
	t.Setenv("CDROM_METRICS_ENABLED", "false")
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Metrics.Enabled {
		t.Errorf("Metrics.Enabled = true, want false (env CDROM_METRICS_ENABLED=false)")
	}

	// Re-enabled via the environment (CDROM_METRICS_ENABLED=true).
	t.Setenv("CDROM_METRICS_ENABLED", "true")
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.Metrics.Enabled {
		t.Errorf("Metrics.Enabled = false, want true (env CDROM_METRICS_ENABLED=true)")
	}

	// Disabled via the config file. Clear the env var (t.Setenv does not
	// auto-clear between subtests) so the file value is what's under test.
	t.Setenv("CDROM_METRICS_ENABLED", "")
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("metrics:\n  enabled: false\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err = LoadWithFile(path)
	if err != nil {
		t.Fatalf("LoadWithFile: %v", err)
	}
	if cfg.Metrics.Enabled {
		t.Errorf("Metrics.Enabled = true, want false (from file)")
	}

	// The environment overrides the file.
	t.Setenv("CDROM_METRICS_ENABLED", "true")
	cfg, err = LoadWithFile(path)
	if err != nil {
		t.Fatalf("LoadWithFile: %v", err)
	}
	if !cfg.Metrics.Enabled {
		t.Errorf("Metrics.Enabled = false, want true (env must override file)")
	}

	// The metrics listen address is set from the environment.
	t.Setenv("CDROM_METRICS_ADDR", "127.0.0.1:9999")
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Metrics.Addr != "127.0.0.1:9999" {
		t.Errorf("Metrics.Addr = %q, want 127.0.0.1:9999 (env CDROM_METRICS_ADDR)", cfg.Metrics.Addr)
	}
}
