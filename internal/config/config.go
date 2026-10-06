// Package config provides shared configuration loading for all cdrom
// binaries (api, worker, agent, and the gRPC service servers).
//
// Configuration is resolved in three layers, in increasing order of
// precedence:
//
//  1. built-in defaults, suitable for a local development setup where
//     every service runs on localhost;
//  2. an optional YAML file, passed with the --config-file command-line
//     flag (see ParseFlags);
//  3. environment variables, which override both.
//
// When a TLS CA certificate is configured (see TLSConfig), all gRPC
// communication between services uses mutual TLS; otherwise it falls back
// to plaintext.
package config

import (
	"encoding/base64"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"cdrom/internal/services/database"
)

// Default addresses of the cdrom services, used when the corresponding
// value is not set in a config file or environment variable.
const (
	DefaultDBAddress        = "127.0.0.1:7101"
	DefaultSchedulerAddress = "127.0.0.1:7102"
	DefaultArtifactsAddress = "127.0.0.1:7103"
	// DefaultAPIAddress is the gRPC address of the API service (the control
	// plane that workers, agents, and the scheduler dial).
	DefaultAPIAddress = "127.0.0.1:7105"
	// DefaultAPIHTTPAddress is the HTTP address the API binds for the UI.
	DefaultAPIHTTPAddress = "127.0.0.1:8080"
	// DefaultIdPAddress is the HTTP address the local OIDC identity provider
	// (cmd/idp) binds to.
	DefaultIdPAddress = "127.0.0.1:7104"
)

// Config is the shared configuration for every cdrom binary.
type Config struct {
	// DBAddress is the gRPC address of the database service.
	DBAddress string
	// SchedulerAddress is the gRPC address of the scheduler service.
	SchedulerAddress string
	// ArtifactsAddress is the gRPC address of the artifacts service.
	ArtifactsAddress string
	// APIAddress is the gRPC address of the API service (the control plane
	// that workers, agents, and the scheduler dial).
	APIAddress string
	// APIHTTPAddress is the HTTP address the API binds for the UI.
	APIHTTPAddress string

	// ListenAddress is the address this binary's own server binds to (the
	// gRPC port for service servers, and the gRPC port for the API). Empty
	// means the binary must supply its own default.
	ListenAddress string

	// DB configures the storage backend (used by the database service).
	DB database.Config

	// ArtifactsRoot is the filesystem root for artifact storage (used by
	// the artifacts service).
	ArtifactsRoot string

	// ArtifactsStore selects the artifacts service's storage backend
	// (e.g. "filesystem", the default; "s3" and "azureblob" are reserved
	// for future built-in implementations). Only the artifacts service uses
	// it; other binaries ignore it.
	ArtifactsStore string

	// WorkerName and WorkerGroup identify a worker process.
	WorkerName  string
	WorkerGroup string

	// AgentJobID is the job an ephemeral agent was spawned to execute.
	AgentJobID string

	// AgentName identifies an ephemeral agent process. It defaults to the host
	// name the agent runs on (see defaultAgentName); it is reported with the
	// job's status so the API can record which target ran the job.
	AgentName string

	// TLS configures mutual TLS for gRPC communication between services.
	// When CAFile is set, all gRPC connections use mTLS; otherwise they
	// fall back to plaintext.
	TLS TLSConfig

	// Auth configures OIDC authentication for the UI-facing HTTP API and
	// the WebSocket endpoint. Disabled by default so a local run works with
	// no identity provider; enable it for deployments that need it.
	Auth AuthConfig

	// IdP configures the local OIDC identity provider (cmd/idp), which acts
	// as a JWT issuer for the API's OIDC authentication. It is only used by
	// the idp binary; other binaries ignore it.
	IdP IdPConfig

	// GRPCAuth configures job-token authentication on the API's gRPC surface
	// (the control plane that workers and agents talk to). When enabled, the
	// API verifies the job tokens it hands to execution targets (minted by the
	// IdP) on job-scoped calls. Disabled by default so a local run works with
	// no identity provider.
	GRPCAuth GRPCAuthConfig

	// Secrets configures the secret store the API uses to encrypt and decrypt
	// pipeline secrets (F-12). The API is the only component that holds the
	// key: it encrypts a secret's plaintext when a pipeline is created or
	// updated, and decrypts the stored ciphertext when it hands a job to an
	// execution target. The database service stores the ciphertext opaquely
	// and never sees the key. Disabled by default (no key) so a local run
	// works without secrets; a pipeline that declares secrets requires the
	// key to be set.
	Secrets SecretsConfig
}

// AuthConfig configures OIDC authentication for the API's HTTP surface (the
// UI) and its WebSocket endpoint. When Enabled is false the API is open and
// no identity provider is required.
type AuthConfig struct {
	// Enabled turns OIDC authentication on. When false (the default) the API
	// does not require authentication.
	Enabled bool
	// Issuer is the OIDC issuer URL (the identity provider's base URL, e.g.
	// https://accounts.example.com or a Keycloak realm URL). Required when
	// Enabled is true.
	Issuer string
	// ClientID is the OIDC client ID registered with the identity provider.
	// Required when Enabled is true.
	ClientID string
	// RedirectURL is the absolute URL the identity provider redirects back to
	// after the client completes the authorization flow. It is advertised in
	// the API's /api/auth/oidc discovery document so a client (the UI or the
	// OpenAPI viewer) knows where to send the IdP's redirect. Required when
	// Enabled is true.
	RedirectURL string
	// Scopes are the OIDC scopes to request. Defaults to openid profile email.
	Scopes []string
	// TokenAudience is the audience the API accepts on the OAuth tokens it
	// verifies (the IdP stamps this on the tokens it issues for the UI). When
	// empty the API accepts the token's audience as-is (the verifier checks
	// it against ClientID). Optional.
	TokenAudience string
}

// GRPCAuthConfig configures job-token authentication on the API's gRPC
// surface. The API mints a short-lived job token (via the IdP, over mTLS) for
// each job and hands it to the execution target; when enabled, the API
// verifies that token on job-scoped calls (ReportJobStatus, artifact access)
// so a target can only act on the job it was given. The token is also what a
// target presents to outside resources, and it can be exchanged (via the API)
// for a token with a different audience.
type GRPCAuthConfig struct {
	// Enabled turns job-token verification on. When false (the default) the
	// API's gRPC surface is open.
	Enabled bool
	// IdPAddress is the address of the IdP that mints job tokens (the API
	// dials it over mTLS to request tokens for dispatched jobs). The API's
	// TLS client certificate is how it authenticates to the IdP — the IdP
	// accepts job-token requests only from mTLS clients.
	IdPAddress string
	// Audiences are the audience values accepted on job tokens. A token is
	// valid for the API when its aud contains any of these.
	Audiences []string
	// ExchangedTokenLifetime is the default lifetime of an exchanged job token
	// (one a job requests for a different audience via the API's
	// ExchangeJobToken RPC). Unlike the main job token, an exchanged token is a
	// scoped credential for an outside resource and SHOULD expire. When zero the
	// API uses its default (15 minutes); a job may override it per request via
	// the request's expires_in.
	ExchangedTokenLifetime time.Duration
}

// defaultExchangedTokenLifetime is the lifetime of an exchanged job token when
// GRPCAuthConfig.ExchangedTokenLifetime is zero.
const defaultExchangedTokenLifetime = 15 * time.Minute

// EffectiveExchangedTokenLifetime returns the configured exchanged-token
// lifetime, or the default (15 minutes) when unset.
func (g GRPCAuthConfig) EffectiveExchangedTokenLifetime() time.Duration {
	if g.ExchangedTokenLifetime <= 0 {
		return defaultExchangedTokenLifetime
	}
	return g.ExchangedTokenLifetime
}

// Validate checks that the gRPC auth configuration is complete when enabled.
func (g GRPCAuthConfig) Validate() error {
	if !g.Enabled {
		return nil
	}
	if g.IdPAddress == "" {
		return fmt.Errorf("grpc_auth: idp_address is required when enabled")
	}
	if len(g.Audiences) == 0 {
		return fmt.Errorf("grpc_auth: audiences is required when enabled")
	}
	return nil
}

// SecretsConfig configures the secret store the API uses to encrypt and
// decrypt pipeline secrets (F-12). The API is the only component that holds
// the key: it encrypts a secret's plaintext when a pipeline is created or
// updated, and decrypts the stored ciphertext when it hands a job to an
// execution target. The database service stores the ciphertext opaquely and
// never sees the key.
//
// Kind selects the store backend (e.g. "aes", the built-in AES-256-GCM store;
// "vault" and "openbao" are reserved for future first-class backends). Key is
// the store's key material: for the built-in AES store it is a base64-encoded
// 32-byte AES-256 key. When Key is empty the built-in AES store falls back to
// an all-zero key, so secrets can be exercised in test / local-dev scenarios
// without a real key (the zero key provides no real security).
type SecretsConfig struct {
	// Kind selects the secret store backend (case-insensitive). Empty means
	// the built-in "aes" store.
	Kind string
	// Key is the store's key material. For the built-in AES store it is a
	// base64-encoded 32-byte AES-256 key. Empty makes the built-in store fall
	// back to an all-zero key (test / local-dev convenience).
	Key string
}

// EffectiveKind returns the configured store kind, or the built-in "aes" kind
// when Kind is empty.
func (s SecretsConfig) EffectiveKind() string {
	if s.Kind == "" {
		return "aes"
	}
	return s.Kind
}

// Validate checks that the secrets configuration is sane. An empty key is
// allowed (the built-in store falls back to an all-zero key); a set key must
// be valid base64 that decodes to a 32-byte AES-256 key (for the built-in
// store).
func (s SecretsConfig) Validate() error {
	if s.Key == "" {
		return nil
	}
	key, err := base64.StdEncoding.DecodeString(s.Key)
	if err != nil {
		return fmt.Errorf("secrets: key is not valid base64: %w", err)
	}
	if len(key) != 32 {
		return fmt.Errorf("secrets: key must decode to 32 bytes (AES-256), got %d", len(key))
	}
	return nil
}

// IdPConfig configures the local OIDC identity provider (cmd/idp). The IdP
// issues signed JWTs (ID tokens) for the API's OIDC authentication and
// automatically rotates its RSA signing key: a new key is generated when the
// current one is within RotateBefore of its expiry, and the JWKS always
// serves the current key plus any not-yet-expired predecessors so clients can
// keep verifying tokens signed with an older key during the overlap window.
type IdPConfig struct {
	// Issuer is the OIDC issuer identifier advertised in the discovery
	// document and the iss claim of issued tokens. It should be the base URL
	// clients use to reach the IdP (e.g. http://127.0.0.1:7104).
	Issuer string
	// KeyLifetime is how long a signing key is valid for.
	KeyLifetime time.Duration
	// RotateBefore is how long before a key's expiry a new key is generated
	// (the rotation lead time). The JWKS serves both keys during the overlap.
	RotateBefore time.Duration
	// TokenLifetime is how long issued ID tokens are valid for.
	TokenLifetime time.Duration
	// CheckInterval is how often the IdP checks whether the current key needs
	// rotating.
	CheckInterval time.Duration
	// Audiences are the audience values the IdP stamps on job tokens it mints
	// (e.g. the API's audience plus any outside resources a job may call).
	// A job may also request a token for a specific audience via the API's
	// ExchangeJobToken RPC; the IdP stamps whatever audience the API asks for.
	Audiences []string
}

// defaultIdPIssuer is the issuer advertised when IdPConfig.Issuer is empty.
const defaultIdPIssuer = "http://127.0.0.1:7104"

// EffectiveIssuer returns the configured issuer or the default.
func (c IdPConfig) EffectiveIssuer() string {
	if c.Issuer == "" {
		return defaultIdPIssuer
	}
	return c.Issuer
}

// Validate checks that the IdP configuration is sane. It is only enforced for
// the idp binary; the values are optional and fall back to defaults.
func (c IdPConfig) Validate() error {
	if c.KeyLifetime <= 0 {
		return fmt.Errorf("idp: key_lifetime must be positive")
	}
	if c.RotateBefore < 0 {
		return fmt.Errorf("idp: rotate_before must not be negative")
	}
	if c.RotateBefore >= c.KeyLifetime {
		return fmt.Errorf("idp: rotate_before must be less than key_lifetime")
	}
	if c.TokenLifetime <= 0 {
		return fmt.Errorf("idp: token_lifetime must be positive")
	}
	if c.CheckInterval <= 0 {
		return fmt.Errorf("idp: check_interval must be positive")
	}
	return nil
}

// defaultAuthScopes are requested when AuthConfig.Scopes is empty.
var defaultAuthScopes = []string{"openid", "profile", "email"}

// EffectiveScopes returns the configured scopes, or the defaults when none
// are set.
func (a AuthConfig) EffectiveScopes() []string {
	if len(a.Scopes) == 0 {
		return defaultAuthScopes
	}
	return a.Scopes
}

// Validate checks that the auth configuration is complete when enabled.
func (a AuthConfig) Validate() error {
	if !a.Enabled {
		return nil
	}
	if a.Issuer == "" {
		return fmt.Errorf("auth: issuer is required when auth is enabled")
	}
	if a.ClientID == "" {
		return fmt.Errorf("auth: client_id is required when auth is enabled")
	}
	if a.RedirectURL == "" {
		return fmt.Errorf("auth: redirect_url is required when auth is enabled")
	}
	return nil
}

// TLSConfig holds the certificate paths used for mutual TLS between
// services. Every service (server and client) loads the same CA, and each
// process presents its own certificate and key signed by that CA.
type TLSConfig struct {
	// CAFile is the path to the CA certificate used to verify peers. When
	// empty, gRPC communication is plaintext.
	CAFile string
	// CertFile is the path to this process's TLS certificate.
	CertFile string
	// KeyFile is the path to this process's TLS private key.
	KeyFile string
}

// Enabled reports whether mTLS is configured (a CA certificate is set).
func (t TLSConfig) Enabled() bool {
	return t.CAFile != ""
}

// ParseFlags parses the command-line flags shared by all cdrom binaries.
// It returns the path of the YAML config file given with --config-file
// (empty when the flag was not provided).
func ParseFlags(args []string) (string, error) {
	fs := flag.NewFlagSet("cdrom", flag.ContinueOnError)
	configFile := fs.String("config-file", "", "path to a YAML config file; values override built-in defaults and are overridden by environment variables")
	if err := fs.Parse(args); err != nil {
		return "", fmt.Errorf("config: %w", err)
	}
	return *configFile, nil
}

// Load reads configuration from the environment, falling back to defaults
// suitable for local development.
func Load() (*Config, error) {
	return LoadWithFile("")
}

// LoadWithFile reads configuration from the three layers described in the
// package comment. file is the path to an optional YAML config file; an
// empty string skips the file layer.
func LoadWithFile(file string) (*Config, error) {
	cfg := &Config{
		DBAddress:        DefaultDBAddress,
		SchedulerAddress: DefaultSchedulerAddress,
		ArtifactsAddress: DefaultArtifactsAddress,
		APIAddress:       DefaultAPIAddress,
		APIHTTPAddress:   DefaultAPIHTTPAddress,
		ArtifactsRoot:    "artifacts", ArtifactsStore: "filesystem", WorkerName: "worker-1",
		WorkerGroup: "default",
		AgentName:   defaultAgentName(),
		DB: database.Config{
			Backend:     database.BackendSQLite,
			SQLitePath:  "cdrom.db",
			PostgresDSN: "",
		},
		TLS:  TLSConfig{},
		Auth: AuthConfig{},
		IdP: IdPConfig{
			Issuer:        defaultIdPIssuer,
			KeyLifetime:   90 * 24 * time.Hour,
			RotateBefore:  14 * 24 * time.Hour,
			TokenLifetime: time.Hour,
			CheckInterval: time.Hour,
		},
		GRPCAuth: GRPCAuthConfig{},
		Secrets:  SecretsConfig{},
	}
	if file != "" {
		if err := applyFile(cfg, file); err != nil {
			return nil, err
		}
	}
	applyEnv(cfg)
	if err := cfg.DB.Validate(); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	if err := cfg.Auth.Validate(); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	if err := cfg.GRPCAuth.Validate(); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	if err := cfg.Secrets.Validate(); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	return cfg, nil
}

// fileConfig mirrors the YAML schema of a config file. Pointer fields
// distinguish "absent" from "set" so a file only overrides the values it
// actually contains.
type fileConfig struct {
	ListenAddress    *string `yaml:"listen_address"`
	DBAddress        *string `yaml:"db_address"`
	SchedulerAddress *string `yaml:"scheduler_address"`
	ArtifactsAddress *string `yaml:"artifacts_address"`
	APIAddress       *string `yaml:"api_address"`
	APIHTTPAddress   *string `yaml:"api_http_address"`
	ArtifactsRoot    *string `yaml:"artifacts_root"`
	ArtifactsStore   *string `yaml:"artifacts_store"`
	WorkerName       *string `yaml:"worker_name"`
	WorkerGroup      *string `yaml:"worker_group"`
	AgentJobID       *string `yaml:"agent_job_id"`
	AgentName        *string `yaml:"agent_name"`
	DB               *struct {
		Backend     *string `yaml:"backend"`
		SQLitePath  *string `yaml:"sqlite_path"`
		PostgresDSN *string `yaml:"postgres_dsn"`
	} `yaml:"db"`
	TLS *struct {
		CAFile   *string `yaml:"ca_file"`
		CertFile *string `yaml:"cert_file"`
		KeyFile  *string `yaml:"key_file"`
	} `yaml:"tls"`
	Auth *struct {
		Enabled       *bool    `yaml:"enabled"`
		Issuer        *string  `yaml:"issuer"`
		ClientID      *string  `yaml:"client_id"`
		RedirectURL   *string  `yaml:"redirect_url"`
		Scopes        []string `yaml:"scopes"`
		TokenAudience *string  `yaml:"token_audience"`
	} `yaml:"auth"`
	IdP *struct {
		Issuer        *string   `yaml:"issuer"`
		KeyLifetime   *string   `yaml:"key_lifetime"`
		RotateBefore  *string   `yaml:"rotate_before"`
		TokenLifetime *string   `yaml:"token_lifetime"`
		CheckInterval *string   `yaml:"check_interval"`
		Audiences     *[]string `yaml:"audiences"`
	} `yaml:"idp"`
	GRPCAuth *struct {
		Enabled                *bool    `yaml:"enabled"`
		IdPAddress             *string  `yaml:"idp_address"`
		Audiences              []string `yaml:"audiences"`
		ExchangedTokenLifetime *string  `yaml:"exchanged_token_lifetime"`
	} `yaml:"grpc_auth"`
	Secrets *struct {
		Kind *string `yaml:"kind"`
		Key  *string `yaml:"key"`
	} `yaml:"secrets"`
}

// applyFile overlays the values from the YAML file at path onto cfg.
func applyFile(cfg *Config, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("config: read %s: %w", path, err)
	}
	var f fileConfig
	if err := yaml.Unmarshal(data, &f); err != nil {
		return fmt.Errorf("config: parse %s: %w", path, err)
	}
	if f.ListenAddress != nil {
		cfg.ListenAddress = *f.ListenAddress
	}
	if f.DBAddress != nil {
		cfg.DBAddress = *f.DBAddress
	}
	if f.SchedulerAddress != nil {
		cfg.SchedulerAddress = *f.SchedulerAddress
	}
	if f.ArtifactsAddress != nil {
		cfg.ArtifactsAddress = *f.ArtifactsAddress
	}
	if f.APIAddress != nil {
		cfg.APIAddress = *f.APIAddress
	}
	if f.APIHTTPAddress != nil {
		cfg.APIHTTPAddress = *f.APIHTTPAddress
	}
	if f.ArtifactsRoot != nil {
		cfg.ArtifactsRoot = *f.ArtifactsRoot
	}
	if f.ArtifactsStore != nil {
		cfg.ArtifactsStore = *f.ArtifactsStore
	}
	if f.WorkerName != nil {
		cfg.WorkerName = *f.WorkerName
	}
	if f.WorkerGroup != nil {
		cfg.WorkerGroup = *f.WorkerGroup
	}
	if f.AgentJobID != nil {
		cfg.AgentJobID = *f.AgentJobID
	}
	if f.AgentName != nil {
		cfg.AgentName = *f.AgentName
	}
	if f.DB != nil {
		if f.DB.Backend != nil {
			cfg.DB.Backend = database.Backend(*f.DB.Backend)
		}
		if f.DB.SQLitePath != nil {
			cfg.DB.SQLitePath = *f.DB.SQLitePath
		}
		if f.DB.PostgresDSN != nil {
			cfg.DB.PostgresDSN = *f.DB.PostgresDSN
		}
	}
	if f.TLS != nil {
		if f.TLS.CAFile != nil {
			cfg.TLS.CAFile = *f.TLS.CAFile
		}
		if f.TLS.CertFile != nil {
			cfg.TLS.CertFile = *f.TLS.CertFile
		}
		if f.TLS.KeyFile != nil {
			cfg.TLS.KeyFile = *f.TLS.KeyFile
		}
	}
	if f.Auth != nil {
		if f.Auth.Enabled != nil {
			cfg.Auth.Enabled = *f.Auth.Enabled
		}
		if f.Auth.Issuer != nil {
			cfg.Auth.Issuer = *f.Auth.Issuer
		}
		if f.Auth.ClientID != nil {
			cfg.Auth.ClientID = *f.Auth.ClientID
		}
		if f.Auth.RedirectURL != nil {
			cfg.Auth.RedirectURL = *f.Auth.RedirectURL
		}
		if f.Auth.Scopes != nil {
			cfg.Auth.Scopes = f.Auth.Scopes
		}
		if f.Auth.TokenAudience != nil {
			cfg.Auth.TokenAudience = *f.Auth.TokenAudience
		}
	}
	if f.IdP != nil {
		if f.IdP.Issuer != nil {
			cfg.IdP.Issuer = *f.IdP.Issuer
		}
		if f.IdP.KeyLifetime != nil {
			if d, err := time.ParseDuration(*f.IdP.KeyLifetime); err == nil {
				cfg.IdP.KeyLifetime = d
			} else {
				return fmt.Errorf("config: parse idp.key_lifetime %q: %w", *f.IdP.KeyLifetime, err)
			}
		}
		if f.IdP.RotateBefore != nil {
			if d, err := time.ParseDuration(*f.IdP.RotateBefore); err == nil {
				cfg.IdP.RotateBefore = d
			} else {
				return fmt.Errorf("config: parse idp.rotate_before %q: %w", *f.IdP.RotateBefore, err)
			}
		}
		if f.IdP.TokenLifetime != nil {
			if d, err := time.ParseDuration(*f.IdP.TokenLifetime); err == nil {
				cfg.IdP.TokenLifetime = d
			} else {
				return fmt.Errorf("config: parse idp.token_lifetime %q: %w", *f.IdP.TokenLifetime, err)
			}
		}
		if f.IdP.CheckInterval != nil {
			if d, err := time.ParseDuration(*f.IdP.CheckInterval); err == nil {
				cfg.IdP.CheckInterval = d
			} else {
				return fmt.Errorf("config: parse idp.check_interval %q: %w", *f.IdP.CheckInterval, err)
			}
		}
		if f.IdP.Audiences != nil {
			cfg.IdP.Audiences = *f.IdP.Audiences
		}
	}
	if f.GRPCAuth != nil {
		if f.GRPCAuth.Enabled != nil {
			cfg.GRPCAuth.Enabled = *f.GRPCAuth.Enabled
		}
		if f.GRPCAuth.IdPAddress != nil {
			cfg.GRPCAuth.IdPAddress = *f.GRPCAuth.IdPAddress
		}
		if f.GRPCAuth.Audiences != nil {
			cfg.GRPCAuth.Audiences = f.GRPCAuth.Audiences
		}
		if f.GRPCAuth.ExchangedTokenLifetime != nil {
			if d, err := time.ParseDuration(*f.GRPCAuth.ExchangedTokenLifetime); err == nil {
				cfg.GRPCAuth.ExchangedTokenLifetime = d
			} else {
				return fmt.Errorf("config: parse grpc_auth.exchanged_token_lifetime %q: %w", *f.GRPCAuth.ExchangedTokenLifetime, err)
			}
		}
	}
	if f.Secrets != nil {
		if f.Secrets.Kind != nil {
			cfg.Secrets.Kind = *f.Secrets.Kind
		}
		if f.Secrets.Key != nil {
			cfg.Secrets.Key = *f.Secrets.Key
		}
	}
	return nil
}

// applyEnv overlays the environment variables onto cfg.
func applyEnv(cfg *Config) {
	cfg.DBAddress = envOr("CDROM_DB_ADDR", cfg.DBAddress)
	cfg.SchedulerAddress = envOr("CDROM_SCHEDULER_ADDR", cfg.SchedulerAddress)
	cfg.ArtifactsAddress = envOr("CDROM_ARTIFACTS_ADDR", cfg.ArtifactsAddress)
	cfg.APIAddress = envOr("CDROM_API_ADDR", cfg.APIAddress)
	cfg.APIHTTPAddress = envOr("CDROM_API_HTTP_ADDR", cfg.APIHTTPAddress)
	cfg.ListenAddress = envOr("CDROM_LISTEN_ADDR", cfg.ListenAddress)
	cfg.ArtifactsRoot = envOr("CDROM_ARTIFACTS_ROOT", cfg.ArtifactsRoot)
	cfg.ArtifactsStore = envOr("CDROM_ARTIFACTS_STORE", cfg.ArtifactsStore)
	cfg.WorkerName = envOr("CDROM_WORKER_NAME", cfg.WorkerName)
	cfg.WorkerGroup = envOr("CDROM_WORKER_GROUP", cfg.WorkerGroup)
	cfg.AgentJobID = envOr("CDROM_AGENT_JOB_ID", cfg.AgentJobID)
	cfg.AgentName = envOr("CDROM_AGENT_NAME", cfg.AgentName)
	cfg.DB.Backend = database.Backend(envOr("CDROM_DB_BACKEND", string(cfg.DB.Backend)))
	cfg.DB.SQLitePath = envOr("CDROM_DB_SQLITE_PATH", cfg.DB.SQLitePath)
	cfg.DB.PostgresDSN = envOr("CDROM_DB_POSTGRES_DSN", cfg.DB.PostgresDSN)
	cfg.TLS.CAFile = envOr("CDROM_TLS_CA_FILE", cfg.TLS.CAFile)
	cfg.TLS.CertFile = envOr("CDROM_TLS_CERT_FILE", cfg.TLS.CertFile)
	cfg.TLS.KeyFile = envOr("CDROM_TLS_KEY_FILE", cfg.TLS.KeyFile)
	if v := os.Getenv("CDROM_AUTH_ENABLED"); v != "" {
		cfg.Auth.Enabled = v == "true" || v == "1"
	}
	cfg.Auth.Issuer = envOr("CDROM_AUTH_ISSUER", cfg.Auth.Issuer)
	cfg.Auth.ClientID = envOr("CDROM_AUTH_CLIENT_ID", cfg.Auth.ClientID)
	cfg.Auth.RedirectURL = envOr("CDROM_AUTH_REDIRECT_URL", cfg.Auth.RedirectURL)
	cfg.Auth.TokenAudience = envOr("CDROM_AUTH_TOKEN_AUDIENCE", cfg.Auth.TokenAudience)
	cfg.IdP.Issuer = envOr("CDROM_IDP_ISSUER", cfg.IdP.Issuer)
	if v := envOr("CDROM_IDP_KEY_LIFETIME", ""); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.IdP.KeyLifetime = d
		}
	}
	if v := envOr("CDROM_IDP_ROTATE_BEFORE", ""); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.IdP.RotateBefore = d
		}
	}
	if v := envOr("CDROM_IDP_TOKEN_LIFETIME", ""); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.IdP.TokenLifetime = d
		}
	}
	if v := envOr("CDROM_IDP_CHECK_INTERVAL", ""); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.IdP.CheckInterval = d
		}
	}
	if v := os.Getenv("CDROM_GRPC_AUTH_ENABLED"); v != "" {
		cfg.GRPCAuth.Enabled = v == "true" || v == "1"
	}
	cfg.GRPCAuth.IdPAddress = envOr("CDROM_GRPC_AUTH_IDP_ADDR", cfg.GRPCAuth.IdPAddress)
	if v := os.Getenv("CDROM_GRPC_AUTH_AUDIENCES"); v != "" {
		cfg.GRPCAuth.Audiences = splitAndTrim(v)
	}
	if v := envOr("CDROM_GRPC_AUTH_EXCHANGED_TOKEN_LIFETIME", ""); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.GRPCAuth.ExchangedTokenLifetime = d
		}
	}
	cfg.Secrets.Kind = envOr("CDROM_SECRETS_KIND", cfg.Secrets.Kind)
	cfg.Secrets.Key = envOr("CDROM_SECRETS_KEY", cfg.Secrets.Key)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// defaultAgentName is the default identity of an ephemeral agent: the host
// name it runs on. When the host name cannot be determined it falls back to
// "agent". It is the same mechanism a worker's name is set by (a config value
// that can be overridden by a file or environment variable), so an agent and
// a worker are identified the same way.
func defaultAgentName() string {
	if host, err := os.Hostname(); err == nil && host != "" {
		return host
	}
	return "agent"
}

// splitAndTrim splits s on commas and trims surrounding whitespace from each
// part, dropping empty parts.
func splitAndTrim(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
