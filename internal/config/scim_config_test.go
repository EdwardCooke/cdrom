package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestScimDefaults(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	scim := cfg.IdP.Scim
	if scim.Enabled {
		t.Errorf("Scim.Enabled = true, want false (default)")
	}
	if scim.PublicBase != "" {
		t.Errorf("Scim.PublicBase = %q, want empty (announce per request)", scim.PublicBase)
	}
	if scim.MaxPageSize != DefaultSCIMMaxPageSize {
		t.Errorf("Scim.MaxPageSize = %d, want %d (default)", scim.MaxPageSize, DefaultSCIMMaxPageSize)
	}
	if scim.EffectiveMaxPageSize() != DefaultSCIMMaxPageSize {
		t.Errorf("EffectiveMaxPageSize() = %d, want %d", scim.EffectiveMaxPageSize(), DefaultSCIMMaxPageSize)
	}
}

func TestScimConfigFromFile(t *testing.T) {
	yaml := `
idp:
  scim:
    enabled: true
    public_base: https://idp.corp.example/ext-idp/
    group_role_mappings:
      admins: admin
      deployers: operator
    max_page_size: 500
`
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadWithFile(path)
	if err != nil {
		t.Fatalf("LoadWithFile: %v", err)
	}
	scim := cfg.IdP.Scim
	if !scim.Enabled {
		t.Errorf("Scim.Enabled = false, want true")
	}
	if scim.PublicBase != "https://idp.corp.example/ext-idp/" {
		t.Errorf("Scim.PublicBase = %q, want the configured value", scim.PublicBase)
	}
	if norm := scim.NormalizePublicBase(); norm != "https://idp.corp.example/ext-idp" {
		t.Errorf("NormalizePublicBase() = %q, want trailing slash stripped", norm)
	}
	if got := scim.GroupRoleMappings["admins"]; got != "admin" {
		t.Errorf("GroupRoleMappings[admins] = %q, want admin", got)
	}
	if got := scim.GroupRoleMappings["deployers"]; got != "operator" {
		t.Errorf("GroupRoleMappings[deployers] = %q, want operator", got)
	}
	if scim.MaxPageSize != 500 {
		t.Errorf("Scim.MaxPageSize = %d, want 500", scim.MaxPageSize)
	}
	if scim.EffectiveMaxPageSize() != 500 {
		t.Errorf("EffectiveMaxPageSize() = %d, want 500", scim.EffectiveMaxPageSize())
	}
	// Other idp keys are unaffected.
	if cfg.IdP.Issuer != defaultIdPIssuer {
		t.Errorf("IdP.Issuer = %q, want default", cfg.IdP.Issuer)
	}
}

func TestScimPartialMergeKeepsDefaults(t *testing.T) {
	yaml := "idp:\n  scim:\n    enabled: true\n"
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadWithFile(path)
	if err != nil {
		t.Fatalf("LoadWithFile: %v", err)
	}
	scim := cfg.IdP.Scim
	if !scim.Enabled {
		t.Errorf("Scim.Enabled = false, want true")
	}
	if scim.PublicBase != "" {
		t.Errorf("Scim.PublicBase = %q, want empty (partial merge keeps default)", scim.PublicBase)
	}
	if scim.MaxPageSize != DefaultSCIMMaxPageSize {
		t.Errorf("Scim.MaxPageSize = %d, want default %d", scim.MaxPageSize, DefaultSCIMMaxPageSize)
	}
}

func TestScimEnvOverrides(t *testing.T) {
	t.Setenv("CDROM_SCIM_ENABLED", "true")
	t.Setenv("CDROM_SCIM_PUBLIC_BASE", "https://portal.example.com/ext-idp")
	t.Setenv("CDROM_SCIM_MAX_PAGE_SIZE", "250")
	t.Setenv("CDROM_SCIM_GROUP_ROLE_MAPPINGS", "eng=developer, sre=admin")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	scim := cfg.IdP.Scim
	if !scim.Enabled {
		t.Errorf("Scim.Enabled = false, want true (env)")
	}
	if scim.PublicBase != "https://portal.example.com/ext-idp" {
		t.Errorf("Scim.PublicBase = %q, want the env value", scim.PublicBase)
	}
	if scim.MaxPageSize != 250 {
		t.Errorf("Scim.MaxPageSize = %d, want 250 (env)", scim.MaxPageSize)
	}
	if got := scim.GroupRoleMappings["eng"]; got != "developer" {
		t.Errorf("GroupRoleMappings[eng] = %q, want developer", got)
	}
	if got := scim.GroupRoleMappings["sre"]; got != "admin" {
		t.Errorf("GroupRoleMappings[sre] = %q, want admin", got)
	}
}

func TestScimPublicBaseValidation(t *testing.T) {
	valid := []string{
		"", // empty = announce per request (legal)
		"https://idp.corp.example",
		"https://idp.corp.example/ext-idp",
		"https://idp.corp.example/ext-idp/", // trailing slash tolerated
	}
	for _, in := range valid {
		if err := (&ScimConfig{PublicBase: in}).ValidatePublicBase(); err != nil {
			t.Errorf("ValidatePublicBase(%q) = %v, want nil", in, err)
		}
	}
	invalid := []string{
		"//relative",                       // no scheme/host
		"ftp://idp.corp.example",           // wrong scheme
		"https:",                           // degenerate
		"idp.corp.example",                 // missing scheme
		"https://idp.corp.example/?x=1",    // query string poisons concatenation
		"https://idp.corp.example/#anchor", // fragment poisons concatenation
	}
	for _, in := range invalid {
		if err := (&ScimConfig{PublicBase: in}).ValidatePublicBase(); err == nil {
			t.Errorf("ValidatePublicBase(%q) = nil, want error", in)
		}
	}
	if got := (&ScimConfig{PublicBase: "https://idp.corp.example/ext-idp/"}).NormalizePublicBase(); got != "https://idp.corp.example/ext-idp" {
		t.Errorf("NormalizePublicBase = %q, want trailing slash stripped", got)
	}
}

func TestScimMaxPageSizeClamp(t *testing.T) {
	cases := []struct {
		configured int
		want       int
	}{
		{0, DefaultSCIMMaxPageSize},  // unset → default
		{-5, DefaultSCIMMaxPageSize}, // nonsense → default
		{1, 1},                       // honored
		{100, 100},                   // honored
		{5000, 5000},                 // honored (above the RFC-free default)
		{SCIMHardCapMaxPageSize, SCIMHardCapMaxPageSize},     // at the cap
		{SCIMHardCapMaxPageSize + 1, SCIMHardCapMaxPageSize}, // clamped
		{1 << 30, SCIMHardCapMaxPageSize},                    // absurd → clamped
	}
	for _, tc := range cases {
		got := (&ScimConfig{MaxPageSize: tc.configured}).EffectiveMaxPageSize()
		if got != tc.want {
			t.Errorf("EffectiveMaxPageSize(%d) = %d, want %d", tc.configured, got, tc.want)
		}
	}
}

func TestScimValidateAllowsEnabledSurface(t *testing.T) {
	// Caller authentication (F-28.2) runs against the database-held SCIM API
	// key pool, so enabling the surface with no keys yet is legal —
	// Validate must not reject it.
	cfg := testValidIdPConfig(t)
	cfg.Scim = ScimConfig{Enabled: true}
	if err := cfg.Validate(); err != nil {
		t.Errorf("Validate(enabled) = %v, want nil", err)
	}
}

// testValidIdPConfig returns a minimally-valid IdPConfig.
func testValidIdPConfig(t *testing.T) IdPConfig {
	t.Helper()
	full, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return full.IdP
}
