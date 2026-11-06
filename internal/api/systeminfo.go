// systeminfo.go — GET /api/system/info (F-28.4 Slice 4).
//
// An UNAUTHENTICATED, strictly NON-SECRET whitelist describing which mechanics
// are switched on, so the UI can make show/hide decisions on load (hide the
// password form when local login is off, badge SCIM-managed users, advertise
// the artifact store kind). It aggregates three sources:
//
//   - the API's OWN config (auth.enablement flags) — always known;
//   - the IdP's DescribeSystem RPC (scim / local-login / break-glass);
//   - the Artifacts service's Describe RPC (store kind).
//
// A component that is unwired or unreachable yields a NULL SEGMENT (the
// endpoint still responds 200) rather than failing the whole request. Nothing
// secret travels here: no addresses, keys, peppers, or DSNs — only capability
// booleans and a store-kind string.
package api

import (
	"context"
	"net/http"

	"google.golang.org/grpc"

	artifactspb "cdrom/internal/gen/cdrom/artifacts/v1"
	idppb "cdrom/internal/gen/cdrom/idp/v1"
)

// idpSystemDescriber abstracts the IdP's DescribeSystem RPC (F-28.4 Slice 4).
// The generated idppb.IdPClient satisfies it; the variadic opts keep the
// signature faithful to the gRPC surface.
type idpSystemDescriber interface {
	DescribeSystem(ctx context.Context, in *idppb.DescribeSystemRequest, opts ...grpc.CallOption) (*idppb.DescribeSystemResponse, error)
}

// SetIdPSystemDescriber attaches the IdP client used to populate the `idp`
// segment of GET /api/system/info. When left nil that segment reports null.
func (s *Server) SetIdPSystemDescriber(d idpSystemDescriber) {
	s.idpSysDesc = d
}

// systemInfo serves GET /api/system/info. It is intentionally placed on the
// auth-exempt path (see the binary's exemption list): the UI reads it before
// it holds a token.
func (s *Server) systemInfo(w http.ResponseWriter, r *http.Request) {
	// The API's own enablement flags (always known; guarded for nil cfg in
	// tests that construct a bare Server).
	authSeg := map[string]any{
		"oidc":             false,
		"userpass":         false,
		"api_keys":         false,
		"service_accounts": false,
		"rbac":             s.rbacEnabled,
	}
	if s.cfg != nil {
		authSeg["oidc"] = s.cfg.Auth.Enabled
		authSeg["userpass"] = s.cfg.Auth.UserPassEnabled
		authSeg["api_keys"] = s.cfg.Auth.APIKey.Enabled
		authSeg["service_accounts"] = s.cfg.Auth.ServiceAccount.Enabled
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"auth":      authSeg,
		"idp":       s.describeIdpSegment(r.Context()),
		"artifacts": s.describeArtifactsSegment(r.Context()),
	})
}

// describeIdpSegment returns the IdP's enablement flags, or nil (=> JSON null)
// when the IdP is unwired or unreachable.
func (s *Server) describeIdpSegment(ctx context.Context) map[string]any {
	if s.idpSysDesc == nil {
		return nil
	}
	resp, err := s.idpSysDesc.DescribeSystem(ctx, &idppb.DescribeSystemRequest{})
	if err != nil {
		return nil
	}
	return map[string]any{
		"scim":        resp.GetScimEnabled(),
		"local_login": resp.GetLocalLoginEnabled(),
		"break_glass": resp.GetBreakGlassEnabled(),
	}
}

// describeArtifactsSegment returns the artifact store kind, or nil (=> JSON
// null) when the artifacts client is unwired or unreachable.
func (s *Server) describeArtifactsSegment(ctx context.Context) map[string]any {
	if s.clients.Artifacts == nil {
		return nil
	}
	resp, err := s.clients.Artifacts.Describe(ctx, &artifactspb.DescribeRequest{})
	if err != nil {
		return nil
	}
	return map[string]any{
		"store_kind": resp.GetStoreKind(),
	}
}
