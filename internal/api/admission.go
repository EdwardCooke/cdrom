// Package api — the admission gate's ENFORCEMENT SIDE (F-28.4 Slice 3).
//
// The gate is CONSOLIDATED in the IdP (AdmitPrincipal adjudicates a presented
// principal against the user directory — the JIT-vs-SCIM mode fork, the
// sanity bounds, the distinct verdicts). The API's auth middleware invokes
// it PER REQUEST (no caching — a SCIM-driven deactivation bites the very next
// hop) for every HUMAN user principal (OIDC tokens and user API keys;
// service accounts and job tokens are exempt). This file is the THIN
// ADAPTER: it translates the auth package's AdmissionPolicy callback into
// the IdP's gRPC call, maps the verdicts to distinct 403 texts, fails CLOSED
// (503) when the IdP is unreachable, and meters every judgment.
package api

import (
	"context"

	"google.golang.org/grpc"

	"cdrom/internal/auth"
	idppb "cdrom/internal/gen/cdrom/idp/v1"
	apimetrics "cdrom/internal/metrics"
)

// admissionClient is the IdP client the gate adjudicates through (the
// generated idppb.IdPClient satisfies it; the variadic opts keep the
// signature faithful to the gRPC surface).
type admissionClient interface {
	AdmitPrincipal(ctx context.Context, req *idppb.AdmitPrincipalRequest, opts ...grpc.CallOption) (*idppb.AdmitPrincipalResponse, error)
}

// NewAdmissionPolicy builds the auth.AdmissionPolicy the API attaches to its
// middleware: the IdP's AdmitPrincipal RPC, verdict-translated. metrics may
// be nil (then judgments are unmetered).
func NewAdmissionPolicy(idp admissionClient, metrics *apimetrics.APIMetrics) auth.AdmissionPolicy {
	return func(ctx context.Context, user auth.User) auth.AdmissionDecision {
		// A MISSING subject (the configured subject claim absent from the
		// token) cannot be correlated — DENIED outright (distinct text),
		// never admitted and never a 503 (the gate itself is healthy).
		if user.Username == "" {
			record(metrics, "unknown_rejected")
			return auth.AdmissionDecision{Reason: "your token carries no recognizable subject; access denied"}
		}
		resp, err := idp.AdmitPrincipal(ctx, &idppb.AdmitPrincipalRequest{
			Username: user.Username,
			Name:     user.Name,
			Email:    user.Email,
			Roles:    user.Roles,
		})
		if err != nil {
			record(metrics, "infra_failure")
			// FAIL CLOSED: a down/unreachable gate must deny with 503 (a
			// decision FAILURE), never masquerade as a 403 denial — and
			// never let the request through.
			return auth.AdmissionDecision{InfrastructureFailure: true}
		}
		switch resp.GetVerdict() {
		case idppb.AdmissionVerdict_ADMITTED:
			record(metrics, "admitted")
			return auth.AdmissionDecision{Allowed: true}
		case idppb.AdmissionVerdict_DEACTIVATED:
			record(metrics, "deactivated")
			return auth.AdmissionDecision{Reason: "your account is deactivated; contact an administrator"}
		case idppb.AdmissionVerdict_DEPROVISIONED:
			record(metrics, "deprovisioned")
			return auth.AdmissionDecision{Reason: "your account has been deprovisioned"}
		case idppb.AdmissionVerdict_UNKNOWN_REJECTED:
			record(metrics, "unknown_rejected")
			return auth.AdmissionDecision{Reason: "your account is not recognized; contact an administrator"}
		case idppb.AdmissionVerdict_NO_ROLES_REJECTED:
			record(metrics, "no_roles_rejected")
			return auth.AdmissionDecision{Reason: "no roles are mapped for your account; contact an administrator"}
		default:
			// Defensive: an unspecified/unknown verdict is treated as a
			// decision failure (fail closed), not as a denial.
			record(metrics, "infra_failure")
			return auth.AdmissionDecision{InfrastructureFailure: true}
		}
	}
}

func record(metrics *apimetrics.APIMetrics, verdict string) {
	if metrics != nil && metrics.Admissions != nil {
		metrics.Admissions.WithLabelValues(verdict).Inc()
	}
}
