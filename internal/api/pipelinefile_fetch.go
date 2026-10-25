package api

import (
	"context"
	"fmt"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	pipelinesourcepb "cdrom/internal/gen/cdrom/pipelinesource/v1"
	"cdrom/internal/pipelinefile"
	"cdrom/internal/sourceref"
)

// pipelineFileFetcher adapts the API's source-fetcher client (F-21.2) into the
// compiler's Fetcher (F-21.4): it turns a full reference ("<type>:<spec>") into
// a FetchSource call, selecting and decrypting the applicable source credential
// (F-21.3.2, by prefix match — an imported file shares the pipeline's source,
// so the same credential applies) and passing the runtime-managed SSH known
// hosts (F-21.2.4.1) for git fetches. The plaintext credential is used for a
// single fetch and is never persisted or logged. Returns nil when the fetcher
// is not configured (imports cannot be resolved).
func (s *Server) pipelineFileFetcher() pipelinefile.Fetcher {
	if s.clients.PipelineSource == nil {
		return nil
	}
	return func(ctx context.Context, reference string) ([]byte, error) {
		ref, err := sourceref.ParseReference(reference)
		if err != nil {
			return nil, err
		}
		// Select the credential by prefix (an imported file shares the
		// pipeline's source, so the same credential applies) and decrypt it
		// for this single fetch (F-21.3.2). Public sources match no credential.
		matchSubject := ref.Repo
		if ref.Kind == sourceref.KindHTTP {
			matchSubject = ref.URL
		}
		var cred *dbpb.SourceCredential
		if s.clients.Database != nil {
			cred, err = s.selectSourceCredential(ctx, "", matchSubject)
			if err != nil {
				return nil, fmt.Errorf("select credential for import: %w", err)
			}
		}
		fetchCred, derr := s.decryptSourceCredential(ctx, cred)
		if derr != nil {
			return nil, fmt.Errorf("decrypt credential for import: %w", derr)
		}
		req := &pipelinesourcepb.FetchSourceRequest{
			Reference:  reference,
			Credential: fetchCred,
		}
		// Pass the runtime-managed SSH host keys (F-21.2.4.1) for a git fetch;
		// the fetcher combines them with its built-in common provider keys.
		if ref.Kind == sourceref.KindGit {
			req.KnownHosts = s.runtimeKnownHosts(ctx)
		}
		resp, ferr := s.clients.PipelineSource.FetchSource(ctx, req)
		if ferr != nil {
			return nil, ferr
		}
		return resp.GetContent(), nil
	}
}

// runtimeKnownHosts returns the runtime-managed SSH host keys (F-21.2.4.1) as
// OpenSSH known_hosts lines ("<host-pattern> <key-type> <key>"). It is
// best-effort: a database that is unavailable yields no keys (the fetcher then
// falls back to its built-in common provider keys).
func (s *Server) runtimeKnownHosts(ctx context.Context) []string {
	if s.clients.Database == nil {
		return nil
	}
	resp, err := s.clients.Database.ListSSHKnownHosts(ctx, &dbpb.ListSSHKnownHostsRequest{})
	if err != nil {
		return nil
	}
	var out []string
	for _, h := range resp.GetHosts() {
		out = append(out, h.GetHostPattern()+" "+h.GetKeyType()+" "+h.GetKey())
	}
	return out
}
