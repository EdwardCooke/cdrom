// Command fetchclient is a tiny gRPC client for the source-fetcher service
// (F-21.2), used by the e2e test scripts (scripts/e2e/13-source-fetcher.sh).
//
// It calls FetchSource with a reference (and an optional credential) and
// prints the file's content to stdout and the version identifier + content
// hash to stderr (as "version=..." and "hash=..." lines), so a shell test can
// capture the content and assert on the metadata.
//
// Usage:
//
//	fetchclient -addr 127.0.0.1:7107 -ref "file:/vars.yaml"
//	fetchclient -addr 127.0.0.1:7107 -ref "http:<url>" -cred-kind http -cred-token tok
//	fetchclient -addr 127.0.0.1:7107 -ref "http:<url>" -cred-kind http -cred-header "X-Api-Key=secret"
//	fetchclient -addr 127.0.0.1:7107 -ref "http:<url>" -cred-kind http -cred-header "Authorization=Bearer tok" -cred-header "X-Request-Id=abc"
//	fetchclient -addr 127.0.0.1:7107 -ref "git:<repo>@main[/f.yaml]"
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pipelinesourcepb "cdrom/internal/gen/cdrom/pipelinesource/v1"
)

// headerFlag is a repeatable -cred-header flag ("Name=Value").
type headerFlag []string

func (h *headerFlag) String() string { return strings.Join(*h, ",") }
func (h *headerFlag) Set(v string) error {
	*h = append(*h, v)
	return nil
}

func main() {
	var (
		addr         = flag.String("addr", "127.0.0.1:7107", "the source-fetcher gRPC address")
		ref          = flag.String("ref", "", "the <type>:<spec> reference to fetch")
		credKind     = flag.String("cred-kind", "", "credential kind (git-ssh | git-https | http)")
		credUsername = flag.String("cred-username", "", "credential username (git-https)")
		credToken    = flag.String("cred-token", "", "credential token (git-https)")
		credHeaders  headerFlag
		credKeyFile  = flag.String("cred-key-file", "", "path to a PEM private key file (git-ssh)")
		credHostFile = flag.String("cred-host-file", "", "path to a host key file (git-ssh)")
	)
	flag.Var(&credHeaders, "cred-header", "credential http header as Name=Value (repeatable; carries the secret)")
	flag.Parse()
	if *ref == "" {
		fmt.Fprintln(os.Stderr, "fetchclient: -ref is required")
		os.Exit(2)
	}

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "fetchclient: dial %q: %v\n", *addr, err)
		os.Exit(1)
	}
	defer conn.Close()
	client := pipelinesourcepb.NewPipelineSourceClient(conn)

	var cred *pipelinesourcepb.SourceCredential
	if *credKind != "" {
		cred = &pipelinesourcepb.SourceCredential{
			Kind:     *credKind,
			Username: *credUsername,
			Token:    *credToken,
		}
		for _, hv := range credHeaders {
			name, value, ok := strings.Cut(hv, "=")
			if !ok {
				fmt.Fprintf(os.Stderr, "fetchclient: -cred-header must be Name=Value (got %q)\n", hv)
				os.Exit(2)
			}
			cred.Headers = append(cred.Headers, &pipelinesourcepb.HTTPHeader{Name: name, Value: value})
		}
		if *credKeyFile != "" {
			if b, err := os.ReadFile(*credKeyFile); err == nil {
				cred.PrivateKey = string(b)
			} else {
				fmt.Fprintf(os.Stderr, "fetchclient: read key file: %v\n", err)
				os.Exit(1)
			}
		}
		if *credHostFile != "" {
			if b, err := os.ReadFile(*credHostFile); err == nil {
				cred.HostKey = string(b)
			} else {
				fmt.Fprintf(os.Stderr, "fetchclient: read host key file: %v\n", err)
				os.Exit(1)
			}
		}
	}

	resp, err := client.FetchSource(context.Background(), &pipelinesourcepb.FetchSourceRequest{
		Reference:  *ref,
		Credential: cred,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "fetchclient: FetchSource: %v\n", err)
		os.Exit(1)
	}
	// Content to stdout; metadata to stderr so a shell test can separate them.
	os.Stdout.Write(resp.GetContent())
	fmt.Fprintf(os.Stderr, "version=%s\n", resp.GetVersion())
	fmt.Fprintf(os.Stderr, "hash=%s\n", resp.GetHash())
}
