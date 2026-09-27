package grpcutil

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"cdrom/internal/config"
	artifactspb "cdrom/internal/gen/cdrom/artifacts/v1"
	"cdrom/internal/services/artifacts"
)

// genCerts generates a CA and a leaf certificate (valid for both server and
// client) in a temp dir, writes them as PEM files, and returns a TLSConfig
// pointing at them.
func genCerts(t *testing.T) config.TLSConfig {
	t.Helper()
	dir := t.TempDir()

	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("gen ca key: %v", err)
	}
	caTmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "cdrom-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, &caTmpl, &caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create ca cert: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse ca cert: %v", err)
	}

	leafKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("gen leaf key: %v", err)
	}
	leafTmpl := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "cdrom"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, &leafTmpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create leaf cert: %v", err)
	}
	leafKeyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		t.Fatalf("marshal leaf key: %v", err)
	}

	writePEM(t, filepath.Join(dir, "ca.crt"), "CERTIFICATE", caDER)
	writePEM(t, filepath.Join(dir, "cert.crt"), "CERTIFICATE", leafDER)
	writePEM(t, filepath.Join(dir, "cert.key"), "PRIVATE KEY", leafKeyDER)

	return config.TLSConfig{
		CAFile:   filepath.Join(dir, "ca.crt"),
		CertFile: filepath.Join(dir, "cert.crt"),
		KeyFile:  filepath.Join(dir, "cert.key"),
	}
}

func writePEM(t *testing.T, path, blockType string, der []byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	defer f.Close()
	if err := pem.Encode(f, &pem.Block{Type: blockType, Bytes: der}); err != nil {
		t.Fatalf("pem encode %s: %v", path, err)
	}
}

func TestCredsDisabledReturnsInsecure(t *testing.T) {
	sc, err := ServerCreds(config.TLSConfig{})
	if err != nil {
		t.Fatalf("ServerCreds: %v", err)
	}
	if got := sc.Info().SecurityProtocol; got != "insecure" {
		t.Errorf("ServerCreds protocol = %q, want insecure", got)
	}
	cc, err := ClientCreds(config.TLSConfig{})
	if err != nil {
		t.Fatalf("ClientCreds: %v", err)
	}
	if got := cc.Info().SecurityProtocol; got != "insecure" {
		t.Errorf("ClientCreds protocol = %q, want insecure", got)
	}
}

func TestCredsMissingFilesError(t *testing.T) {
	cfg := config.TLSConfig{
		CAFile:   filepath.Join(t.TempDir(), "nope.crt"),
		CertFile: filepath.Join(t.TempDir(), "nope.crt"),
		KeyFile:  filepath.Join(t.TempDir(), "nope.key"),
	}
	if _, err := ServerCreds(cfg); err == nil {
		t.Error("ServerCreds: expected error for missing files")
	}
	if _, err := ClientCreds(cfg); err == nil {
		t.Error("ClientCreds: expected error for missing files")
	}
}

// TestMTLSRoundTrip boots a real gRPC server with mTLS server credentials and
// dials it with mTLS client credentials, then makes a call. This proves the
// full mutual-TLS handshake (server verifies the client cert, client verifies
// the server against the CA) works.
func TestMTLSRoundTrip(t *testing.T) {
	tlsCfg := genCerts(t)

	serverCreds, err := ServerCreds(tlsCfg)
	if err != nil {
		t.Fatalf("ServerCreds: %v", err)
	}
	if _, ok := serverCreds.(credentials.TransportCredentials); !ok {
		t.Fatal("ServerCreds: expected TLS transport credentials")
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(grpc.Creds(serverCreds))
	artSrv, err := artifacts.NewServer(t.TempDir())
	if err != nil {
		t.Fatalf("artifacts.NewServer: %v", err)
	}
	artifactspb.RegisterArtifactsServer(srv, artSrv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := Dial(context.Background(), lis.Addr().String(), tlsCfg)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client := artifactspb.NewArtifactsClient(conn)
	resp, err := client.ListArtifacts(context.Background(), &artifactspb.ListArtifactsRequest{JobId: "job-1"})
	if err != nil {
		t.Fatalf("ListArtifacts over mTLS: %v", err)
	}
	if len(resp.GetArtifacts()) != 0 {
		t.Errorf("ListArtifacts artifacts = %d, want 0", len(resp.GetArtifacts()))
	}
}

// TestMTLSRequiresClientCert proves the server actually enforces client
// authentication: a client that does not present a certificate must be
// rejected.
func TestMTLSRequiresClientCert(t *testing.T) {
	tlsCfg := genCerts(t)

	serverCreds, err := ServerCreds(tlsCfg)
	if err != nil {
		t.Fatalf("ServerCreds: %v", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(grpc.Creds(serverCreds))
	artSrv, err := artifacts.NewServer(t.TempDir())
	if err != nil {
		t.Fatalf("artifacts.NewServer: %v", err)
	}
	artifactspb.RegisterArtifactsServer(srv, artSrv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	// Dial with plaintext (no client certificate) — the mTLS server must
	// reject the handshake.
	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client := artifactspb.NewArtifactsClient(conn)
	if _, err := client.ListArtifacts(context.Background(), &artifactspb.ListArtifactsRequest{JobId: "job-1"}); err == nil {
		t.Fatal("ListArtifacts: expected the mTLS server to reject a client without a certificate")
	}
}
