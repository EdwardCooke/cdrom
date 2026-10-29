// packages_test.go — the package registry HTTP surface (F-21.6,
// storage-redesign era): publish (two-phase record + blob, immutability,
// explicit prerelease flag), browse (FROM THE RECORDS), and promote
// (SERVER-SIDE COPY — byte-identity by construction; the former
// checksum-comparison machinery is obsolete and no longer exercisable).
//
// Rig: a REAL Database service (SQLite, via startDBServer in grpc_test.go)
// supplying the record ledger, and a FAKE blob store (in-memory) standing in
// for the artifacts service.
package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	artifactspb "cdrom/internal/gen/cdrom/artifacts/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// ---------------------------------------------------------------------------
// Fake blob store (stands in for the artifacts service)
// ---------------------------------------------------------------------------

// fakeBlobStore is an in-memory artifacts client. It embeds the interface
// (nil) and overrides the methods the surface uses.
type fakeBlobStore struct {
	artifactspb.ArtifactsClient // nil; only the methods below are used
	mu                          sync.Mutex
	blobs                       map[string][]byte // uid -> data
	deleted                     []string          // tombstones (cleanup assertions)
}

func newFakeBlobStore() *fakeBlobStore {
	return &fakeBlobStore{blobs: map[string][]byte{}}
}

func (f *fakeBlobStore) put(uid string, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blobs[uid] = data
}

// --- PutBlob (client streaming) --------------------------------------------

type fakePutStream struct {
	grpc.ClientStreamingClient[artifactspb.BlobChunk, artifactspb.BlobStat] // nil; Send/CloseAndRecv overridden
	uid                                                                     string
	data                                                                    []byte
	fs                                                                      *fakeBlobStore
}

func (f *fakeBlobStore) PutBlob(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[artifactspb.BlobChunk, artifactspb.BlobStat], error) {
	return &fakePutStream{fs: f}, nil
}

func (s *fakePutStream) Send(ch *artifactspb.BlobChunk) error {
	if s.uid == "" {
		if ch.GetId() == "" {
			return status.Error(codes.InvalidArgument, "first chunk must carry the blob id")
		}
		s.uid = ch.GetId()
	}
	s.data = append(s.data, ch.GetData()...)
	return nil
}

func (s *fakePutStream) CloseAndRecv() (*artifactspb.BlobStat, error) {
	s.fs.put(s.uid, s.data)
	return &artifactspb.BlobStat{Size: int64(len(s.data))}, nil
}

func (s *fakePutStream) CloseSend() error                { return nil }
func (s *fakePutStream) Context() context.Context        { return context.Background() }
func (s *fakePutStream) SetHeader(md metadata.MD) error  { return nil }
func (s *fakePutStream) SendHeader(md metadata.MD) error { return nil }
func (s *fakePutStream) Header() (metadata.MD, error)    { return nil, nil }

// --- GetBlob (server streaming) ---------------------------------------------

type fakeGetStream struct {
	grpc.ServerStreamingClient[artifactspb.BlobChunk] // nil; Recv overridden
	data                                              []byte
	pos                                               int
	done                                              bool
}

func (f *fakeBlobStore) GetBlob(ctx context.Context, req *artifactspb.BlobRef, opts ...grpc.CallOption) (grpc.ServerStreamingClient[artifactspb.BlobChunk], error) {
	f.mu.Lock()
	data, ok := f.blobs[req.GetId()]
	f.mu.Unlock()
	if !ok {
		return nil, status.Error(codes.NotFound, "blob not found")
	}
	return &fakeGetStream{data: append([]byte(nil), data...)}, nil
}

func (s *fakeGetStream) Recv() (*artifactspb.BlobChunk, error) {
	if !s.done {
		s.done = true
		return &artifactspb.BlobChunk{Size: int64(len(s.data))}, nil
	}
	if s.pos >= len(s.data) {
		return nil, io.EOF
	}
	n := 64
	if len(s.data)-s.pos < n {
		n = len(s.data) - s.pos
	}
	out := s.data[s.pos : s.pos+n]
	s.pos += n
	return &artifactspb.BlobChunk{Data: out}, nil
}

// --- StatBlob / CopyBlob / DeleteBlob ----------------------------------------

func (f *fakeBlobStore) StatBlob(ctx context.Context, req *artifactspb.BlobRef, opts ...grpc.CallOption) (*artifactspb.BlobStat, error) {
	f.mu.Lock()
	data, ok := f.blobs[req.GetId()]
	f.mu.Unlock()
	if !ok {
		return nil, status.Error(codes.NotFound, "blob not found")
	}
	return &artifactspb.BlobStat{Size: int64(len(data))}, nil
}

func (f *fakeBlobStore) CopyBlob(ctx context.Context, req *artifactspb.CopyBlobRequest, opts ...grpc.CallOption) (*artifactspb.BlobStat, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	src, ok := f.blobs[req.GetSrc()]
	if !ok {
		return nil, status.Error(codes.NotFound, "source blob not found")
	}
	if cur, busy := f.blobs[req.GetDst()]; busy && len(cur) > 0 {
		return nil, status.Error(codes.AlreadyExists, "destination blob already holds data")
	}
	f.blobs[req.GetDst()] = append([]byte(nil), src...)
	return &artifactspb.BlobStat{Size: int64(len(src))}, nil
}

func (f *fakeBlobStore) DeleteBlob(ctx context.Context, req *artifactspb.BlobRef, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.blobs, req.GetId())
	f.deleted = append(f.deleted, req.GetId())
	return &emptypb.Empty{}, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// publishBody crafts the header-line + payload body of a publish request.
func publishBody(name, version, ext, payload string, prerelease bool) []byte {
	header, _ := json.Marshal(map[string]any{"name": name, "version": version, "ext": ext, "prerelease": prerelease})
	return append(append(header, '\n'), payload...)
}

func newPackagesHarness(t *testing.T) (*httptest.Server, *fakeBlobStore, dbpb.DatabaseClient) {
	t.Helper()
	store := newFakeBlobStore()
	db := startDBServer(t)
	srv := New(Clients{Database: db, Artifacts: store}, nil, testSecretStore(t))
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, store, db
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestPublishPackageLifecycle(t *testing.T) {
	ts, store, _ := newPackagesHarness(t)

	payload := "PACKAGE-PAYLOAD-123"
	sum := sha256.Sum256([]byte(payload))
	wantSHA := hex.EncodeToString(sum[:])

	do := func(method, path string, body []byte) (int, []byte) {
		var rdr io.Reader
		if body != nil {
			rdr = bytes.NewReader(body)
		}
		req, _ := http.NewRequest(method, ts.URL+path, rdr)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		defer resp.Body.Close()
		data, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, data
	}

	// Publish (regular channel).
	code, pubData := do("POST", "/api/packages", publishBody("demo", "1.0.0", ".zip", payload, false))
	if code != http.StatusCreated {
		t.Fatalf("publish = %d, want 201 (body %s)", code, pubData)
	}
	var out map[string]any
	json.Unmarshal(pubData, &out)
	if out["filename"] != "demo-1.0.0.zip" || out["sha256"] != wantSHA || out["prerelease"] != false {
		t.Fatalf("publish out = %v, want demo-1.0.0.zip + %s + prerelease=false", out, wantSHA)
	}
	uid, _ := out["id"].(string)
	if uid == "" {
		t.Fatalf("publish out lacks the record uid: %v", out)
	}
	if got := string(store.blobs[uid]); got != payload {
		t.Fatalf("stored blob = %q, want %q", got, payload)
	}

	// Publish a PRE-RELEASE (explicit flag — the channel is asserted, never
	// guessed from the version string).
	code, pubData = do("POST", "/api/packages", publishBody("demo", "2.0.0-rc.1", ".zip", "PRE-BUILD", true))
	if code != http.StatusCreated {
		t.Fatalf("prerelease publish = %d, want 201 (body %s)", code, pubData)
	}
	var pre map[string]any
	json.Unmarshal(pubData, &pre)
	if pre["prerelease"] != true {
		t.Fatalf("prerelease publish out = %v, want prerelease=true", pre)
	}

	// Duplicate publish → 409 (immutability).
	code, _ = do("POST", "/api/packages", publishBody("demo", "1.0.0", ".zip", "OTHER", false))
	if code != http.StatusConflict {
		t.Fatalf("dup publish = %d, want 409", code)
	}
	if got := string(store.blobs[uid]); got != payload {
		t.Fatalf("immutable filing was overwritten: %q", got)
	}

	// Bad coordinates → 400.
	code, _ = do("POST", "/api/packages", publishBody("bad name!", "1.0", ".zip", "x", false))
	if code != http.StatusBadRequest {
		t.Fatalf("bad coordinates = %d, want 400", code)
	}

	// Browse: list (grouped by name, newest first) + get + contents.
	code, listData := do("GET", "/api/packages", nil)
	if code != http.StatusOK {
		t.Fatalf("list = %d, want 200", code)
	}
	var grouped map[string][]map[string]any
	if err := json.Unmarshal(listData, &grouped); err != nil {
		t.Fatalf("list decode: %v", err)
	}
	demoVers := grouped["demo"]
	if len(demoVers) != 2 {
		t.Fatalf("list out = %v, want demo→[2 versions]", grouped)
	}
	if demoVers[0]["version"] != "2.0.0-rc.1" || demoVers[0]["prerelease"] != true {
		t.Fatalf("newest-first ordering = %v, want 2.0.0-rc.1 (prerelease) first", demoVers)
	}
	code, getData := do("GET", "/api/packages/demo", nil)
	if code != http.StatusOK {
		t.Fatalf("get = %d, want 200", code)
	}
	var getArr []any
	if err := json.Unmarshal(getData, &getArr); err != nil {
		t.Fatalf("get decode: %v", err)
	}
	if len(getArr) != 2 {
		t.Fatalf("get out = %v, want [2 descriptors]", getArr)
	}
	// Contents round-trip.
	resp, err := http.Get(ts.URL + "/api/packages/demo/1.0.0/contents")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("contents = %d, want 200", resp.StatusCode)
	}
	data, _ := io.ReadAll(resp.Body)
	if string(data) != payload {
		t.Fatalf("contents = %q, want %q", data, payload)
	}
	// Unknown package → 404.
	resp, err = http.Get(ts.URL + "/api/packages/ghost")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("ghost = %d, want 404", resp.StatusCode)
	}
}

// seedJobArtifact plants a JOB-OWNED artifact (record + blob) so promotion
// has a source to lift.
func seedJobArtifact(t *testing.T, db dbpb.DatabaseClient, store *fakeBlobStore, ns, name, payload string) {
	t.Helper()
	rec, err := db.AllocateStorageObject(context.Background(), &dbpb.AllocateStorageObjectRequest{
		Kind: "artifact", Namespace: ns, Name: name,
	})
	if err != nil {
		t.Fatalf("seed allocate: %v", err)
	}
	sum := sha256.Sum256([]byte(payload))
	uid := rec.GetUid()
	store.put(uid, []byte(payload))
	if _, err := db.ConfirmStorageObject(context.Background(), &dbpb.ConfirmStorageObjectRequest{
		Uid: uid, Size: int64(len(payload)), ContentSha256: hex.EncodeToString(sum[:]),
	}); err != nil {
		t.Fatalf("seed confirm: %v", err)
	}
}

func TestPromoteBetweenNamespaces(t *testing.T) {
	ts, store, db := newPackagesHarness(t)

	source := "SOURCE-BYTES"
	seedJobArtifact(t, db, store, "42", "build.zip", source)

	post := func(body string) (int, map[string]any) {
		resp, err := http.Post(ts.URL+"/api/artifacts/promote", "application/json", bytes.NewReader([]byte(body)))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		data, _ := io.ReadAll(resp.Body)
		var out map[string]any
		json.Unmarshal(data, &out)
		return resp.StatusCode, out
	}

	// Into the registry (coords REQUIRED).
	code, out := post(`{"source_namespace":"42","source_name":"build.zip","dest_namespace":"packages","package_name":"lifted","version":"1.0.0"}`)
	if code != http.StatusOK {
		t.Fatalf("promote = %d: %v", code, out)
	}
	sum := sha256.Sum256([]byte(source))
	if out["sha256"] != hex.EncodeToString(sum[:]) {
		t.Fatalf("promote sha = %v, want the source's measured hash", out["sha256"])
	}
	dstUID, _ := out["id"].(string)
	if dstUID == "" {
		t.Fatalf("promote out lacks the destination uid: %v", out)
	}
	if got := string(store.blobs[dstUID]); got != source {
		t.Fatalf("promoted blob = %q, want %q (bit-identical by construction)", got, source)
	}

	// Missing source → 404.
	code, _ = post(`{"source_namespace":"42","source_name":"ghost.zip","dest_namespace":"packages","package_name":"lifted","version":"9.9.9"}`)
	if code != http.StatusNotFound {
		t.Fatalf("missing source = %d, want 404", code)
	}

	// Occupied destination → 409.
	code, _ = post(`{"source_namespace":"42","source_name":"build.zip","dest_namespace":"packages","package_name":"lifted","version":"1.0.0"}`)
	if code != http.StatusConflict {
		t.Fatalf("occupied dest = %d, want 409", code)
	}

	// Plain (non-registry) promotion: same display name by default.
	code, out = post(`{"source_namespace":"42","source_name":"build.zip","dest_namespace":"77"}`)
	if code != http.StatusOK {
		t.Fatalf("plain promote = %d: %v", code, out)
	}
	if out["dest_name"] != "build.zip" {
		t.Fatalf("plain promote dest_name = %v, want build.zip", out["dest_name"])
	}
}
