package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// TestSSHKnownHostsEndpoints verifies the runtime-managed SSH host key
// endpoints (F-21.2.4.1): GET /api/ssh-known-hosts lists the stored keys,
// POST adds one, and DELETE removes one.
func TestSSHKnownHostsEndpoints(t *testing.T) {
	fake := &fakeDatabase{
		sshKnownHosts: &dbpb.ListSSHKnownHostsResponse{
			Hosts: []*dbpb.SSHKnownHost{
				{Id: 1, HostPattern: "git.example.com", KeyType: "ssh-ed25519", Key: "AAAAC3NzaC1lZDI1NTE5"},
			},
		},
	}
	srv := New(Clients{Database: fake}, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// GET lists the stored keys.
	resp, err := http.Get(ts.URL + "/api/ssh-known-hosts")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET status = %d, want 200 (body %s)", resp.StatusCode, data)
	}
	var hosts []*dbpb.SSHKnownHost
	if err := json.NewDecoder(resp.Body).Decode(&hosts); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(hosts) != 1 || hosts[0].GetHostPattern() != "git.example.com" {
		t.Fatalf("hosts = %+v, want one git.example.com", hosts)
	}

	// POST adds a key.
	body, _ := json.Marshal(sshKnownHostRequest{
		HostPattern: "git.corp.example.com",
		KeyType:     "ssh-ed25519",
		Key:         "AAAAC3NzaC1lZDI1NTE5",
		Description: "corporate git",
	})
	resp, err = http.Post(ts.URL+"/api/ssh-known-hosts", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST status = %d, want 201 (body %s)", resp.StatusCode, data)
	}
	if fake.createdSSHKnownHost == nil || fake.createdSSHKnownHost.GetHostPattern() != "git.corp.example.com" {
		t.Fatalf("create not recorded: %+v", fake.createdSSHKnownHost)
	}

	// DELETE removes a key by id.
	req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/api/ssh-known-hosts/1", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("DELETE status = %d, want 200 (body %s)", resp.StatusCode, data)
	}
	if fake.deletedSSHKnownHost != 1 {
		t.Fatalf("delete id = %d, want 1", fake.deletedSSHKnownHost)
	}
}
