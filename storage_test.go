package etcdstorage

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/certmagic"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestStorage runs against a real etcd, under a prefix of its own that it
// removes afterwards. For example, with a throwaway container:
//
//	podman run --rm -d -p 127.0.0.1:52379:2379 quay.io/coreos/etcd:v3.6.14 etcd \
//		--listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://127.0.0.1:2379
//	ETCDSTORAGE_TEST_ENDPOINTS=127.0.0.1:52379 go test ./...
func TestStorage(t *testing.T) {
	endpoints := strings.Fields(os.Getenv("ETCDSTORAGE_TEST_ENDPOINTS"))
	if len(endpoints) == 0 {
		t.Skip("set ETCDSTORAGE_TEST_ENDPOINTS to an etcd the test may write to")
	}
	lockTTL = 2 * time.Second
	prefix := fmt.Sprintf("/caddy-storage-etcd-test/%d", time.Now().UnixNano())
	ctx := context.Background()

	// Two instances with separate clients stand in for two Caddy servers.
	a, b := provision(t, endpoints, prefix), provision(t, endpoints, prefix)
	t.Cleanup(func() { a.client.Delete(ctx, prefix+"/", clientv3.WithPrefix()) })

	t.Run("keys", func(t *testing.T) {
		before := time.Now()
		store(t, a, "certs/site.crt", "first")
		store(t, a, "certs/site.crt", "second")
		if got, err := b.Load(ctx, "certs/site.crt"); err != nil || string(got) != "second" {
			t.Fatalf("Load = %q, %v; want the overwritten value", got, err)
		}
		info, err := b.Stat(ctx, "certs/site.crt")
		if err != nil || !info.IsTerminal || info.Size != int64(len("second")) || info.Modified.Before(before) {
			t.Fatalf("Stat = %+v, %v", info, err)
		}

		if err := a.Delete(ctx, "certs/site.crt"); err != nil {
			t.Fatal(err)
		}
		if b.Exists(ctx, "certs/site.crt") {
			t.Fatal("deleted key still exists")
		}
		if _, err := b.Load(ctx, "certs/site.crt"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("Load of a missing key: %v; want fs.ErrNotExist", err)
		}
		if _, err := b.Stat(ctx, "certs/site.crt"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("Stat of a missing key: %v; want fs.ErrNotExist", err)
		}
	})

	t.Run("directories", func(t *testing.T) {
		for _, key := range []string{
			"acme/ca/users/me.json",
			"acme/ca/users/me.key",
			"acme/ca/site.crt",
			"acme/ca2",
			"acme/c_d",
			"acme_b/x", // shares a string prefix, not the directory
		} {
			store(t, a, key, "v")
		}

		if !b.Exists(ctx, "acme/ca") || b.Exists(ctx, "acme/c") {
			t.Fatal("Exists must match whole path segments")
		}
		info, err := b.Stat(ctx, "acme/ca")
		if err != nil || info != (certmagic.KeyInfo{Key: "acme/ca"}) {
			t.Fatalf("Stat of a directory = %+v, %v", info, err)
		}

		list := func(dir string, recursive bool, want ...string) {
			t.Helper()
			if got, err := b.List(ctx, dir, recursive); err != nil || !slices.Equal(got, want) {
				t.Fatalf("List(%q, %v) = %q, %v; want %q", dir, recursive, got, err, want)
			}
		}
		list("acme", false, "acme/c_d", "acme/ca", "acme/ca2")
		list("acme/", false, "acme/c_d", "acme/ca", "acme/ca2")
		list("acme", true, "acme/c_d", "acme/ca/site.crt", "acme/ca/users/me.json", "acme/ca/users/me.key", "acme/ca2")
		if _, err := b.List(ctx, "nothing", false); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("List of a missing directory: %v; want fs.ErrNotExist", err)
		}

		if err := a.Delete(ctx, "acme/ca"); err != nil {
			t.Fatal(err)
		}
		list("acme", true, "acme/c_d", "acme/ca2")
		list("", false, "acme", "acme_b")
		if err := a.Delete(ctx, ""); err == nil {
			t.Fatal("Delete of the root must be refused")
		}
	})

	t.Run("lock waits for its holder", func(t *testing.T) {
		lock(t, a, ctx, "renew")
		acquired := make(chan error)
		go func() { acquired <- b.Lock(ctx, "renew") }()

		select {
		case err := <-acquired:
			t.Fatalf("second Lock returned while the lock was held: %v", err)
		case <-time.After(300 * time.Millisecond):
		}
		unlock(t, a, "renew")
		if err := <-acquired; err != nil {
			t.Fatal(err)
		}
		unlock(t, b, "renew")
	})

	t.Run("lock excludes within one instance", func(t *testing.T) {
		lock(t, a, ctx, "renew")
		short, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer cancel()
		if err := a.Lock(short, "renew"); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("second Lock on the same instance: %v; want context.DeadlineExceeded", err)
		}
		unlock(t, a, "renew")
	})

	t.Run("lock outlives its TTL while held", func(t *testing.T) {
		lock(t, a, ctx, "renew")
		time.Sleep(3 * lockTTL)
		short, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer cancel()
		if err := b.Lock(short, "renew"); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Lock of a held lock: %v; want context.DeadlineExceeded", err)
		}
		unlock(t, a, "renew")
	})

	t.Run("lock dies with its holder", func(t *testing.T) {
		holder := provision(t, endpoints, prefix)
		lock(t, holder, ctx, "renew")
		holder.client.Close() // crash: no unlock, no more keepalives
		start := time.Now()
		bounded, cancel := context.WithTimeout(ctx, 5*lockTTL)
		defer cancel()
		lock(t, b, bounded, "renew")
		if waited := time.Since(start); waited < lockTTL/2 {
			t.Fatalf("got the lock after %v, before its lease could expire", waited)
		}
		unlock(t, b, "renew")
	})
}

func provision(t *testing.T, endpoints []string, prefix string) *Storage {
	t.Helper()
	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	t.Cleanup(cancel)
	s := &Storage{Endpoints: endpoints, Prefix: prefix}
	if err := s.Provision(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Cleanup() })
	return s
}

func store(t *testing.T, s *Storage, key, value string) {
	t.Helper()
	if err := s.Store(context.Background(), key, []byte(value)); err != nil {
		t.Fatal(err)
	}
}

func lock(t *testing.T, s *Storage, ctx context.Context, name string) {
	t.Helper()
	if err := s.Lock(ctx, name); err != nil {
		t.Fatal(err)
	}
}

func unlock(t *testing.T, s *Storage, name string) {
	t.Helper()
	if err := s.Unlock(context.Background(), name); err != nil {
		t.Fatal(err)
	}
}
