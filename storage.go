// Package etcdstorage is a Caddy storage module that keeps certificates, keys
// and the other assets Caddy manages in etcd, so that several Caddy instances
// can share them.
//
// Under the configured prefix, values live below <prefix>/data/ and locks
// below <prefix>/locks/. Keys are certmagic's slash-separated paths. A
// "directory" is any key prefix that ends at a slash, and it exists while
// some key lies below it.
//
// Locks are etcd mutexes held on leases. Waiters are served first come, first
// served; a held lock is kept alive by the client; and if its holder dies,
// the lease expires and takes the lock with it.
package etcdstorage

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"sync"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/certmagic"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

func init() {
	caddy.RegisterModule(new(Storage))
}

// Storage is the caddy.storage.etcd module.
type Storage struct {
	// Endpoints are the etcd members to talk to, as host:port or URLs.
	Endpoints []string `json:"endpoints,omitempty"`

	// Prefix is the etcd key prefix this module owns. Default: /caddy.
	Prefix string `json:"prefix,omitempty"`

	client *clientv3.Client
	data   string // prefix of stored values, ends in "/"
	lockNS string // prefix of locks, ends in "/"
	logger *zap.Logger

	mu    sync.Mutex
	locks map[string]*concurrency.Session // held locks, by name
}

const defaultPrefix = "/caddy"

// lockTTL is how long a lock outlives a holder that dies without unlocking.
var lockTTL = 15 * time.Second

// CaddyModule returns the Caddy module information.
func (*Storage) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "caddy.storage.etcd",
		New: func() caddy.Module { return new(Storage) },
	}
}

// Provision connects to etcd, and fails if it can't be reached.
func (s *Storage) Provision(ctx caddy.Context) error {
	s.logger = ctx.Logger()
	s.locks = make(map[string]*concurrency.Session)
	if len(s.Endpoints) == 0 {
		return errors.New("etcd storage: no endpoints")
	}
	prefix := strings.TrimSuffix(cmp.Or(s.Prefix, defaultPrefix), "/")
	s.data, s.lockNS = prefix+"/data/", prefix+"/locks/"

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   s.Endpoints,
		DialTimeout: 5 * time.Second,
		Logger:      s.logger.Named("client"),
	})
	if err != nil {
		return fmt.Errorf("etcd storage: %w", err)
	}
	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := client.Get(checkCtx, s.data, clientv3.WithPrefix(), clientv3.WithCountOnly()); err != nil {
		client.Close()
		return fmt.Errorf("etcd storage: %w", err)
	}
	s.client = client
	return nil
}

// Cleanup releases the locks this instance still holds and disconnects.
// Caddy calls it when the config that loaded the module is unloaded.
func (s *Storage) Cleanup() error {
	s.mu.Lock()
	for name, session := range s.locks {
		session.Close()
		delete(s.locks, name)
	}
	s.mu.Unlock()
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

// CertMagicStorage returns the module itself: it is the storage.
func (s *Storage) CertMagicStorage() (certmagic.Storage, error) {
	return s, nil
}

// String identifies the storage in logs.
func (s *Storage) String() string {
	return fmt.Sprintf("etcd%v%s", s.Endpoints, strings.TrimSuffix(s.data, "/data/"))
}

// Store puts value at key, replacing any value already there.
func (s *Storage) Store(ctx context.Context, key string, value []byte) error {
	_, err := s.client.Put(ctx, s.data+key, string(encode(value, time.Now())))
	return err
}

// Load returns the value at key.
func (s *Storage) Load(ctx context.Context, key string) ([]byte, error) {
	resp, err := s.client.Get(ctx, s.data+key)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, notExist(key)
	}
	value, _, err := decode(resp.Kvs[0].Value)
	return value, err
}

// Delete removes key, or, if key is a directory, everything below it.
func (s *Storage) Delete(ctx context.Context, key string) error {
	if below(key) == "" {
		return errors.New("refusing to delete the root of the storage")
	}
	_, err := s.client.Txn(ctx).Then(
		clientv3.OpDelete(s.data+key),
		clientv3.OpDelete(s.data+below(key), clientv3.WithPrefix()),
	).Commit()
	return err
}

// Exists reports whether key is a stored key or a directory.
func (s *Storage) Exists(ctx context.Context, key string) bool {
	kv, isDir, err := s.lookup(ctx, key)
	return err == nil && (kv != nil || isDir)
}

// Stat describes key, which may be a stored key or a directory.
func (s *Storage) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	kv, isDir, err := s.lookup(ctx, key)
	switch {
	case err != nil:
		return certmagic.KeyInfo{}, err
	case kv != nil:
		value, modified, err := decode(kv.Value)
		return certmagic.KeyInfo{Key: key, Modified: modified, Size: int64(len(value)), IsTerminal: true}, err
	case isDir:
		return certmagic.KeyInfo{Key: key}, nil
	default:
		return certmagic.KeyInfo{}, notExist(key)
	}
}

// List returns the entries directly below dir, keys and directories, or, if
// recursive, every key below it.
func (s *Storage) List(ctx context.Context, dir string, recursive bool) ([]string, error) {
	prefix := below(dir)
	resp, err := s.client.Get(ctx, s.data+prefix, clientv3.WithPrefix(), clientv3.WithKeysOnly(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, notExist(dir)
	}

	// etcd sorts keys bytewise, which keeps a directory's contents together:
	// comparing with the previous entry is enough to drop duplicates.
	var entries []string
	for _, kv := range resp.Kvs {
		key := strings.TrimPrefix(string(kv.Key), s.data)
		if !recursive {
			child, _, _ := strings.Cut(strings.TrimPrefix(key, prefix), "/")
			key = prefix + child
		}
		if len(entries) == 0 || entries[len(entries)-1] != key {
			entries = append(entries, key)
		}
	}
	return entries, nil
}

// Lock takes the lock for name, waiting as long as ctx allows.
func (s *Storage) Lock(ctx context.Context, name string) error {
	// Each lock gets a lease (session) of its own: mutexes on a shared
	// session share their key, so they would not exclude each other within
	// this process. The lease is granted with ctx, but kept alive for as long
	// as the client lives, not just ctx.
	lease, err := s.client.Grant(ctx, int64(lockTTL/time.Second))
	if err != nil {
		return fmt.Errorf("locking %s: %w", name, err)
	}
	session, err := concurrency.NewSession(s.client, concurrency.WithLease(lease.ID))
	if err != nil {
		s.client.Revoke(context.WithoutCancel(ctx), lease.ID)
		return fmt.Errorf("locking %s: %w", name, err)
	}
	if err := concurrency.NewMutex(session, s.lockNS+name).Lock(ctx); err != nil {
		session.Close()
		return fmt.Errorf("locking %s: %w", name, err)
	}
	s.mu.Lock()
	s.locks[name] = session
	s.mu.Unlock()
	return nil
}

// Unlock releases the lock for name, taken by Lock on this instance.
func (s *Storage) Unlock(_ context.Context, name string) error {
	s.mu.Lock()
	session, ok := s.locks[name]
	delete(s.locks, name)
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("unlocking %s: not locked by this instance", name)
	}
	// Revoking the lease deletes the mutex key, which is the unlock.
	return session.Close()
}

// lookup fetches key, and whether anything lies below it, in one round trip.
func (s *Storage) lookup(ctx context.Context, key string) (kv *mvccpb.KeyValue, isDir bool, err error) {
	resp, err := s.client.Txn(ctx).Then(
		clientv3.OpGet(s.data+key),
		clientv3.OpGet(s.data+below(key), clientv3.WithPrefix(), clientv3.WithCountOnly()),
	).Commit()
	if err != nil {
		return nil, false, err
	}
	if kvs := resp.Responses[0].GetResponseRange().Kvs; len(kvs) > 0 {
		kv = kvs[0]
	}
	return kv, resp.Responses[1].GetResponseRange().Count > 0, nil
}

// below returns the prefix of every key under dir; the empty dir is the root.
func below(dir string) string {
	dir = strings.TrimSuffix(dir, "/")
	if dir == "" {
		return ""
	}
	return dir + "/"
}

// A stored value is a format byte, the time it was stored (Unix nanoseconds,
// big-endian) and then the bytes Caddy gave. etcd keeps no such time itself,
// and Stat has to report one.
const (
	format    = 1
	headerLen = 1 + 8
)

func encode(value []byte, modified time.Time) []byte {
	record := make([]byte, headerLen, headerLen+len(value))
	record[0] = format
	binary.BigEndian.PutUint64(record[1:], uint64(modified.UnixNano()))
	return append(record, value...)
}

func decode(record []byte) (value []byte, modified time.Time, err error) {
	if len(record) < headerLen || record[0] != format {
		return nil, time.Time{}, errors.New("etcd storage: not a value stored by this module")
	}
	nanos := int64(binary.BigEndian.Uint64(record[1:headerLen]))
	return record[headerLen:], time.Unix(0, nanos), nil
}

func notExist(key string) error {
	return fmt.Errorf("%s: %w", key, fs.ErrNotExist)
}

// Interface guards
var (
	_ caddy.Provisioner      = (*Storage)(nil)
	_ caddy.CleanerUpper     = (*Storage)(nil)
	_ caddy.StorageConverter = (*Storage)(nil)
	_ certmagic.Storage      = (*Storage)(nil)
	_ fmt.Stringer           = (*Storage)(nil)
)
