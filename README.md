# caddy-storage-etcd

A [Caddy](https://caddyserver.com) storage module that keeps certificates, keys and
the other assets Caddy manages in [etcd](https://etcd.io), so that several Caddy
instances share them, along with the locks that stop them from all renewing
the same certificate at once.

```
xcaddy build --with github.com/alien43/caddy-storage-etcd
```

## Configuration

```caddyfile
{
	storage etcd {
		endpoints etcd1:2379 etcd2:2379 etcd3:2379
		prefix    /caddy
	}
}
```

The endpoints can also go on the first line: `storage etcd etcd1:2379 etcd2:2379`.

JSON:

```json
"storage": {
	"module": "etcd",
	"endpoints": ["etcd1:2379", "etcd2:2379", "etcd3:2379"],
	"prefix": "/caddy"
}
```

| Option | Default | |
|---|---|---|
| `endpoints` | (required) | etcd members, as `host:port` or URLs. The client fails over between them. |
| `prefix` | `/caddy` | The etcd key prefix this module owns. |

Caddy can't start or reload while etcd is unreachable. It needs its storage
then, as with any storage module. Running instances keep serving.

## How it works

- **Values** live at `<prefix>/data/<key>`, where the key is certmagic's
  slash-separated path. A *directory* is any key prefix ending at a slash; it
  exists while something lies below it. Each value starts with a 9-byte header:
  a format byte and the time it was stored, because etcd keeps no
  modification time and Caddy asks for one.
- **Locks** are etcd mutexes under `<prefix>/locks/`, each on a lease of its own.
  Waiters are served first come, first served, and `Lock` returns when the
  context ends. The client keeps a held lock's lease alive. If the holder dies,
  the lease expires after 15 seconds and the lock goes with it, so there's no
  stale lock to clean up.

## Moving existing certificates in

Caddy can copy its storage between modules:

```
caddy storage export --config old.json --output certs.tar   # config with the old storage
caddy storage import --config new.json --input certs.tar    # config with storage etcd
```

The archive holds private keys; delete it afterwards.

## Limits

- No TLS or authentication to etcd yet: use it on a network you trust, or put
  a firewall in front of etcd's client port.
- A value can't exceed etcd's request size limit (1.5 MiB by default).
  Certificates and keys are a few kilobytes.
- If etcd is unreachable for longer than the lock lease, a held lock lapses
  while its holder still works. The worst case is two instances ordering the
  same certificate.

## Testing

The test needs an etcd it may write to. It works under a prefix of its own
and removes it afterwards:

```
podman run --rm -d -p 127.0.0.1:52379:2379 quay.io/coreos/etcd:v3.6.14 etcd \
	--listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://127.0.0.1:2379
ETCDSTORAGE_TEST_ENDPOINTS=127.0.0.1:52379 go test -race ./...
```

## License

MIT
