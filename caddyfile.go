package etcdstorage

import (
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

// UnmarshalCaddyfile sets up the storage from Caddyfile tokens:
//
//	storage etcd [<endpoints...>] {
//		endpoints <endpoints...>
//		prefix    <prefix>
//	}
func (s *Storage) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	d.Next() // the module name
	s.Endpoints = append(s.Endpoints, d.RemainingArgs()...)
	for d.NextBlock(0) {
		switch d.Val() {
		case "endpoints":
			args := d.RemainingArgs()
			if len(args) == 0 {
				return d.ArgErr()
			}
			s.Endpoints = append(s.Endpoints, args...)
		case "prefix":
			if !d.AllArgs(&s.Prefix) {
				return d.ArgErr()
			}
		default:
			return d.Errf("unrecognized subdirective %q", d.Val())
		}
	}
	return nil
}

var _ caddyfile.Unmarshaler = (*Storage)(nil)
