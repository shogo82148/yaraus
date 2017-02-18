package yaraus

import (
	"fmt"
	"net"
	"net/url"
	"path"
	"strconv"
	"strings"

	"gopkg.in/redis.v5"
)

const (
	// DefaultNamespace is default namespace.
	DefaultNamespace = "yaraus"

	// DefaultURI is default uri.
	DefaultURI = "redis://localhost:6379/0?ns=" + DefaultNamespace
)

// ParseURI parses an uri for redis.
// e.g. redis://localhost:6379/0?ns=yaraus
func ParseURI(s string) (*redis.Options, string, error) {
	u, err := url.Parse(s)
	if err != nil {
		return nil, "", err
	}

	if u.Scheme != "redis" {
		return nil, "", fmt.Errorf("yaraus: invalid scheme %s", u.Scheme)
	}

	hostname := u.Hostname()
	port := u.Port()
	if port == "" {
		port = "6379"
	}
	opt := &redis.Options{
		Addr: net.JoinHostPort(hostname, port),
	}

	p := path.Clean(u.Path)
	if strings.HasPrefix(p, "/") {
		p = p[1:]
	}
	if idx := strings.Index(p, "/"); idx > 0 {
		p = p[:idx]
	}
	if p != "" {
		db, err := strconv.Atoi(p)
		if err != nil {
			return nil, "", fmt.Errorf("yaraus: invalid database %s", p)
		}
		opt.DB = db
	}

	ns := u.Query().Get("ns")
	if ns == "" {
		ns = DefaultNamespace
	}

	return opt, ns, nil
}
