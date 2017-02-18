package main

import (
	"context"
	"encoding/json"
	"flag"
	"net"
	"net/url"
	"os"
	"time"

	"github.com/shogo82148/yaraus"
	"gopkg.in/redis.v5"
)

func main() {
	var replacement string
	var interval, expire time.Duration
	var server string
	var min, max uint
	var stats bool
	flag.StringVar(&replacement, "replacement", "worker-id", "replacement text for worker id")
	flag.DurationVar(&interval, "interval", time.Second, "interval duration time")
	flag.DurationVar(&expire, "expire", 3*time.Second, "expire duration time")
	flag.StringVar(&server, "server", "redis://localhost:6379/?ns=yaraus", "url for redis")
	flag.UintVar(&min, "min", 1, "minimam worker id")
	flag.UintVar(&max, "max", 1023, "maximam worker id")
	flag.BoolVar(&stats, "stats", false, "show stats")
	flag.Parse()

	u, err := url.Parse(server)
	if err != nil {
		os.Exit(1)
	}
	hostname := u.Hostname()
	port := u.Port()
	if port == "" {
		port = "6379"
	}
	y := yaraus.New(&redis.Options{
		Addr: net.JoinHostPort(hostname, port),
	}, u.Query().Get("ns"), min, max)
	y.Interval = interval
	y.Expire = expire

	if stats {
		s, err := y.Stats()
		if err != nil {
			return
		}
		b, _ := json.MarshalIndent(s, "", "    ")
		os.Stdout.Write(b)
		return
	}

	runner := yaraus.NewCommandRunner(replacement, flag.Args())

	y.Run(context.Background(), runner)
}
