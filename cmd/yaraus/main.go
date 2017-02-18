package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"time"

	"github.com/shogo82148/yaraus"
)

func main() {
	var replacement string
	var interval, delay, expire time.Duration
	var server string
	var min, max uint
	var stats bool
	flag.StringVar(&replacement, "replacement", "worker-id", "replacement text for worker id")
	flag.DurationVar(&interval, "interval", time.Second, "interval duration time")
	flag.DurationVar(&delay, "delay", 2*time.Second, "delay duration time for get id and use it")
	flag.DurationVar(&expire, "expire", 3*time.Second, "expire duration time")
	flag.StringVar(&server, "server", "redis://localhost:6379/?ns=yaraus", "url for redis")
	flag.BoolVar(&yaraus.DisableSafeguard, "i-am-a-database-removable-specialist", false, "disables safe options")
	flag.UintVar(&min, "min", 1, "minimam worker id")
	flag.UintVar(&max, "max", 1023, "maximam worker id")
	flag.BoolVar(&stats, "stats", false, "show stats")
	flag.Parse()

	opt, ns, err := yaraus.ParseURI(server)
	if err != nil {
		log.Fatal(err)
	}
	y := yaraus.New(opt, ns, min, max)

	if yaraus.DisableSafeguard {
		log.Println("Oh. You are a database removable specialist, are'nt you? GREAT!!")
	}
	y.Interval = interval
	y.Delay = delay
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
