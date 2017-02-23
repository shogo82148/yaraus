package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/shogo82148/yaraus"
)

func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		usage()
		return
	}

	var exitCode int
	cmd := args[0]
	switch cmd {
	case "init":
		exitCode = commandInit(args)
	case "run":
		exitCode = commandRun(args)
	case "stats":
		exitCode = commandStats(args)
	case "list":
		exitCode = commandList(args)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
		usage()
		exitCode = 2
	}

	os.Exit(exitCode)
}

func usage() {
	fmt.Fprint(os.Stderr, `yaraus is Yet Another Ranged Unique id Supplier

Usage:
	yaraus command [arguments]

The commands are:

	init    initialize
	run     execute commends
	stats   show id statiscation
	list    list using id
`)
}

func commandInit(args []string) int {
	var server string
	var min, max uint

	flags := flag.NewFlagSet("run", flag.ExitOnError)
	flags.StringVar(&server, "server", yaraus.DefaultURI, "url for redis")
	flags.UintVar(&min, "min", 1, "minimam worker id")
	flags.UintVar(&max, "max", 1023, "maximam worker id")
	flags.Parse(args[1:])

	opt, ns, err := yaraus.ParseURI(server)
	if err != nil {
		log.Println(err)
		return 1
	}
	y := yaraus.New(opt, ns, min, max)
	if ok, err := y.Init(); err != nil {
		log.Println(err)
	} else if ok {
		log.Println("initialize success")
	} else {
		log.Println("already initialized")
	}
	return 0
}

func commandRun(args []string) int {
	var replacement string
	var interval, delay, expire time.Duration
	var server string
	var min, max uint

	flags := flag.NewFlagSet("run", flag.ExitOnError)
	flags.StringVar(&replacement, "replacement", "worker-id", "replacement text for worker id")
	flags.DurationVar(&interval, "interval", time.Second, "interval duration time")
	flags.DurationVar(&delay, "delay", 2*time.Second, "delay duration time for get id and use it")
	flags.DurationVar(&expire, "expire", 3*time.Second, "expire duration time")
	flags.StringVar(&server, "server", yaraus.DefaultURI, "url for redis")
	flags.BoolVar(&yaraus.DisableSafeguard, "i-am-a-database-removable-specialist", false, "disables safe options")
	flags.UintVar(&min, "min", 1, "minimam worker id")
	flags.UintVar(&max, "max", 1023, "maximam worker id")
	flags.Parse(args[1:])

	opt, ns, err := yaraus.ParseURI(server)
	if err != nil {
		log.Println(err)
		return 1
	}
	y := yaraus.New(opt, ns, min, max)

	if yaraus.DisableSafeguard {
		log.Println("Oh. You are a database removable specialist, are'nt you? GREAT!!")
	}
	y.Interval = interval
	y.Delay = delay
	y.Expire = expire

	runner := yaraus.NewCommandRunner(replacement, flags.Args())
	err = y.Run(context.Background(), runner)
	if err != nil {
		if e, ok := err.(*exec.ExitError); ok {
			if s, ok := e.Sys().(syscall.WaitStatus); ok {
				return s.ExitStatus()
			}
		}
		log.Println(err)
		return 1
	}
	return 0
}

func commandStats(args []string) int {
	var server string
	flags := flag.NewFlagSet("stats", flag.ExitOnError)
	flags.StringVar(&server, "server", yaraus.DefaultURI, "url for redis")
	flags.Parse(args[1:])

	opt, ns, err := yaraus.ParseURI(server)
	if err != nil {
		log.Println(err)
		return 1
	}

	y := yaraus.New(opt, ns, 1, 1)
	s, err := y.Stats()
	if err != nil {
		log.Println(err)
		return 1
	}
	b, err := json.MarshalIndent(s, "", "    ")
	if err != nil {
		log.Println(err)
		return 1
	}
	os.Stdout.Write(b)

	return 0
}

func commandList(args []string) int {
	var server string
	flags := flag.NewFlagSet("list", flag.ExitOnError)
	flags.StringVar(&server, "server", yaraus.DefaultURI, "url for redis")
	flags.Parse(args[1:])

	opt, ns, err := yaraus.ParseURI(server)
	if err != nil {
		log.Println(err)
		return 1
	}

	y := yaraus.New(opt, ns, 1, 1)
	list, err := y.List()
	if err != nil {
		log.Println(err)
		return 1
	}
	b, err := json.MarshalIndent(list, "", "    ")
	if err != nil {
		log.Println(err)
		return 1
	}
	os.Stdout.Write(b)

	return 0
}
