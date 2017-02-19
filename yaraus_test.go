package yaraus

import (
	"testing"
	"time"

	"fmt"

	redistest "github.com/soh335/go-test-redisserver"
	"gopkg.in/redis.v5"
)

func TestID(t *testing.T) {
	cases := []struct {
		id   yarausID
		want string
	}{
		{0, "A0"},
		{1, "A1"},
		{9, "A9"},
		{10, "B10"},
		{11, "B11"},
		{99, "B99"},
		{100, "C100"},
		{101, "C101"},
		{999, "C999"},
		{1000, "D1000"},
		{1001, "D1001"},
		{9999, "D9999"},
		{10000, "E10000"},
		{10001, "E10001"},
		{99999, "E99999"},
		{100000, "F100000"},
		{100001, "F100001"},
		{999999, "F999999"},
		{1000000, "G1000000"},
		{1000001, "G1000001"},
		{18446744073709551615, "T18446744073709551615"},
	}
	for _, c := range cases {
		got := c.id.String()
		if got != c.want {
			t.Errorf("%d: got %s, want %s", uint(c.id), got, c.want)
		}
		i, err := parseYarausID(got)
		if err != nil {
			t.Errorf("parseYarausID(%s) err=%v", got, err)
		}
		if yarausID(i) != c.id {
			t.Errorf("%d: got %s, want %s", got, yarausID(i), c.id)
		}
	}
}

func TestID_error(t *testing.T) {
	cases := []string{
		"",   // empty
		"@",  // 'A' - 1
		"[",  // 'Z' + 1
		"A",  // missing number
		"AB", // 'B' is not number
		"T18446744073709551616", // max(uint64) + 1
	}
	for _, s := range cases {
		_, err := parseYarausID(s)
		if err == nil {
			t.Errorf("parseYarausID(%s) want error, got nil", s)
		}
	}
}

func TestGetClientID(t *testing.T) {
	s, err := redistest.NewServer(true, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	g := New(&redis.Options{
		Network: "unix",
		Addr:    s.Config["unixsocket"],
	}, "yaraus", 1, 1023)

	seen := map[string]struct{}{}
	for i := 0; i < 100; i++ {
		err = g.getClientID()
		if err != nil {
			t.Fatal(err)
		}
		if _, haveSeen := seen[g.clientID]; haveSeen {
			t.Errorf("id %s is not unique", g.clientID)
		}
		seen[g.clientID] = struct{}{}
	}
}

func TestGet(t *testing.T) {
	s, err := redistest.NewServer(true, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	var min, max uint = 1, 1023

	for i := min; i <= max; i++ {
		g := New(&redis.Options{
			Network: "unix",
			Addr:    s.Config["unixsocket"],
		}, "yaraus", min, max)

		err = g.Get(10 * time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if g.id != yarausID(i) {
			t.Errorf("want %d, got %d", i, g.id)
		}
		if g.expireAt.Before(time.Now()) {
			t.Errorf("invalid expire time: %s", g.expireAt)
		}
	}

	g := New(&redis.Options{
		Network: "unix",
		Addr:    s.Config["unixsocket"],
	}, "yaraus", 1, 1023)
	err = g.Get(10 * time.Second)
	if err == nil {
		t.Error("want `no available id' error, got nil")
	}
}

func TestExtendTTL(t *testing.T) {
	s, err := redistest.NewServer(true, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	var min, max uint = 1, 1023
	g := New(&redis.Options{
		Network: "unix",
		Addr:    s.Config["unixsocket"],
	}, "yaraus", min, max)
	g.Get(10 * time.Second)

	err = g.ExtendTTL(10 * time.Second)
	if err != nil {
		t.Error(err)
	}
}

func TestExtendTTLError(t *testing.T) {
	s, err := redistest.NewServer(true, nil)
	if err != nil {
		t.Fatal(err)
	}

	var min, max uint = 1, 1023
	g := New(&redis.Options{
		Network: "unix",
		Addr:    s.Config["unixsocket"],
	}, "yaraus", min, max)
	g.Get(10 * time.Second)

	s.Stop() // STOP!
	err = g.ExtendTTL(10 * time.Second)

	// we can use the id until it expires.
	err2, ok := err.(InvalidID)
	if ok && err2.InvalidID() {
		t.Errorf("want not invalid id error, got %v", t)
	}
}

func TestExtendTTLInvaidID(t *testing.T) {
	s, err := redistest.NewServer(true, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	var min, max uint = 1, 1023
	g := New(&redis.Options{
		Network: "unix",
		Addr:    s.Config["unixsocket"],
	}, "yaraus", min, max)
	g.Get(10 * time.Second)

	// I AM A DATABASE REMOVABLE SPECIALIST!!
	if err := g.c.FlushAll().Err(); err != nil {
		t.Fatal(err)
	}

	err = g.ExtendTTL(10 * time.Second)

	// we must invalidate the id for avoiding duplicate
	err2, ok := err.(InvalidID)
	if !ok || !err2.InvalidID() {
		t.Errorf("want invalid id error, got %v", t)
	}
}

func TestList(t *testing.T) {
	s, err := redistest.NewServer(true, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	now := time.Now()
	var min, max uint = 1, 1023
	g := New(&redis.Options{
		Network: "unix",
		Addr:    s.Config["unixsocket"],
	}, "yaraus", min, max)
	g.Get(10 * time.Second)

	ret, err := g.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(ret) != 1023 {
		t.Errorf("want 1, got %d", len(ret))
	}
	if ret[0].ClientID != g.ClientID() {
		t.Errorf("want %s, got %s", g.ClientID(), ret[0].ClientID)
	}
	if ret[0].ID != g.ID() {
		t.Errorf("want %d, got %d", g.ID(), ret[0].ID)
	}
	d := ret[0].ExpireAt.Sub(now)
	if 0 < d && d <= 10*time.Second {
		t.Errorf("got %s", d)
	}
}

func benchmarkList(b *testing.B, min, max uint) {
	s, err := redistest.NewServer(true, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Stop()

	g := New(&redis.Options{
		Network: "unix",
		Addr:    s.Config["unixsocket"],
	}, "yaraus", min, max)
	for i := min; i <= max; i++ {
		g.Get(time.Hour)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g.List()
	}
}

func Benchmark(b *testing.B) {
	cases := []uint{1, 10, 100, 1000, 10000}
	for _, c := range cases {
		c := c
		b.Run(
			fmt.Sprintf("%d", c),
			func(b *testing.B) {
				benchmarkList(b, 1, c)
			},
		)
	}

}
