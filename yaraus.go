package yaraus

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	redis "gopkg.in/redis.v5"
)

// DisableSafeguard disables safe options.
// Use this only if you are a database removable specialist.
var DisableSafeguard bool

// dictionary sortable integer id
type yarausID uint

func parseYarausID(s string) (yarausID, error) {
	if len(s) == 0 {
		return 0, errors.New("yaraus: id is empty")
	}
	l := int(s[0] - 'A' + 1)
	if len(s) <= l {
		return 0, fmt.Errorf("yaraus: id is too short %s", s)
	}
	s = s[1 : l+1]
	id, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, err
	}
	return yarausID(id), nil
}

func (id yarausID) String() string {
	b := make([]byte, 1, 26)
	b = strconv.AppendUint(b, uint64(id), 10)
	b[0] = byte(len(b[1:]) - 1 + 'A')
	return string(b)
}

// Stats is statistics information.
type Stats struct {
	SuppliedCount int64 `json:"supplied_count"`
	UnusedIDs     int64 `json:"unused_ids"`
	UsedIDs       int64 `json:"used_ids"`
}

// Yaraus is Yet Another Ranged Unique id Supplier
type Yaraus struct {
	c *redis.Client

	clientID string
	id       yarausID
	expireAt time.Time
	mu       sync.RWMutex

	scriptGet       *redis.Script
	scriptExtendTTL *redis.Script

	min, max  uint
	namespace string
	Interval  time.Duration
	Delay     time.Duration
	Expire    time.Duration
}

// New returns new yaraus client.
func New(redisOptions *redis.Options, namespace string, min, max uint) *Yaraus {
	return &Yaraus{
		c:         redis.NewClient(redisOptions),
		min:       min,
		max:       max,
		namespace: namespace,
		Interval:  1 * time.Second,
		Delay:     2 * time.Second,
		Expire:    3 * time.Second,
	}
}

// ClientID returns the client's id.
func (y *Yaraus) ClientID() string {
	y.mu.RLock()
	defer y.mu.RUnlock()
	return y.clientID
}

// ID returns the id.
func (y *Yaraus) ID() uint {
	y.mu.RLock()
	defer y.mu.RUnlock()
	return uint(y.id)
}

// ExpireAt returns when the id expires.
func (y *Yaraus) ExpireAt() time.Time {
	y.mu.RLock()
	defer y.mu.RUnlock()
	return y.expireAt
}

// getClientID generates clients's id
func (y *Yaraus) getClientID() error {
	h, err := os.Hostname()
	if err != nil {
		return err
	}

	id, err := y.c.Incr(y.keyNextID()).Result()
	if err != nil {
		return err
	}
	t := time.Now()
	y.clientID = fmt.Sprintf("%s-%d.%03d-%d", h, t.Unix(), t.Nanosecond()/1e6, id)

	return nil
}

// Get gets new id
func (y *Yaraus) Get(d time.Duration) error {
	y.mu.Lock()
	defer y.mu.Unlock()

	err := y.getClientID()
	if err != nil {
		return err
	}

	if y.scriptGet == nil {
		y.scriptGet = redis.NewScript(`
local key_ids = KEYS[1]
local key_clients = KEYS[2]
local min = tonumber(ARGV[1])
local max = tonumber(ARGV[2])
local generator_id = ARGV[3]
local time = tonumber(ARGV[4])
local expire = tonumber(ARGV[5])

-- initailize
if redis.call("EXISTS", key_ids) == 0 then
    for i = min, max do
        local s = string.format("%d", i)
        s = string.char(string.len(s)-1+string.byte('A')) .. s
        redis.call("ZADD", key_ids, 0, s)
    end
end

-- search available id
local ret = redis.call("ZRANGE", key_ids, 0, 0, "WITHSCORES")
local worker_id = ret[1]
local worker_exp = tonumber(ret[2])
if worker_exp >= time then
    return {err="no available id"}
end

-- register
worker_exp = time + expire
redis.call("ZADD", key_ids, worker_exp, worker_id)
redis.call("HSET", key_clients, worker_id, generator_id)
return {worker_id, tostring(worker_exp)}
`)
	}

	ret, err := y.scriptGet.Run(
		y.c,
		[]string{
			y.keyIDs(),
			y.keyClients(),
		},
		y.min,
		y.max,
		y.clientID,
		timeToNumber(time.Now()),
		durationToNumber(d),
	).Result()
	if err != nil {
		return err
	}
	iret := ret.([]interface{})
	workerID, err := parseYarausID(iret[0].(string))
	if err != nil {
		return &Error{
			Err:         err.Error(),
			ClientID:    y.clientID,
			IsInvalidID: true,
		}
	}
	y.id = workerID
	y.expireAt = numberToTime(iret[1].(string))

	return nil
}

// ExtendTTL extends TTL of worker id
func (y *Yaraus) ExtendTTL(d time.Duration) error {
	y.mu.Lock()
	defer y.mu.Unlock()

	if y.scriptExtendTTL == nil {
		y.scriptExtendTTL = redis.NewScript(`
local key_ids = KEYS[1]
local key_clients = KEYS[2]
local generator_id = ARGV[1]
local worker_id = ARGV[2]
local time = tonumber(ARGV[3])
local expire = tonumber(ARGV[4])

-- check ownership
local owner = redis.call("HGET", key_clients, worker_id)
if owner ~= generator_id then
    return {err="` + invalidErrorSentinel + `"}
end

-- extend expire time
local worker_exp = time + expire
redis.call("ZADD", key_ids, worker_exp, worker_id)
return {worker_id, tostring(worker_exp)}
`)
	}

	ret, err := y.scriptExtendTTL.Run(
		y.c,
		[]string{
			y.keyIDs(),
			y.keyClients(),
		},
		y.clientID,
		y.id.String(),
		timeToNumber(time.Now()),
		durationToNumber(d),
	).Result()
	if err != nil {
		return &Error{
			Err:         err.Error(),
			ClientID:    y.clientID,
			ID:          uint(y.id),
			IsInvalidID: strings.Index(err.Error(), invalidErrorSentinel) >= 0,
		}
	}
	iret := ret.([]interface{})
	id, err := parseYarausID(iret[0].(string))
	if err != nil {
		return &Error{
			Err:         err.Error(),
			ClientID:    y.clientID,
			IsInvalidID: true,
		}
	}
	y.id = id
	y.expireAt = numberToTime(iret[1].(string))

	return nil
}

// Release releases worker id.
func (y *Yaraus) Release() error {
	return y.ExtendTTL(0)
}

func (y *Yaraus) checkSettings() {
	if !DisableSafeguard {
		var min, max time.Duration
		min = time.Second
		if y.Expire < min {
			log.Printf("WARNING: expire duration %s is too short. I use %s instead of it.", y.Expire, min)
			y.Expire = min
		}
		max = y.Expire / 2
		if y.Interval > max {
			log.Printf("WARNING: interval duration %s is too long. I use %s instead of it.", y.Interval, max)
			y.Interval = max
		}
		min = y.Interval * 2
		if y.Delay < min {
			log.Printf("WARNING: interval duration %s is too short. I use %s instead of it.", y.Delay, min)
			y.Delay = min
		}
	}
}

// Run runs the runner r.
func (y *Yaraus) Run(ctx context.Context, r Runner) error {
	y.checkSettings()

	for {
		log.Println("getting new id...")
		err := y.Get(y.Expire)
		if err == nil {
			log.Printf("client id: %s, id: %d", y.ClientID(), y.ID())
			break
		}
		log.Printf("error: %v", err)
		time.Sleep(y.Interval)
	}
	defer func() {
		log.Println("releasing id...")
		y.Release()
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// start goroutine for watching expire time
	ttlChanged := make(chan struct{}, 1)
	go func() {
		timer := time.NewTimer(y.ExpireAt().Sub(time.Now()))
		defer timer.Stop()

		for {
			select {
			case <-timer.C:
				log.Println("id expired")
				cancel()
			case <-ttlChanged:
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(y.ExpireAt().Sub(time.Now()))
			case <-ctx.Done():
				return
			}
		}
	}()

	// start goroutine for extending ttl
	go func() {
		ticker := time.NewTicker(y.Interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}

			err := y.ExtendTTL(y.Expire)
			if err != nil {
				log.Printf("error: %v", err)
				if err, ok := err.(InvalidID); ok && err.InvalidID() {
					cancel()
					return
				}
				continue
			}
			select {
			case ttlChanged <- struct{}{}:
			default:
			}
		}
	}()

	d := y.Delay
	if d > 0 {
		log.Printf("sleep %s for making sure that other generates which has same id expire.", d)
		time.Sleep(d)
	}

	log.Println("starting...")
	return r.Run(ctx, y.ID())
}

// Stats returns statistics information.
func (y *Yaraus) Stats() (Stats, error) {
	now := timeToNumber(time.Now())
	pipeline := y.c.TxPipeline()
	count := pipeline.Get(y.keyNextID())
	unused := pipeline.ZCount(y.keyIDs(), "-inf", "("+now)
	used := pipeline.ZCount(y.keyIDs(), now, "+inf")
	_, err := pipeline.Exec()
	if err != nil {
		return Stats{}, err
	}
	c, _ := strconv.ParseInt(count.Val(), 10, 64)

	return Stats{
		SuppliedCount: c,
		UnusedIDs:     unused.Val(),
		UsedIDs:       used.Val(),
	}, nil
}

func (y *Yaraus) keyNextID() string {
	return y.namespace + ":next_id"
}

func (y *Yaraus) keyIDs() string {
	return y.namespace + ":ids"
}

func (y *Yaraus) keyClients() string {
	return y.namespace + ":clients"
}
