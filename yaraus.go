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
	if s[0] < 'A' || 'Z' < s[0] {
		return 0, errors.New("yaraus: id is invalid")
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
	ClientIDCount           int64   `json:"client_id_count"`
	GetIDCount              int64   `json:"client_get_id_count"`
	GetIDSuccess            int64   `json:"client_get_id_success"`
	GetIDNoAvailableID      int64   `json:"get_id_no_available_id"`
	ExtendTTLCount          int64   `json:"extend_ttl_count"`
	ExtendTTLSuccess        int64   `json:"extend_ttl_success"`
	ExtendTTLOwnershipError int64   `json:"extend_ttl_ownership_error"`
	ExtendTTLExpireWarning  int64   `json:"extend_ttl_expire_warning"`
	UnusingIDs              int64   `json:"unusing_ids"`
	UsingIDs                int64   `json:"using_ids"`
	UsingTTLMax             float64 `json:"using_ttl_max"`
	UsingTTLMid             float64 `json:"using_ttl_mid"`
	UsingTTLMin             float64 `json:"using_ttl_min"`
}

const (
	// Total number of client IDs generated
	statsClientIDCount = "client_id_count"

	// Total number of GetID called
	statsGetIDCount = "get_id_count"

	// Total number of GetID succeeded
	statsGetIDSuccess = "get_id_success"

	// Total number of GetID error
	statsGetIDNoAvailableID = "get_id_no_available_id"

	// Total number of ExtendTTL called
	statsExtendTTLCount = "extend_ttl_count"

	// Total number of ExtendTTL succeeded
	statsExtendTTLSuccess = "extend_ttl_success"

	// Total number of ExtendTTL ownership check failed
	statsExtendTTLOwnershipError = "extend_ttl_ownership_error"

	// Total number of ExtendTTL expire check failed
	statsExtendTTLExpireWarning = "extend_ttl_expire_warning"

	// max value of id
	statsMax = "max"

	// min value of id
	statsMin = "min"
)

// Yaraus is Yet Another Ranged Unique id Supplier
type Yaraus struct {
	c *redis.Client

	clientID string
	id       yarausID
	expireAt time.Time
	mu       sync.RWMutex

	scriptInit      *redis.Script
	scriptGet       *redis.Script
	scriptExtendTTL *redis.Script
	scriptRelease   *redis.Script
	scriptGetTime   string
	useTimeCommand  bool
	useWait         bool

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
		Expire:    30 * time.Minute,
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

	id, err := y.c.HIncrBy(y.keyStats(), statsClientIDCount, 1).Result()
	if err != nil {
		return err
	}
	t := time.Now()
	y.clientID = fmt.Sprintf("%s-%d.%03d-%d", h, t.Unix(), t.Nanosecond()/1e6, id)

	return nil
}

func (y *Yaraus) initGetTimeScript() error {
	// check it is able to stop scripts replication (from Redis 3.2)
	ret, err := y.c.Eval(`return redis.replicate_commands ~= nil and 1 or 0`, nil).Result()
	if err != nil {
		return err
	}
	iret, ok := ret.(int64)
	if !ok || iret == 0 {
		log.Println("WARNING: your redis not support redis.replicate_commands")
		return nil
	}

	// it is Redis 3.2 or over.
	// use TIME command to get the time.
	y.scriptGetTime = `
if redis.replicate_commands() then
    local t = redis.call("TIME")
    time = t[1] + t[2]*1e-6
end`
	y.useTimeCommand = true

	return nil
}

// Init initializes id database.
// If the initialization is success, it returns true.
// If the database has already initialized, it returns false.
func (y *Yaraus) Init() (bool, error) {
	y.mu.Lock()
	defer y.mu.Unlock()

	if y.scriptInit == nil {
		y.scriptInit = redis.NewScript(`
local key_ids = KEYS[1]
local key_clients = KEYS[2]
local key_stats = KEYS[3]
local min = tonumber(ARGV[1])
local max = tonumber(ARGV[2])

if redis.call("EXISTS", key_ids) ~= 0 then
    return 0 -- already initialized
end

for i = min, max do
    local s = string.format("%d", i)
    s = string.char(string.len(s)-1+string.byte('A')) .. s
    redis.call("ZADD", key_ids, 0, s)
end
redis.call("HMSET", key_stats, "` + statsMin + `", min, "` + statsMax + `", max)
return 1
`)
	}

	ret, err := y.scriptInit.Run(
		y.c,
		[]string{
			y.keyIDs(),
			y.keyClients(),
			y.keyStats(),
		},
		y.min,
		y.max,
	).Result()
	if err != nil {
		return false, err
	}
	iret, ok := ret.(int64)
	return ok && iret == 1, nil
}

func (y *Yaraus) checkWaitCommand() error {
	_, err := y.c.Wait(0, 0).Result()
	if err == nil {
		y.useWait = true
		return nil
	}
	if strings.Index(strings.ToLower(err.Error()), "unknown command") >= 0 {
		log.Println("WARNING: your redis not support WAIT command")
		return nil
	}
	return err
}

func slaveCount(client *redis.Client) (int, error) {
	cmd := redis.NewSliceCmd("ROLE")
	client.Process(cmd)
	v, err := cmd.Result()
	if err != nil {
		return 0, err
	}
	if len(v) < 3 {
		return 0, errors.New("yaraus: role format error")
	}
	role, ok := v[0].(string)
	if !ok {
		return 0, errors.New("yaraus: role format error")
	}
	if role != "master" {
		return 0, fmt.Errorf("yaraus: invalid role: %s", role)
	}
	slaves, ok := v[2].([]interface{})
	if !ok {
		return 0, errors.New("yaraus: role format error")
	}
	return len(slaves), nil
}

// wait waits slaves of redis
func (y *Yaraus) wait(id uint) *Error {
	if !y.useWait {
		return nil
	}

	// get slaves count
	sc, err := slaveCount(y.c)
	if err != nil {
		return &Error{
			Err:      err,
			ID:       id,
			ClientID: y.clientID,
		}
	}
	if sc == 0 {
		return nil
	}

	// wait for redis slaves.
	i, err := y.c.Wait(sc/2+1, y.Interval).Result()
	if err != nil {
		return &Error{
			Err:      err,
			ID:       id,
			ClientID: y.clientID,
		}
	}
	if int(i) < sc/2+1 {
		return &Error{
			Err:      fmt.Errorf("failed to sync, got %d, want %d", int(i), sc/2+1),
			ID:       id,
			ClientID: y.clientID,
		}
	}

	return nil
}

// Get gets new id
func (y *Yaraus) Get(d time.Duration) error {
	y.mu.Lock()
	defer y.mu.Unlock()

	if err := y.initGetTimeScript(); err != nil {
		return err
	}
	if err := y.checkWaitCommand(); err != nil {
		return err
	}

	err := y.getClientID()
	if err != nil {
		return err
	}

	if y.scriptGet == nil {
		y.scriptGet = redis.NewScript(`
local key_ids = KEYS[1]
local key_clients = KEYS[2]
local key_stats = KEYS[3]
local min = tonumber(ARGV[1])
local max = tonumber(ARGV[2])
local generator_id = ARGV[3]
local time = tonumber(ARGV[4])
local expire = tonumber(ARGV[5])
` + y.scriptGetTime + `

if redis.call("EXISTS", key_ids) == 0 then
    redis.call("HINCRBY", key_stats, "` + statsGetIDNoAvailableID + `", 1)
    return {err="no available id. database is not initialized."}
end

-- search available id
redis.call("HINCRBY", key_stats, "` + statsGetIDCount + `", 1)
local ret = redis.call("ZRANGE", key_ids, 0, 0, "WITHSCORES")
local worker_id = ret[1]
local worker_exp = tonumber(ret[2])
if worker_exp >= time then
    redis.call("HINCRBY", key_stats, "` + statsGetIDNoAvailableID + `", 1)
    return {err="no available id"}
end

-- register
worker_exp = time + expire
redis.call("ZADD", key_ids, worker_exp, worker_id)
redis.call("HSET", key_clients, worker_id, generator_id)
redis.call("HINCRBY", key_stats, "` + statsGetIDSuccess + `", 1)
return {worker_id, tostring(worker_exp)}
`)
	}

	now := time.Now()
	ret, err := y.scriptGet.Run(
		y.c,
		[]string{
			y.keyIDs(),
			y.keyClients(),
			y.keyStats(),
		},
		y.min,
		y.max,
		y.clientID,
		timeToNumber(now),
		durationToNumber(d),
	).Result()
	if err != nil {
		return err
	}
	iret, ok := ret.([]interface{})
	if !ok {
		return fmt.Errorf("unexpected response %v", ret)
	}
	workerID, err := parseYarausID(iret[0].(string))
	if err != nil {
		return &Error{
			Err:         err,
			ClientID:    y.clientID,
			IsInvalidID: true,
		}
	}
	y.id = workerID
	y.expireAt = now.Add(d)

	if err := y.wait(uint(workerID)); err != nil {
		y.release()
		err.IsInvalidID = true
	}

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
local key_stats = KEYS[3]
local generator_id = ARGV[1]
local worker_id = ARGV[2]
local time = tonumber(ARGV[3])
local expire = tonumber(ARGV[4])
` + y.scriptGetTime + `
redis.call("HINCRBY", key_stats, "` + statsExtendTTLCount + `", 1)

-- check ownership
local owner = redis.call("HGET", key_clients, worker_id)
if owner ~= generator_id then
    redis.call("HINCRBY", key_stats, "` + statsExtendTTLOwnershipError + `", 1)
    return {err="[` + invalidErrorSentinel + `] ownership check error"}
end

-- check expire
local score = redis.call("ZSCORE", key_ids, worker_id)
if score == false then
    return {err="[` + invalidErrorSentinel + `] id has removed"}
end
if tonumber(score) < time then
    redis.call("HINCRBY", key_stats, "` + statsExtendTTLExpireWarning + `", 1)
end

-- extend expire time
local worker_exp = time + expire
redis.call("ZADD", key_ids, worker_exp, worker_id)
redis.call("HINCRBY", key_stats, "` + statsExtendTTLSuccess + `", 1)
return {worker_id, tostring(worker_exp)}
`)
	}

	now := time.Now()
	ret, err := y.scriptExtendTTL.Run(
		y.c,
		[]string{
			y.keyIDs(),
			y.keyClients(),
			y.keyStats(),
		},
		y.clientID,
		y.id.String(),
		timeToNumber(now),
		durationToNumber(d),
	).Result()
	if err != nil {
		invalidID := strings.Index(err.Error(), invalidErrorSentinel) >= 0
		if invalidID {
			y.id = 0
		}
		return &Error{
			Err:         err,
			ClientID:    y.clientID,
			ID:          uint(y.id),
			IsInvalidID: invalidID,
		}
	}
	iret, ok := ret.([]interface{})
	if !ok {
		return fmt.Errorf("unexpected response %v", ret)
	}
	id, err := parseYarausID(iret[0].(string))
	if err != nil {
		y.id = id
		return &Error{
			Err:         err,
			ClientID:    y.clientID,
			IsInvalidID: true,
		}
	}

	if err := y.wait(uint(id)); err != nil {
		return err
	}

	y.id = id
	y.expireAt = now.Add(d)

	return nil
}

// Release releases worker id.
func (y *Yaraus) Release() error {
	y.mu.Lock()
	defer y.mu.Unlock()
	return y.release()
}

// Release releases worker id.
func (y *Yaraus) release() error {
	if y.scriptRelease == nil {
		y.scriptRelease = redis.NewScript(`
local key_ids = KEYS[1]
local key_clients = KEYS[2]
local yaraus_id = ARGV[1]
local id = ARGV[2]
local time = tonumber(ARGV[3])
` + y.scriptGetTime + `

-- check ownership
local owner = redis.call("HGET", key_clients, id)
if owner ~= yaraus_id then
    return {err="ownership check error"}
end

-- check expire
local score = redis.call("ZSCORE", key_ids, id)
if score == false then
    return {err="id has already removed"}
end
if tonumber(score) < time then
    redis.call("HINCRBY", key_stats, "` + statsExtendTTLExpireWarning + `", 1)
end

-- release ownership
redis.call("HDEL", key_clients, id)

-- set id expored
redis.call("ZADD", key_ids, time, id)

return {id, tostring(time)}
`)
	}

	now := time.Now()
	ret, err := y.scriptRelease.Run(
		y.c,
		[]string{
			y.keyIDs(),
			y.keyClients(),
		},
		y.clientID,
		y.id.String(),
		timeToNumber(now),
	).Result()
	if err != nil {
		return &Error{
			Err:         err,
			ClientID:    y.clientID,
			ID:          uint(y.id),
			IsInvalidID: strings.Index(err.Error(), invalidErrorSentinel) >= 0,
		}
	}
	iret := ret.([]interface{})
	id, err := parseYarausID(iret[0].(string))
	if err != nil {
		return &Error{
			Err:         err,
			ClientID:    y.clientID,
			IsInvalidID: true,
		}
	}
	y.id = id
	y.expireAt = now

	return nil
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
		timer := time.NewTimer(y.Interval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
	defer func() {
		log.Println("releasing id...")
		err := y.Release()
		if err != nil {
			log.Println("release error", err)
		}
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
		timer := time.NewTimer(d)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}

	log.Println("starting...")
	return r.Run(ctx, y.ID())
}

// Stats returns statistics information.
func (y *Yaraus) Stats() (Stats, error) {
	var stats Stats
	now := time.Now() // TODO: use TIME command
	epoch := timeToNumber(now)
	pipeline := y.c.TxPipeline()
	retStats := pipeline.HGetAll(y.keyStats())
	unusing := pipeline.ZCount(y.keyIDs(), "-inf", "("+epoch)
	using := pipeline.ZCount(y.keyIDs(), epoch, "+inf")
	_, err := pipeline.Exec()
	if err != nil {
		return Stats{}, err
	}
	val := retStats.Val()
	stats.ClientIDCount, _ = strconv.ParseInt(val[statsClientIDCount], 10, 64)
	stats.GetIDCount, _ = strconv.ParseInt(val[statsGetIDCount], 10, 64)
	stats.GetIDSuccess, _ = strconv.ParseInt(val[statsGetIDSuccess], 10, 64)
	stats.GetIDNoAvailableID, _ = strconv.ParseInt(val[statsGetIDNoAvailableID], 10, 64)
	stats.ExtendTTLCount, _ = strconv.ParseInt(val[statsExtendTTLCount], 10, 64)
	stats.ExtendTTLSuccess, _ = strconv.ParseInt(val[statsExtendTTLSuccess], 10, 64)
	stats.ExtendTTLOwnershipError, _ = strconv.ParseInt(val[statsExtendTTLOwnershipError], 10, 64)
	stats.ExtendTTLExpireWarning, _ = strconv.ParseInt(val[statsExtendTTLExpireWarning], 10, 64)
	stats.UnusingIDs = unusing.Val()
	stats.UsingIDs = using.Val()

	// get ttl of the using ids
	if using.Val() > 0 {
		epoch := timeToFloat64(now)
		pipeline := y.c.TxPipeline()
		min := pipeline.ZRangeWithScores(y.keyIDs(), -1, -1)
		mid := pipeline.ZRangeWithScores(y.keyIDs(), -(stats.UsingIDs+1)/2, -(stats.UsingIDs+1)/2)
		max := pipeline.ZRangeWithScores(y.keyIDs(), -stats.UsingIDs, -stats.UsingIDs)
		_, err := pipeline.Exec()
		if err != nil {
			return Stats{}, err
		}
		if len(max.Val()) >= 1 {
			stats.UsingTTLMax = max.Val()[0].Score - epoch
		}
		if len(mid.Val()) >= 1 {
			stats.UsingTTLMid = mid.Val()[0].Score - epoch
		}
		if len(min.Val()) >= 1 {
			stats.UsingTTLMin = min.Val()[0].Score - epoch
		}
	}

	return stats, nil
}

// ClientInfo is information of Yaraus client.
type ClientInfo struct {
	ClientID string    `json:"client_id"`
	ID       uint      `json:"id"`
	ExpireAt time.Time `json:"expire_at"`
	Using    bool      `json:"using"`
	TTL      float64   `json:"ttl"`
}

// List lists yaraus's clients
func (y *Yaraus) List() ([]ClientInfo, error) {
	// get min and max fron redis
	pipeline := y.c.TxPipeline()
	retMin := pipeline.HGet(y.keyStats(), statsMin)
	retMax := pipeline.HGet(y.keyStats(), statsMax)
	_, err := pipeline.Exec()
	if err != nil {
		return nil, err
	}
	min, err := retMin.Uint64()
	if err != nil {
		return nil, fmt.Errorf("yaraus: invalid min value %v", err)
	}
	max, err := retMax.Uint64()
	if err != nil {
		return nil, fmt.Errorf("yaraus: invalid max value %v", err)
	}

	// get info of each ids.
	clients := make([]ClientInfo, 0, max-min+1)
	for i := min; i <= max; i++ {
		var now time.Time
		var cmdTime *redis.TimeCmd
		strID := yarausID(i).String()
		pipeline := y.c.TxPipeline()
		retClientID := pipeline.HGet(y.keyClients(), strID)
		retScore := pipeline.ZScore(y.keyIDs(), strID)
		if y.useTimeCommand {
			cmdTime = pipeline.Time()
		}
		pipeline.Exec()
		var clientID string
		if y.useTimeCommand {
			now = cmdTime.Val()
		} else {
			now = time.Now()
		}
		if retClientID.Err() == nil {
			clientID, _ = retClientID.Result()
		}
		expire := float64ToTime(retScore.Val())
		using := now.Before(expire)
		ttl := expire.Sub(now).Seconds()
		clients = append(clients, ClientInfo{
			ClientID: clientID,
			ID:       uint(i),
			ExpireAt: expire,
			Using:    using,
			TTL:      ttl,
		})
	}

	return clients, nil
}

func (y *Yaraus) keyIDs() string {
	return y.namespace + ":ids"
}

func (y *Yaraus) keyClients() string {
	return y.namespace + ":clients"
}

func (y *Yaraus) keyStats() string {
	return y.namespace + ":stats"
}
