package yaraus

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

// time2Number converts time.Time to lua number
func timeToNumber(t time.Time) string {
	return fmt.Sprintf("%d.%09d", t.Unix(), t.Nanosecond())
}

// numberToTime converts lua number to time.Time
func numberToTime(s string) time.Time {
	timestamp, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return time.Time{}
	}
	fintSec, fracSec := math.Modf(timestamp)
	t := time.Unix(int64(fintSec), int64(fracSec*1e9))
	return t
}

func durationToNumber(d time.Duration) string {
	sign := int64(1)
	if d < 0 {
		sign = -1
	}
	return fmt.Sprintf("%d.%09d", int64(d/time.Second), sign*int64(d%time.Second))
}
