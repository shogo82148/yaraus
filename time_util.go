package yaraus

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

// time2Number converts time.Time to lua number(unix epoch time).
func timeToNumber(t time.Time) string {
	return fmt.Sprintf("%d.%09d", t.Unix(), t.Nanosecond())
}

// time2Float64 converts time.Time to float64(unix epoch time)
func timeToFloat64(t time.Time) float64 {
	return float64(t.Unix()) + float64(t.Nanosecond())/1e9
}

// numberToTime converts lua number(unix epoch time) to time.Time
func numberToTime(s string) time.Time {
	timestamp, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return time.Time{}
	}
	fintSec, fracSec := math.Modf(timestamp)
	t := time.Unix(int64(fintSec), int64(fracSec*1e9))
	return t
}

// float64ToTime float64(unix epoch time) to time.Time
func float64ToTime(timestamp float64) time.Time {
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
