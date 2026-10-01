package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/machinebox/graphql"
)

const liveRateLimitPolicy = `"Free";q=60;w=60;burst=10, "daily";q=5000;w=86400`
const liveRateLimitState = `"Free";r=8;t=42, "daily";r=4231;t=51234`

func TestParseRateLimitTwoPolicies(t *testing.T) {
	h := http.Header{}
	h.Set("RateLimit-Policy", liveRateLimitPolicy)
	h.Set("RateLimit", liveRateLimitState)
	want := []Limit{
		{Name: "Free", Quota: 60, Window: time.Minute, Burst: 10, Remaining: 8, Reset: 42 * time.Second, Known: true},
		{Name: "daily", Quota: 5000, Window: 24 * time.Hour, Remaining: 4231, Reset: 51234 * time.Second, Known: true},
	}
	got := parseRateLimit(h)
	if got == nil || len(got.Limits) != 2 || got.Limits[0] != want[0] || got.Limits[1] != want[1] {
		t.Fatalf("parseRateLimit = %+v, want %+v", got, want)
	}
	if r := got.Rate(); r == nil || r.Name != "Free" {
		t.Fatalf("Rate() = %+v, want the Free plan", r)
	}
	if d := got.Daily(); d == nil || d.Name != "daily" {
		t.Fatalf("Daily() = %+v, want the daily budget", d)
	}
}

func TestParseRateLimitQuotedNameWithSeparators(t *testing.T) {
	h := http.Header{}
	h.Set("RateLimit-Policy", `"a,b;c";q=10;w=60`)
	got := parseRateLimit(h)
	if got == nil || len(got.Limits) != 1 || got.Limits[0].Name != "a,b;c" || got.Limits[0].Quota != 10 {
		t.Fatalf("parseRateLimit = %+v, want one limit named a,b;c", got)
	}
}

func TestParseRateLimitAbsentAndMalformed(t *testing.T) {
	if got := parseRateLimit(http.Header{}); got != nil {
		t.Fatalf("no headers: got %+v, want nil", got)
	}
	h := http.Header{}
	h.Set("RateLimit", `"default";r=abc;t=-3;junk`)
	got := parseRateLimit(h)
	if got == nil || len(got.Limits) != 1 || got.Limits[0] != (Limit{Name: "default"}) {
		t.Fatalf("malformed params: got %+v, want name only", got)
	}
}

func TestClientRecordsRateLimitOnSuccessAndError(t *testing.T) {
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("RateLimit", `"default";r=7;t=30`)
		w.WriteHeader(status)
		if status == http.StatusOK {
			w.Write([]byte(`{"data":{}}`))
		}
	}))
	defer srv.Close()

	c := newClientWithEndpoint(srv.URL, "tok")
	if c.LastRateLimit() != nil {
		t.Fatal("LastRateLimit before any request should be nil")
	}
	for _, code := range []int{http.StatusOK, http.StatusForbidden} {
		status = code
		c.lastReq = time.Time{}
		_ = c.do(context.Background(), graphql.NewRequest(`query { me { id } }`), &struct{}{})
		if rl := c.LastRateLimit(); rl == nil || len(rl.Limits) != 1 || rl.Limits[0].Remaining != 7 || rl.Limits[0].Reset != 30*time.Second {
			t.Fatalf("status %d: LastRateLimit = %+v, want remaining 7 reset 30s", code, rl)
		}
		c.setRateLimit(nil)
	}
}

func init() {
	// Deterministic delays; TestJitter* use the real one.
	jitterFn = func(time.Duration) time.Duration { return 0 }
}

func timeThrottle(t *testing.T, c *Client, calls int) time.Duration {
	t.Helper()
	start := time.Now()
	for i := 0; i < calls; i++ {
		if err := c.throttle(context.Background()); err != nil {
			t.Fatalf("throttle: %v", err)
		}
	}
	return time.Since(start)
}

func rateLimitOf(limits ...Limit) *RateLimit { return &RateLimit{Limits: limits} }

// plan10PerSec: 10 tokens/s, burst 3.
func plan10PerSec(remaining int) Limit {
	return Limit{Name: "plan", Quota: 600, Window: time.Minute, Burst: 3, Remaining: remaining, Known: true}
}

func TestThrottleDefaultsToOnePerSecond(t *testing.T) {
	c := &Client{}
	if got := timeThrottle(t, c, 2); got < 900*time.Millisecond {
		t.Fatalf("two throttled calls took %v, want ~%v", got, minRequestInterval)
	}
}

func TestThrottleFallsBackToPolicyIntervalWhenStateUnknown(t *testing.T) {
	c := &Client{}
	// Policy only: Remaining 0 is unreported, not empty.
	c.setRateLimit(rateLimitOf(Limit{Name: "plan", Quota: 10, Window: time.Second}))
	got := timeThrottle(t, c, 3) // first is free, then two gaps of 100ms
	if got < 150*time.Millisecond || got > 600*time.Millisecond {
		t.Fatalf("three calls took %v, want ~200ms from the 10/s policy", got)
	}
}

func TestThrottleSpendsBurstWithoutWaiting(t *testing.T) {
	c := &Client{}
	c.setRateLimit(rateLimitOf(plan10PerSec(3)))
	if got := timeThrottle(t, c, 3); got > 50*time.Millisecond {
		t.Fatalf("burst of 3 took %v, want no waiting while tokens remain", got)
	}
	// Empty: next token in 100ms.
	if got := timeThrottle(t, c, 1); got < 80*time.Millisecond || got > 400*time.Millisecond {
		t.Fatalf("call after the burst took %v, want ~100ms for one token", got)
	}
}

func TestThrottleQueuesWhenBucketEmpty(t *testing.T) {
	c := &Client{}
	c.setRateLimit(rateLimitOf(plan10PerSec(1)))
	got := timeThrottle(t, c, 3) // 1 token, then 100ms and 200ms out
	if got < 150*time.Millisecond || got > 600*time.Millisecond {
		t.Fatalf("three calls took %v, want ~200ms", got)
	}
}

func TestThrottleEmptyBucketWaitsForRefill(t *testing.T) {
	c := &Client{}
	c.setRateLimit(rateLimitOf(plan10PerSec(0)))
	if got := timeThrottle(t, c, 1); got < 80*time.Millisecond || got > 400*time.Millisecond {
		t.Fatalf("empty bucket: call took %v, want ~100ms for one token", got)
	}
}

func TestThrottleBucketRefillsToBurstCap(t *testing.T) {
	c := &Client{}
	c.setRateLimit(rateLimitOf(plan10PerSec(0)))
	c.rateLimitAt = time.Now().Add(-10 * time.Second) // long idle: refilled, capped at burst
	if got := timeThrottle(t, c, 3); got > 50*time.Millisecond {
		t.Fatalf("3 calls after idling took %v, want a full burst", got)
	}
	if got := timeThrottle(t, c, 1); got < 80*time.Millisecond {
		t.Fatalf("4th call took %v, want the bucket capped at burst 3", got)
	}
}

func TestThrottleDailyBudgetCapsPlan(t *testing.T) {
	c := &Client{}
	plan := plan10PerSec(3)
	daily := Limit{Name: "daily", Quota: 5000, Window: 24 * time.Hour, Remaining: 1, Reset: 2 * time.Hour, Known: true}
	c.setRateLimit(rateLimitOf(plan, daily))
	// 3 tokens in the bucket, 1 request left today.
	if err := c.throttle(context.Background()); err != nil {
		t.Fatalf("first call: %v", err)
	}
	err := c.throttle(context.Background())
	if err == nil || !strings.Contains(err.Error(), "rate limit exhausted") {
		t.Fatalf("second call = %v, want a rate limit exhausted error", err)
	}
}

func TestThrottleFailsFastOnExhaustedDaily(t *testing.T) {
	c := &Client{}
	// Plan has no state once daily is spent.
	c.setRateLimit(rateLimitOf(
		Limit{Name: "plan", Quota: 60, Window: time.Minute, Burst: 10},
		Limit{Name: "daily", Quota: 5000, Window: 24 * time.Hour, Remaining: 0, Reset: 4 * time.Hour, Known: true},
	))
	start := time.Now()
	err := c.throttle(context.Background())
	if err == nil || !strings.Contains(err.Error(), "rate limit exhausted") {
		t.Fatalf("throttle = %v, want a rate limit exhausted error", err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("throttle took %v, want an immediate failure", time.Since(start))
	}
	if !c.lastReq.IsZero() || c.sentSince != 0 {
		t.Fatal("a rejected call must not reserve a slot")
	}
}

func TestSetRateLimitResetsSentCount(t *testing.T) {
	c := &Client{}
	c.setRateLimit(rateLimitOf(plan10PerSec(3)))
	timeThrottle(t, c, 3)
	c.setRateLimit(rateLimitOf(plan10PerSec(3)))
	if got := timeThrottle(t, c, 3); got > 50*time.Millisecond {
		t.Fatalf("fresh state: 3 calls took %v, want no waiting", got)
	}
}

func TestJitterBounds(t *testing.T) {
	orig := jitterFn
	defer func() { jitterFn = orig }()
	jitterFn = func(max time.Duration) time.Duration { return max - 1 } // worst case

	retryAfter := &StatusError{Code: http.StatusTooManyRequests, RetryAfter: 4 * time.Second}
	if got := retryDelay(1, retryAfter); got < 4*time.Second || got >= 5*time.Second {
		t.Fatalf("Retry-After delay = %v, want in [4s, 5s)", got)
	}
	// attempt 2 base is 800ms.
	if got := retryDelay(2, errors.New("boom")); got < 400*time.Millisecond || got >= 800*time.Millisecond {
		t.Fatalf("backoff = %v, want in [400ms, 800ms)", got)
	}
	jitterFn = func(time.Duration) time.Duration { return 0 }
	if got := retryDelay(2, errors.New("boom")); got != 400*time.Millisecond {
		t.Fatalf("minimum backoff = %v, want 400ms", got)
	}
}

func TestJitterIsRandomAndBounded(t *testing.T) {
	orig := jitterFn
	defer func() { jitterFn = orig }()
	jitterFn = realJitter
	seen := map[time.Duration]bool{}
	for i := 0; i < 50; i++ {
		d := jitterFn(time.Second)
		if d < 0 || d >= time.Second {
			t.Fatalf("jitter = %v, want in [0, 1s)", d)
		}
		seen[d] = true
	}
	if len(seen) < 2 {
		t.Fatal("jitter returned the same value 50 times")
	}
	if jitterFn(0) != 0 || jitterFn(-time.Second) != 0 {
		t.Fatal("jitter of a non-positive max must be 0")
	}
}
