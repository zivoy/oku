package api

import (
	"context"
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
		{Name: "Free", Quota: 60, Window: time.Minute, Burst: 10, Remaining: 8, Reset: 42 * time.Second},
		{Name: "daily", Quota: 5000, Window: 24 * time.Hour, Remaining: 4231, Reset: 51234 * time.Second},
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

func TestThrottleDefaultsToOnePerSecond(t *testing.T) {
	c := &Client{}
	if got := timeThrottle(t, c, 2); got < 900*time.Millisecond {
		t.Fatalf("two throttled calls took %v, want ~%v", got, minRequestInterval)
	}
}

func rateLimitOf(limits ...Limit) *RateLimit { return &RateLimit{Limits: limits} }

func TestThrottleUsesPlanNotDailyBudget(t *testing.T) {
	c := &Client{}
	c.setRateLimit(rateLimitOf(
		Limit{Name: "plan", Quota: 10, Window: time.Second, Remaining: 5},
		Limit{Name: "daily", Quota: 50000, Window: 24 * time.Hour, Remaining: 49000, Reset: time.Hour},
	))
	got := timeThrottle(t, c, 3) // two gaps of 100ms; daily would be ~1.7s each
	if got < 150*time.Millisecond || got > 600*time.Millisecond {
		t.Fatalf("three calls took %v, want ~200ms from the plan policy", got)
	}
}

func TestThrottleWaitsOutExhaustedPlan(t *testing.T) {
	c := &Client{}
	c.setRateLimit(rateLimitOf(Limit{Name: "plan", Quota: 100, Window: time.Second, Remaining: 0, Reset: 300 * time.Millisecond}))
	got := timeThrottle(t, c, 1)
	if got < 250*time.Millisecond || got > 800*time.Millisecond {
		t.Fatalf("exhausted plan: call took %v, want ~300ms until reset", got)
	}
}

func TestThrottleFailsFastOnExhaustedDaily(t *testing.T) {
	c := &Client{}
	c.setRateLimit(rateLimitOf(
		Limit{Name: "plan", Quota: 60, Window: time.Minute, Remaining: 30},
		Limit{Name: "daily", Quota: 50000, Window: 24 * time.Hour, Remaining: 0, Reset: 4 * time.Hour},
	))
	start := time.Now()
	err := c.throttle(context.Background())
	if err == nil || !strings.Contains(err.Error(), "rate limit exhausted") {
		t.Fatalf("throttle = %v, want a rate limit exhausted error", err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("throttle took %v, want an immediate failure", time.Since(start))
	}
	if !c.lastReq.IsZero() {
		t.Fatal("a rejected call must not reserve a slot")
	}
}

func TestThrottleIgnoresZeroRemainingWithoutReset(t *testing.T) {
	c := &Client{}
	c.setRateLimit(rateLimitOf(Limit{Name: "plan", Quota: 100, Window: time.Second})) // policy header only
	if got := timeThrottle(t, c, 1); got > 100*time.Millisecond {
		t.Fatalf("call took %v, want no wait when remaining is merely unreported", got)
	}
}
