package api

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Limit is one named rate limit policy and its current state, from the
// RateLimit-Policy and RateLimit headers. A field is zero when its header or
// parameter is absent.
type Limit struct {
	Name string

	// From RateLimit-Policy: "<name>";q=<quota>;w=<window>;burst=<burst>
	Quota  int
	Window time.Duration
	Burst  int

	// From RateLimit: "<name>";r=<remaining>;t=<reset_seconds>
	Remaining int
	Reset     time.Duration
}

// dailyWindow is the shortest window treated as a daily budget rather than a
// request rate.
const dailyWindow = 24 * time.Hour

// IsDaily reports whether l is a per-day budget (it is spent, not paced).
func (l Limit) IsDaily() bool { return l.Window >= dailyWindow }

// RateLimit holds every policy the API reported on a response. The API sends
// two: the plan's per-minute rate and a daily budget.
// See https://docs.hardcover.app/api/getting-started/#ratelimit-headers
type RateLimit struct {
	Limits []Limit
}

// Rate returns the pacing policy: the shortest-window limit with a quota, or
// nil when none was reported.
func (r *RateLimit) Rate() *Limit {
	var best *Limit
	for i := range r.Limits {
		l := &r.Limits[i]
		if l.IsDaily() || l.Quota <= 0 || l.Window <= 0 {
			continue
		}
		if best == nil || l.Window < best.Window {
			best = l
		}
	}
	return best
}

// Daily returns the daily budget, or nil when none was reported.
func (r *RateLimit) Daily() *Limit {
	for i := range r.Limits {
		if r.Limits[i].IsDaily() {
			return &r.Limits[i]
		}
	}
	return nil
}

// parseRateLimit reads the rate limit headers, returning nil when neither is
// present. Malformed parameters are skipped rather than failing the response.
func parseRateLimit(h http.Header) *RateLimit {
	policy, state := h.Get("RateLimit-Policy"), h.Get("RateLimit")
	if policy == "" && state == "" {
		return nil
	}
	rl := &RateLimit{}
	index := map[string]int{}
	limit := func(name string) *Limit {
		i, ok := index[name]
		if !ok {
			i = len(rl.Limits)
			index[name] = i
			rl.Limits = append(rl.Limits, Limit{Name: name})
		}
		return &rl.Limits[i]
	}

	for _, item := range splitUnquoted(policy, ',') {
		name, params := parseRateLimitItem(item)
		l := limit(name)
		for k, v := range params {
			switch k {
			case "q":
				l.Quota = v
			case "w":
				l.Window = time.Duration(v) * time.Second
			case "burst":
				l.Burst = v
			}
		}
	}
	for _, item := range splitUnquoted(state, ',') {
		name, params := parseRateLimitItem(item)
		l := limit(name)
		for k, v := range params {
			switch k {
			case "r":
				l.Remaining = v
			case "t":
				l.Reset = time.Duration(v) * time.Second
			}
		}
	}
	return rl
}

// parseRateLimitItem splits `"<name>";k=v;k=v` into the name and its integer
// parameters.
func parseRateLimitItem(item string) (string, map[string]int) {
	parts := splitUnquoted(item, ';')
	name := strings.Trim(strings.TrimSpace(parts[0]), `"`)
	params := map[string]int{}
	for _, p := range parts[1:] {
		k, v, ok := strings.Cut(strings.TrimSpace(p), "=")
		if !ok {
			continue
		}
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && n >= 0 {
			params[strings.ToLower(k)] = n
		}
	}
	return name, params
}

// splitUnquoted splits s on sep, ignoring separators inside double quotes
// (plan names are quoted and could contain either separator). Empty items
// are dropped.
func splitUnquoted(s string, sep rune) []string {
	var out []string
	var cur strings.Builder
	inQuote := false
	for _, r := range s {
		switch {
		case r == '"':
			inQuote = !inQuote
			cur.WriteRune(r)
		case r == sep && !inQuote:
			out = append(out, cur.String())
			cur.Reset()
		default:
			cur.WriteRune(r)
		}
	}
	out = append(out, cur.String())

	trimmed := out[:0]
	for _, item := range out {
		if strings.TrimSpace(item) != "" {
			trimmed = append(trimmed, item)
		}
	}
	return trimmed
}
