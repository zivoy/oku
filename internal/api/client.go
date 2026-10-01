package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/machinebox/graphql"
)

const endpoint = "https://api.hardcover.app/v1/graphql"

const (
	requestTimeout = 30 * time.Second
	attemptTimeout = 10 * time.Second
	maxRetries     = 3

	// minRequestInterval keeps us under Hardcover's 60 req/min limit; it is the
	// fallback until the API reports its own policy.
	minRequestInterval = time.Second

	// maxErrorBody bounds how much of a non-2xx response body is kept for
	// diagnostics; maxRetryAfter caps how long a Retry-After header may hold
	// a request back.
	maxErrorBody  = 512
	maxRetryAfter = 30 * time.Second
)

// Version identifies the build in the User-Agent header. It defaults to "dev";
// main can overwrite it with the binary's version at startup.
var Version = "dev"

// userAgent returns the User-Agent sent with every request.
func userAgent() string {
	return "oku/" + Version + " (+https://github.com/Kameleon21/oku)"
}

// Client wraps the machinebox/graphql client with authentication and rate limiting.
type Client struct {
	gql   *graphql.Client
	token string

	// Simple rate limiter: lastReq holds when the most recently reserved
	// request goes out, so throttle can space requests one second apart.
	// 60 req/min = 1 per second is a safe margin.
	mu      sync.Mutex
	lastReq time.Time

	// rateLimit is the most recent quota state the API reported and
	// rateLimitAt when it arrived, both guarded by mu.
	rateLimit   *RateLimit
	rateLimitAt time.Time
}

// LastRateLimit returns the quota state from the most recent response that
// carried rate limit headers, or nil if none has been seen.
func (c *Client) LastRateLimit() *RateLimit {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rateLimit
}

func (c *Client) setRateLimit(rl *RateLimit) {
	c.mu.Lock()
	c.rateLimit = rl
	c.rateLimitAt = time.Now()
	c.mu.Unlock()
}

// NetworkError marks transient request failures (timeouts, 429, 5xx, transport errors).
// The CLI maps these to exit code 2.
type NetworkError struct {
	Err error
}

func (e *NetworkError) Error() string {
	return e.Err.Error()
}

func (e *NetworkError) Unwrap() error {
	return e.Err
}

// IsNetworkError reports whether an error should map to network/API exit code 2.
func IsNetworkError(err error) bool {
	var netErr *NetworkError
	return errors.As(err, &netErr)
}

// StatusError reports a non-2xx HTTP response from the API. The graphql
// library never exposes the status itself, so statusTransport converts
// non-2xx responses into this typed error at the transport layer.
type StatusError struct {
	Code int
	// Body holds up to maxErrorBody bytes of the response body, with runs of
	// whitespace collapsed, so failures carry the server's explanation
	// instead of just a status number.
	Body string
	// RetryAfter is the parsed Retry-After header, or 0 when absent or unparseable.
	RetryAfter time.Duration
	// RateLimit is the quota state from the response headers, nil when absent.
	RateLimit *RateLimit
}

func (e *StatusError) Error() string {
	msg := fmt.Sprintf("unexpected HTTP status %d (%s)", e.Code, http.StatusText(e.Code))
	if e.Body != "" {
		msg += ": " + e.Body
	}
	return msg
}

// statusTransport turns non-2xx responses into *StatusError so retry and
// classification logic can match on the status code.
type statusTransport struct {
	base http.RoundTripper
	// onRateLimit, when set, receives the rate limit headers of every
	// response that carries them, successful or not.
	onRateLimit func(*RateLimit)
}

func (t *statusTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	rl := parseRateLimit(resp.Header)
	if rl != nil && t.onRateLimit != nil {
		t.onRateLimit(rl)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
		resp.Body.Close()
		return nil, &StatusError{
			Code: resp.StatusCode,
			// The body lands on stderr verbatim, and an edge proxy's HTML 503
			// is 512 bytes of newlines and indentation, so flatten it first.
			Body:       strings.Join(strings.Fields(string(body)), " "),
			RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
			RateLimit:  rl,
		}
	}
	return resp, nil
}

// parseRetryAfter reads the delay-seconds form of the Retry-After header,
// clamped to maxRetryAfter. The HTTP-date form and invalid values yield 0.
func parseRetryAfter(v string) time.Duration {
	secs, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || secs < 0 {
		return 0
	}
	d := time.Duration(secs) * time.Second
	if d > maxRetryAfter {
		d = maxRetryAfter
	}
	return d
}

// retryAfterDelay returns the server-requested backoff for errors carrying a
// Retry-After header worth honouring (429, or 503 when the header is present).
func retryAfterDelay(err error) time.Duration {
	var statusErr *StatusError
	if !errors.As(err, &statusErr) || statusErr.RetryAfter <= 0 {
		return 0
	}
	if statusErr.Code == http.StatusTooManyRequests || statusErr.Code == http.StatusServiceUnavailable {
		return statusErr.RetryAfter
	}
	return 0
}

// NewClient creates a new Hardcover API client with the given auth token.
// The token is normalised to include a "Bearer " prefix if missing.
func NewClient(token string) *Client {
	return newClientWithEndpoint(endpoint, token)
}

func newClientWithEndpoint(url, token string) *Client {
	c := &Client{token: normalizeToken(token)}
	httpClient := &http.Client{
		Timeout:   attemptTimeout,
		Transport: &statusTransport{base: http.DefaultTransport, onRateLimit: c.setRateLimit},
	}
	c.gql = graphql.NewClient(url, graphql.WithHTTPClient(httpClient))
	return c
}

// normalizeToken ensures the token carries the "Bearer " prefix required by the API.
// Surrounding whitespace is trimmed and any existing bearer prefix
// (case-insensitive) stripped before re-adding the canonical form.
func normalizeToken(token string) string {
	const prefix = "Bearer "
	token = strings.TrimSpace(token)
	if strings.HasPrefix(strings.ToLower(token), "bearer ") {
		return prefix + strings.TrimSpace(token[len("bearer "):])
	}
	return prefix + token
}

// do executes a GraphQL request with auth header and rate limiting.
func (c *Client) do(ctx context.Context, req *graphql.Request, resp interface{}) error {
	ctx, cancel := withRequestTimeout(ctx)
	defer cancel()

	req.Header.Set("authorization", c.token)
	req.Header.Set("User-Agent", userAgent())

	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if err := c.throttle(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			return &NetworkError{Err: err}
		}

		err := c.gql.Run(ctx, req, resp)
		if err == nil {
			return nil
		}

		if !isRetryable(err) {
			if isNetworkish(err) {
				return &NetworkError{Err: err}
			}
			return err
		}

		lastErr = err
		if attempt == maxRetries {
			break
		}

		backoff := time.Duration(attempt*attempt) * 200 * time.Millisecond
		if d := retryAfterDelay(err); d > 0 {
			backoff = d
		}
		// Sleeping out the deadline would report a bare timeout and throw away
		// lastErr — the status and body the server actually sent. A Retry-After
		// longer than the time left is a definite failure, so say so now.
		if backoff >= remaining(ctx) {
			return &NetworkError{Err: lastErr}
		}
		select {
		case <-ctx.Done():
			// A user-initiated cancel stays a cancellation; a deadline that
			// expires mid-backoff still reports what the server last said.
			if errors.Is(ctx.Err(), context.Canceled) {
				return ctx.Err()
			}
			return &NetworkError{Err: lastErr}
		case <-time.After(backoff):
		}
	}

	// The loop only exits via break, which always assigns lastErr first.
	return &NetworkError{Err: lastErr}
}

// remaining reports how long ctx has left. do always runs under a deadline
// because withRequestTimeout adds one, so the fallback is only a safety net.
func remaining(ctx context.Context) time.Duration {
	deadline, ok := ctx.Deadline()
	if !ok {
		return requestTimeout
	}
	return time.Until(deadline)
}

// throttle spaces requests to respect the API's rate limit: the interval is
// derived from the policy the API last reported, falling back to
// minRequestInterval (60 req/min), and an exhausted quota holds requests until
// its reset. The slot is reserved under the lock and the wait happens outside
// it, so concurrent callers queue up rather than blocking each other, and a
// cancelled context aborts the wait.
func (c *Client) throttle(ctx context.Context) error {
	c.mu.Lock()
	now := time.Now()
	slot := now
	if !c.lastReq.IsZero() {
		if next := c.lastReq.Add(c.intervalLocked()); next.After(slot) {
			slot = next
		}
	}
	if resetAt, ok := c.quotaResetLocked(); ok && resetAt.After(slot) {
		slot = resetAt
	}
	wait := slot.Sub(now)
	// A wait that outlasts the request (a spent daily budget resets hours
	// away) can only end in a bare timeout, so say what is wrong now, without
	// reserving the slot.
	if wait >= remaining(ctx) {
		c.mu.Unlock()
		return fmt.Errorf("rate limit exhausted; resets in %s", wait.Round(time.Second))
	}
	// Record when the request will actually go out, so a backoff that already
	// covered the interval does not trigger a second full wait.
	c.lastReq = slot
	c.mu.Unlock()

	if wait <= 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(wait):
		return nil
	}
}

// intervalLocked is the steady-state gap between requests: the plan's window
// divided by its quota when the API reported one, else minRequestInterval. The
// daily budget is spent, not paced, so it plays no part. Callers must hold c.mu.
func (c *Client) intervalLocked() time.Duration {
	if c.rateLimit != nil {
		if l := c.rateLimit.Rate(); l != nil {
			if d := l.Window / time.Duration(l.Quota); d > 0 {
				return d
			}
		}
	}
	return minRequestInterval
}

// quotaResetLocked reports when the latest exhausted limit refills. ok is
// false when every limit has requests left, no state is known, or the reset
// has already passed. A reset of 0 means the RateLimit header carried no t, so
// an empty-looking limit can't be trusted. Callers must hold c.mu.
func (c *Client) quotaResetLocked() (resetAt time.Time, ok bool) {
	if c.rateLimit == nil {
		return time.Time{}, false
	}
	for _, l := range c.rateLimit.Limits {
		if l.Remaining > 0 || l.Reset <= 0 {
			continue
		}
		if at := c.rateLimitAt.Add(l.Reset); at.After(resetAt) {
			resetAt = at
		}
	}
	return resetAt, resetAt.After(time.Now())
}

func withRequestTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, requestTimeout)
}

func isRetryable(err error) bool {
	if err == nil {
		return false
	}

	// User-initiated cancellation should not be retried.
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	// Check the HTTP status before the generic net.Error test: a *url.Error
	// wrapping a StatusError satisfies net.Error, but a 4xx is not retryable.
	if status := extractHTTPStatus(err); status != 0 {
		return status == http.StatusTooManyRequests ||
			status == http.StatusRequestTimeout ||
			status >= http.StatusInternalServerError
	}

	var netErr net.Error
	return errors.As(err, &netErr)
}

func isNetworkish(err error) bool {
	if err == nil {
		return false
	}

	// Cancellation is intentional and should not be classified as network failure.
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	if status := extractHTTPStatus(err); status != 0 {
		return status == http.StatusTooManyRequests || status == http.StatusRequestTimeout || status >= http.StatusInternalServerError
	}

	var netErr net.Error
	return errors.As(err, &netErr)
}

func extractHTTPStatus(err error) int {
	var statusErr *StatusError
	if errors.As(err, &statusErr) {
		return statusErr.Code
	}
	return 0
}
