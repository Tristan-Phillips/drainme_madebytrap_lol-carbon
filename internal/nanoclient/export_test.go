package nanoclient

import "time"

// SetClock replaces the clock that decides when a failed endpoint is tried first again.
func SetClock(c *Client, now func() time.Time) { c.now = now }
