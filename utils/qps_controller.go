package utils

import (
	"time"

	"golang.org/x/time/rate"
)

type QPSController struct {
	tokenBucket *rate.Limiter
}

func (c *QPSController) Init(qps uint64) {
	if c == nil {
		return
	}
	if qps == 0 {
		return
	}
	// 1 second = 1000000us
	limit := rate.Every(time.Duration(1000000.0/qps) * time.Microsecond)
	c.tokenBucket = rate.NewLimiter(limit, 1000)
}

func (c *QPSController) TakeToken() {
	if c == nil {
		return
	}
	c.tokenBucket.Wait(nil)
}
