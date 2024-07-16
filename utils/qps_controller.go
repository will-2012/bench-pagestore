package utils

import (
	"context"

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
	c.tokenBucket = rate.NewLimiter(rate.Limit(qps), 100)
}

func (c *QPSController) TakeToken() {
	if c == nil {
		return
	}
	c.tokenBucket.Wait(context.Background())
}
