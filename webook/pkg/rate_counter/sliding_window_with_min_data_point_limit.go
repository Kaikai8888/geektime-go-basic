package rate_counter

import (
	"context"
	"time"
)

type SlidingWindowRateCounterWithMinDataPointLimit struct {
	counter           RateCounterWithAccessToAccumulatedCount
	minDataPointCount int // 至少要收集到一定数量的资料, 才能计算rate, 不然rate = 0
}

func NewSlidingWindowRateCounterWithMinDataPointLimit(counter RateCounterWithAccessToAccumulatedCount, window int, countInterval int, unit time.Duration, minDataPointCount int) RateCounter {
	return &SlidingWindowRateCounterWithMinDataPointLimit{
		counter:           counter,
		minDataPointCount: minDataPointCount,
	}
}

func (c *SlidingWindowRateCounterWithMinDataPointLimit) StartExpireJob(ctx context.Context) error {
	return c.counter.StartExpireJob(ctx)
}

func (c *SlidingWindowRateCounterWithMinDataPointLimit) Add(matched bool) (float64, error) {
	rate, err := c.counter.Add(matched)
	if err != nil {
		return 0, err
	}

	if c.counter.AccumulatedCount() < c.minDataPointCount {
		rate = 0
	}

	return rate, nil
}
