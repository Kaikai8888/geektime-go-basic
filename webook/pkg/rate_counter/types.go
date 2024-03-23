package rate_counter

import "context"

type RateCounter interface {
	StartExpireJob(ctx context.Context) error
	Add(matched bool) (float64, error) // return rate
}

type RateCounterWithAccessToAccumulatedCount interface {
	RateCounter
	AccumulatedCount() int
}
