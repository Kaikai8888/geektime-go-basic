package fault_detect

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func batchAdd(counter RateCounter, matched bool, count int) (float64, error) {
	if count <= 0 {
		panic("count should > 0")
	}

	var finalRate float64

	for i := 0; i < count; i++ {
		rate, err := counter.Add(matched)
		fmt.Printf("--- Added one: matched: %t, rate: %f, counter: %+v\n", matched, rate, counter)

		if err != nil {
			return 0, err
		}
		finalRate = rate
	}

	return finalRate, nil
}

func TestRateCounter(t *testing.T) {

	const (
		unit = time.Second
	)

	testCases := []struct {
		name          string
		window        int
		countInterval int
		testDuration  int
		test          func(ctx context.Context, t *testing.T, counter *SlidingWindowRateCounter)
		willPanic     bool
	}{
		{
			name:          "success case: 每秒发生频率相同",
			window:        3,
			countInterval: 1,
			testDuration:  5,
			test: func(ctx context.Context, t *testing.T, counter *SlidingWindowRateCounter) {
				err := counter.StartExpireJob(ctx)
				assert.NoError(t, err)

				for i := 0; i < 4; i++ {

					rate, err := batchAdd(counter, true, 1)
					assert.NoError(t, err)

					if i == 3 {
						// i = 0 的资料已过期
						assert.Less(t, counter.accumulatedCount, 3*5, "records should be expired")
						assert.Equal(t, 3, counter.accumulatedMatchedCount, "records should be expired")
						assert.Equal(t, (2.0+1.0)/(10.0+1.0), rate)
					} else {
						assert.Equal(t, float64(i+1)/float64(i*5+1), rate)
					}

					rate, err = batchAdd(counter, false, 4)
					assert.NoError(t, err)
					assert.Equal(t, 1.0/5.0, rate)

					time.Sleep(unit)
				}

			},
			willPanic: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rateCounter := NewSlidingWindowRateCounter(tc.window, tc.countInterval, unit)
			counter, ok := (rateCounter).(*SlidingWindowRateCounter)
			if !ok {
				assert.FailNow(t, "rate counter is not expected type")
			}
			ctx, cancel := context.WithCancel(context.Background())
			tc.test(ctx, t, counter)
			time.Sleep(time.Duration(tc.testDuration) * unit)
			cancel()
		})
	}

}
