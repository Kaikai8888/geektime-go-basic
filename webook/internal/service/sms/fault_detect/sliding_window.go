package fault_detect

import (
	"context"
	"errors"
	"time"

	"gitee.com/geekbang/basic-go/webook/internal/service/sms"
)

var (
	ErrThirdPartyProviderCrash = errors.New("third party provider crash down")
)

type FaultDetectBySlidingWindowErrorRateSMSService struct {
	smsSvc        sms.Service
	rateCounter   RateCounter
	threshold     float64
	errorDetector func(error) bool
}

func NewFaultDetectBySlidingWindowErrorRateSMSService(smsSvc sms.Service, rateCounter RateCounter, threshold float64, errDetector func(error) bool) sms.Service {
	return &FaultDetectBySlidingWindowErrorRateSMSService{
		smsSvc:        smsSvc,
		rateCounter:   rateCounter,
		threshold:     threshold,
		errorDetector: errDetector,
	}
}

func (s *FaultDetectBySlidingWindowErrorRateSMSService) Send(ctx context.Context, tplId string, args []string, numbers ...string) error {
	if err := s.smsSvc.Send(ctx, tplId, args, numbers...); err != nil {
		if s.errorDetector(err) {
			if rate := s.rateCounter.Add(true); rate > s.threshold {
				return ErrThirdPartyProviderCrash
			}
		}
		return err
	}
	return nil
}

type RateCounter interface {
	Add(matched bool) float64 // return rate
}

type SlidingWindowRateCounter struct {
	window        int
	countInterval int
	unit          time.Duration

	accumulatedMatchedCount int
	accumulatedCount        int // rate = accumulatedMatchedCount / accumulatedCount

	startAt    time.Time
	statistics map[time.Time]statistic // key: interval start, value: count
}

type statistic struct {
	matchedCount int
	totalCount   int
}

func NewSlidingWindowRateCounter(window int, countInterval int, unit time.Duration) RateCounter {
	if window < countInterval {
		panic("window should be larger then count interval")
	}

	if window%countInterval != 0 {
		panic("window is not divisible by count interval")
	}

	now := time.Now()
	return &SlidingWindowRateCounter{
		window:        window,
		countInterval: countInterval,
		startAt:       now,
		unit:          unit,
	}
}

func (c *SlidingWindowRateCounter) Add(matched bool) float64 {
	// TODO: atomic operation
	now := time.Now()
	key := c.intervalStartAt(now)
	intervalStatistic, ok := c.statistics[key]
	if !ok {
		intervalStatistic = statistic{}
	}

	if matched {
		c.accumulatedMatchedCount++
		intervalStatistic.matchedCount++
	}
	c.accumulatedCount++
	intervalStatistic.totalCount++

	c.statistics[key] = intervalStatistic

	return float64(c.accumulatedMatchedCount) / float64(c.accumulatedCount) // TODO: 浮點數問題？四捨五入到小數點下第二位？
}

func (c *SlidingWindowRateCounter) intervalStartAt(now time.Time) time.Time {
	windowStartAt := c.windowStartAt(now)
	durationSinceWindowStart := now.Sub(c.windowStartAt(now))
	interval := c.getCountInterval()
	intervalCount := durationSinceWindowStart.Truncate(interval)
	durationBtwWindowStartAndIntervalStart := time.Duration(intervalCount) * interval

	return windowStartAt.Add(durationBtwWindowStartAndIntervalStart)
}

func (c *SlidingWindowRateCounter) windowStartAt(now time.Time) time.Time {
	window := time.Duration(-1) * c.getWindow()
	return now.Add(window)
}

func (c *SlidingWindowRateCounter) getCountInterval() time.Duration {
	return time.Duration(c.countInterval) * c.unit
}

func (c *SlidingWindowRateCounter) getWindow() time.Duration {
	return time.Duration(c.window) * c.unit
}
