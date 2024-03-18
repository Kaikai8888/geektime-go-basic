package fault_detect

import (
	"context"
	"errors"
	"time"

	"gitee.com/geekbang/basic-go/webook/internal/service/sms"
	list "github.com/ecodeclub/ekit/list"
)

var (
	ErrThirdPartyProviderCrash = errors.New("third party provider crash down")
	ErrUnexpectedType          = errors.New("unexpected data type")
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

	startAt          time.Time
	records          list.List[record]
	isExpireJobStart bool
}

type record struct {
	time      time.Time
	matched   bool
	expiredAt time.Time
}

func (s *record) IsExpired() bool {
	return s.expiredAt.Before(time.Now())
}

func NewSlidingWindowRateCounter(window int, countInterval int, unit time.Duration) RateCounter {
	if window < countInterval {
		panic("window should be larger then count interval")
	}

	if window%countInterval != 0 {
		panic("window is not divisible by count interval")
	}

	now := time.Now()
	rateCounter := &SlidingWindowRateCounter{
		window:        window,
		countInterval: countInterval,
		startAt:       now,
		unit:          unit,
		records: &list.ConcurrentList[record]{
			List: list.NewLinkedList[record](),
		},
	}

	return rateCounter
}

func (c *SlidingWindowRateCounter) Add(matched bool) float64 {
	if !c.isExpireJobStart {
		c.startExpireJob()
	}

	now := time.Now()
	r := record{time: now, matched: matched, expiredAt: now.Add(c.getWindow())}
	c.records.Append(r)

	// TODO: atomic
	if matched {
		c.accumulatedMatchedCount++
	}
	c.accumulatedCount++

	return float64(c.accumulatedMatchedCount) / float64(c.accumulatedCount) // TODO: 浮點數問題？四捨五入到小數點下第二位？
}

func (c *SlidingWindowRateCounter) startExpireJob() {
	interval := time.Duration(c.countInterval) * c.unit

	expireJob := func() error {
		time.Sleep(interval)

		for {
			r, err := c.records.Get(0)
			if err != nil {
				return err
			}
			if !r.IsExpired() {
				break
			}

			if _, err := c.records.Delete(0); err != nil {
				// 即使删到的资料跟前面get的不同, 也只会有一点点误差
				return err
			}

			// TODO: atomic
			if r.matched {
				c.accumulatedMatchedCount--
			}
			c.accumulatedCount--
		}
		return nil
	}

	go func() {
		// TODO: gracefully shutdown?
		for {
			expireJob()
		}
	}()

}

func (c *SlidingWindowRateCounter) getWindow() time.Duration {
	return time.Duration(c.window) * c.unit
}
