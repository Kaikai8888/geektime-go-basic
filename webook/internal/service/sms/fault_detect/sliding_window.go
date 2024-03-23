package fault_detect

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"gitee.com/geekbang/basic-go/webook/internal/service/sms"
	list "github.com/ecodeclub/ekit/list"
)

var (
	ErrThirdPartyProviderCrash = errors.New("third party provider crash down")
	ErrUnexpectedType          = errors.New("unexpected data type")
	ErrExpireJobNotStarted     = errors.New("expire job not started yet") // should call StartExpireJob before calculating rate
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
			if rate, err := s.rateCounter.Add(true); err != nil {
				return err
			} else if rate > s.threshold {
				return ErrThirdPartyProviderCrash
			}
		}
		return err
	}
	return nil
}

type RateCounter interface {
	StartExpireJob(ctx context.Context) error
	Add(matched bool) (float64, error) // return rate
}

type SlidingWindowRateCounterWithMinDataPointLimit struct {
	counter           SlidingWindowRateCounter
	minDataPointCount int // 至少要收集到一定数量的资料, 才能计算rate, 不然rate = 0
}

func NewSlidingWindowRateCounterWithMinDataPointLimit(window int, countInterval int, unit time.Duration, minDataPointCount int) RateCounter {
	counter := NewSlidingWindowRateCounter(window, countInterval, unit)

	rateCounter, ok := counter.(*SlidingWindowRateCounter)
	if !ok {
		panic("unexpected type")
	}
	return &SlidingWindowRateCounterWithMinDataPointLimit{
		counter:           *rateCounter,
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

	if c.counter.accumulatedCount < c.minDataPointCount {
		rate = 0
	}

	return rate, nil
}

type SlidingWindowRateCounter struct {
	window        int
	countInterval int
	unit          time.Duration

	accumulatedMatchedCount int
	accumulatedCount        int // rate = accumulatedMatchedCount / accumulatedCount
	lock                    sync.RWMutex

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

func (c *SlidingWindowRateCounter) Add(matched bool) (float64, error) {
	if !c.isExpireJobStart {
		return 0, ErrExpireJobNotStarted
	}

	c.expireJob() // 确保过期资料有删除, counter 有更新

	c.lock.Lock()
	defer c.lock.Unlock()

	now := time.Now()
	r := record{time: now, matched: matched, expiredAt: now.Add(c.getWindow())}
	c.records.Append(r)

	if matched {
		c.accumulatedMatchedCount++
	}
	c.accumulatedCount++

	return float64(c.accumulatedMatchedCount) / float64(c.accumulatedCount), nil
}

func (c *SlidingWindowRateCounter) StartExpireJob(ctx context.Context) error {
	if c.isExpireJobStart {
		return nil
	}

	interval := time.Duration(c.countInterval) * c.unit

	// subCtx, cancel := context.WithCancel(ctx)
	// 定时更新, 避免资料持续累积
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			default: // non blocking
				fmt.Printf("Wait till next expire job, interval: %d (%d seconds)....\n", c.countInterval, int(c.unit.Seconds()))
				time.Sleep(interval)
				c.expireJob()
			}
		}
	}(ctx)

	// select {
	// case <-ctx.Done(): // without default, blocking
	// 	cancel()
	// 	return nil
	// }
	c.isExpireJobStart = true
	return nil
}

func (c *SlidingWindowRateCounter) expireJob() {
	// lock the whole counter until current expire job end
	c.lock.Lock()
	fmt.Printf("Start checking if expired records exist\n")
	defer c.lock.Unlock()
	// delete all expired records
	for c.records.Len() > 0 {

		r, err := c.records.Get(0)
		fmt.Printf("Get one record: %+v\n", r)

		if err != nil {
			fmt.Printf("error occurs when getting records: %v\n", err)
			break
		}
		if !r.IsExpired() {
			fmt.Printf("the earliest record is not expired\n")
			break
		}

		if _, err := c.records.Delete(0); err != nil {
			// 即使删到的资料跟前面get的不同, 也只会有一点点误差
			fmt.Printf("error occurs when deleting records: %v\n", err)
			break
		}

		if r.matched {
			c.accumulatedMatchedCount--
		}
		c.accumulatedCount--
		fmt.Printf("Delete one expired record: %+v\ncurrent counter: %+v\n", r, c)
	}
}

func (c *SlidingWindowRateCounter) getWindow() time.Duration {
	return time.Duration(c.window) * c.unit
}
