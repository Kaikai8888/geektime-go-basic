package fault_detect

import (
	"context"
	"errors"

	"gitee.com/geekbang/basic-go/webook/internal/service/sms"
	rateCounter "gitee.com/geekbang/basic-go/webook/pkg/rate_counter"
)

var (
	ErrThirdPartyProviderCrash = errors.New("third party provider crash down")
)

type FaultDetectBySlidingWindowErrorRateSMSService struct {
	smsSvc        sms.Service
	rateCounter   rateCounter.RateCounter
	threshold     float64
	errorDetector func(error) bool
}

func NewFaultDetectBySlidingWindowErrorRateSMSService(smsSvc sms.Service, rateCounter rateCounter.RateCounter, threshold float64, errDetector func(error) bool) sms.Service {
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
