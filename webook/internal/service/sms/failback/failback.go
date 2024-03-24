package failback

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"gitee.com/geekbang/basic-go/webook/internal/domain"
	"gitee.com/geekbang/basic-go/webook/internal/repository"
	"gitee.com/geekbang/basic-go/webook/internal/service/sms"
	faultDetect "gitee.com/geekbang/basic-go/webook/internal/service/sms/fault_detect"
	"gitee.com/geekbang/basic-go/webook/internal/service/sms/ratelimit"
)

var (
	ErrAutoRetryLatter = errors.New("errors occur, will retry latter")
)

type AsyncFailBackSmsService struct {
	smsSvc         sms.Service
	smsRequestRepo repository.SmsRequestRepository

	// retry config
	maxConcurrency           int64
	activatedGoroutineCount  int64
	retryConfig              domain.SmsRequestRetryConfig
	getRetryRecordsBatchSize int64
}

func NewAsyncFailBackSmsService(smsSvc sms.Service, maxConcurrency int64, config domain.SmsRequestRetryConfig, getRetryRecordsBatchSize int64) sms.Service {
	return &AsyncFailBackSmsService{
		smsSvc:                   smsSvc,
		maxConcurrency:           maxConcurrency,
		retryConfig:              config,
		getRetryRecordsBatchSize: getRetryRecordsBatchSize,
	}
}

func (s *AsyncFailBackSmsService) Send(ctx context.Context, tplId string, args []string, numbers ...string) error {
	err := s.smsSvc.Send(ctx, tplId, args, numbers...)

	switch err {
	case ratelimit.ErrLimited, faultDetect.ErrThirdPartyProviderCrash:
		// save record to db
		request := domain.SmsRequest{
			TplId:   tplId,
			Args:    args,
			Numbers: numbers,
		}
		s.smsRequestRepo.Create(ctx, request)

		// start retry background job if not reaching limit
		count := atomic.LoadInt64(&s.activatedGoroutineCount)
		if count < s.maxConcurrency {
			if atomic.CompareAndSwapInt64(&s.activatedGoroutineCount, count, count-1) {
				go func(ctx context.Context) {
					for {
						select {
						case <-ctx.Done():
							return
						default:
							s.retrySend(ctx)
						}
					}
				}(ctx)
			}
		}
		return ErrAutoRetryLatter
	}

	return err
}

func (s *AsyncFailBackSmsService) retrySend(ctx context.Context) error {
	time.Sleep(s.retryConfig.GetRecordsInterval) // TODO: use ctx.WithDeadline / ctx.WithTimeout instead?

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			requests, err := s.smsRequestRepo.FindRequestToRetry(ctx, s.retryConfig, int(s.getRetryRecordsBatchSize))
			if err != nil {
				fmt.Printf("failed to get request records from db to retry")
				break
			}

			if len(requests) == 0 {
				break
			}

			for _, request := range requests {
				s.retrySendOneRequest(ctx, request)
			}
		}
	}
}

func (s *AsyncFailBackSmsService) retrySendOneRequest(ctx context.Context, request domain.SmsRequest) error {
	fmt.Printf("start processing request, id: %d", request.Id)
	if err := s.sendWithRequestRecord(ctx, request); err != nil {
		fmt.Printf("failed to resend request as processing, skip to next one. id: %d, error: %v", request.Id, err)
		if err := s.smsRequestRepo.MarkAsRetryFailed(ctx, request.Id); err != nil {
			fmt.Printf("failed to mark failed retry, skip to next one. id: %d, error: %v", request.Id, err)
			return err
		}
		return err
	}

	if err := s.smsRequestRepo.MarkAsRetrySucceeded(ctx, request.Id); err != nil {
		fmt.Printf("failed to mark successful retry, skip to next one, id: %d, error: %v", request.Id, err)
		return err
	}

	return nil
}

func (s *AsyncFailBackSmsService) sendWithRequestRecord(ctx context.Context, request domain.SmsRequest) error {
	err := s.smsSvc.Send(ctx, request.TplId, request.Args, request.Numbers...)

	switch err {
	case ratelimit.ErrLimited, faultDetect.ErrThirdPartyProviderCrash:

		return ErrAutoRetryLatter
	}
	return err
}
