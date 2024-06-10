package service

import (
	"context"
	"errors"
	"time"

	"gitee.com/geekbang/basic-go/webook/internal/repository"
	"gitee.com/geekbang/basic-go/webook/pkg/logger"
	"github.com/shirou/gopsutil/cpu"
)

type LoadService interface {
	ReportLoad(ctx context.Context, biz, instanceId string) error
	IsLowestLoad(ctx context.Context, biz, instanceId string) (bool, error)
}

type loadService struct {
	repo repository.LoadRepository
	l    logger.LoggerV1

	sampleInterval time.Duration
}

func NewLoadService(repo repository.LoadRepository, l logger.LoggerV1) LoadService {
	return &loadService{
		repo:           repo,
		l:              l,
		sampleInterval: time.Second,
	}
}

func (s *loadService) ReportLoad(ctx context.Context, biz, instanceId string) error {
	cpu, err := s.getCpuUsage()
	if err != nil {
		return err
	}
	return s.repo.Record(ctx, biz, instanceId, cpu)
}

func (s *loadService) getCpuUsage() (float64, error) {
	usages, err := cpu.Percent(s.sampleInterval, false)
	if err != nil {
		return 0, err
	}
	if len(usages) != 1 {
		return 0, errors.New("Unexpected length of cpu usage data")
	}
	return usages[0], nil
}

func (s *loadService) IsLowestLoad(ctx context.Context, biz, instanceId string) (bool, error) {
	return s.repo.IsLowest(ctx, biz, instanceId)
}
