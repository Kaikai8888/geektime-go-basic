package job

import (
	"context"
	"time"

	"gitee.com/geekbang/basic-go/webook/internal/service"
	"gitee.com/geekbang/basic-go/webook/pkg/logger"
	"github.com/redis/go-redis/v9"
)

type ReportLoadJob struct {
	svc   service.LoadService
	redis redis.Cmdable
	l     logger.LoggerV1

	biz        string
	instanceId string
	timeout    time.Duration
}

func NewReportLoadJob(svc service.LoadService, redis redis.Cmdable, l logger.LoggerV1, instanceId string, timeout time.Duration) *ReportLoadJob {
	return &ReportLoadJob{
		svc:        svc,
		redis:      redis,
		l:          l,
		biz:        "cronjob",
		instanceId: instanceId,
		timeout:    timeout,
	}
}

func (j *ReportLoadJob) Name() string {
	return "report-load"
}

func (j *ReportLoadJob) Run() error {
	ctx, cancel := context.WithTimeout(context.Background(), j.timeout)
	defer cancel()
	if err := j.svc.ReportLoad(ctx, j.biz, j.instanceId); err != nil {
		j.l.Error("Failed to report load to redis")
		return err
	}
	return nil
}
