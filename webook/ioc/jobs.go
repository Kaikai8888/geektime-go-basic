package ioc

import (
	"time"

	"gitee.com/geekbang/basic-go/webook/internal/job"
	"gitee.com/geekbang/basic-go/webook/internal/service"
	"gitee.com/geekbang/basic-go/webook/pkg/logger"
	"gitee.com/geekbang/basic-go/webook/pkg/util"
	rlock "github.com/gotomicro/redis-lock"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
)

func InitRankingJobAndReportLoadJob(svc service.RankingService, loadSvc service.LoadService,
	client *rlock.Client,
	redis redis.Cmdable,
	uuidFn util.UuidFn,
	l logger.LoggerV1) (*job.RankingJob, *job.ReportLoadJob) {
	instanceId, err := uuidFn()
	if err != nil {
		l.Error("Failed to generate instance id", logger.Error(err))
		panic("Failed to generate instance id")
	}
	return job.NewRankingJob(svc, client, loadSvc, instanceId, l, time.Second*30),
		job.NewReportLoadJob(loadSvc, redis, l, instanceId, time.Second*30)
}

func InitJobs(l logger.LoggerV1, rankingJob *job.RankingJob) *cron.Cron {
	bd := job.NewCronJobBuilder(l, prometheus.SummaryOpts{
		Namespace: "geekbang_daming",
		Subsystem: "webook",
		Name:      "cron_job",
		Help:      "定时任务",
	})
	expr := cron.New(cron.WithSeconds())
	_, err := expr.AddJob("@every 1m",
		bd.Build(rankingJob))
	if err != nil {
		panic(err)
	}
	return expr
}
