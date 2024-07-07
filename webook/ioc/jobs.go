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

type rankingJobSet struct {
	rankingJob    *job.RankingJob
	reportLoadJob *job.ReportLoadJob
}

func InitRankingJobAndReportLoadJob(svc service.RankingService, loadSvc service.LoadService,
	client *rlock.Client,
	redis redis.Cmdable,
	uuidFn util.UuidFn,
	l logger.LoggerV1) *rankingJobSet {
	instanceId, err := uuidFn()
	if err != nil {
		l.Error("Failed to generate instance id", logger.Error(err))
		panic("Failed to generate instance id")
	}
	return &rankingJobSet{
		rankingJob:    job.NewRankingJob(svc, client, loadSvc, instanceId, l, time.Second*30),
		reportLoadJob: job.NewReportLoadJob(loadSvc, redis, l, instanceId, time.Second*30),
	}
}

func InitJobs(l logger.LoggerV1, rankingJobSet *rankingJobSet) *cron.Cron {
	bd := job.NewCronJobBuilder(l, prometheus.SummaryOpts{
		Namespace: "geekbang_daming",
		Subsystem: "webook",
		Name:      "cron_job",
		Help:      "定时任务",
	})
	expr := cron.New(cron.WithSeconds())
	_, err := expr.AddJob("@every 1m",
		bd.Build(rankingJobSet.rankingJob))
	if err != nil {
		panic(err)
	}

	_, err = expr.AddJob("@every 15s",
		bd.Build(rankingJobSet.reportLoadJob))
	if err != nil {
		panic(err)
	}
	return expr
}
