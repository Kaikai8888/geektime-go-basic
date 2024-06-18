package integration

import (
	"testing"
	"time"

	"gitee.com/geekbang/basic-go/webook/internal/integration/startup"
	"gitee.com/geekbang/basic-go/webook/internal/job"
	"gitee.com/geekbang/basic-go/webook/internal/repository/cache"
	svcmocks "gitee.com/geekbang/basic-go/webook/internal/service/mocks"
	"gitee.com/geekbang/basic-go/webook/pkg/logger"
	rlock "github.com/gotomicro/redis-lock"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	repostiory "gitee.com/geekbang/basic-go/webook/internal/repository"
	"gitee.com/geekbang/basic-go/webook/internal/service"
)

// TestRankingJob 这个测试只是测试调度和 Redis 交互两个部分，但是不会真的测试计算的逻辑
func TestRankingJob(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	rdb := startup.InitRedis()
	svc := svcmocks.NewMockRankingService(ctrl)
	l := logger.NewNoOpLogger()
	loadCache := cache.NewRedisLoadCache(rdb, l)
	loadRepo := repostiory.NewLoadRepository(loadCache)
	loadSvc := service.NewLoadService(loadRepo, l)
	// 会调用三次
	svc.EXPECT().RankTopN(gomock.Any()).Times(3).Return(nil)
	j := job.NewRankingJob(svc, rlock.NewClient(rdb), loadSvc, "mock-instance-id",
		logger.NewNoOpLogger(), time.Minute)
	c := cron.New(cron.WithSeconds())
	bd := job.NewCronJobBuilder(logger.NewNoOpLogger(),
		prometheus.SummaryOpts{
			Name: "test",
		})
	_, err := c.AddJob("@every 1s", bd.Build(j))
	require.NoError(t, err)
	c.Start()
	time.Sleep(time.Second * 3)
	ctx := c.Stop()
	<-ctx.Done()
}
