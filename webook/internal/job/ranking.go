package job

import (
	"context"
	"sync"
	"time"

	"gitee.com/geekbang/basic-go/webook/internal/service"
	"gitee.com/geekbang/basic-go/webook/pkg/logger"
	rlock "github.com/gotomicro/redis-lock"
)

type RankingJob struct {
	svc service.RankingService
	// 一次运行的超时时间
	timeout    time.Duration
	lockClient *rlock.Client
	l          logger.LoggerV1
	key        string

	// V1 使用
	// 本地锁，因为要在多个 goroutine 之间操作 lock，所以需要保护起来
	// 也可以用原子操作。但是作为一个定时指定的任务，不在意这么一点性能
	localLock sync.Mutex
	lock      *rlock.Lock

	loadBiz    string
	loadSvc    service.LoadService
	instanceId string
}

func NewRankingJob(
	svc service.RankingService,
	lockClient *rlock.Client,
	loadSvc service.LoadService,
	instanceId string,
	l logger.LoggerV1,
	timeout time.Duration) *RankingJob {
	return &RankingJob{
		svc:        svc,
		lockClient: lockClient,
		timeout:    timeout,
		key:        "job:ranking",
		l:          l,
		instanceId: instanceId,
		loadBiz:    "cronjob",
		loadSvc:    loadSvc,
	}
}

func (r *RankingJob) Name() string {
	return "ranking"
}

func (r *RankingJob) Run() error {
	return r.RunV1()
}

// RunV1 持有锁之后，就一直不放，除非关机，或者突然宕机
func (r *RankingJob) RunV1() error {
	r.localLock.Lock()
	lock := r.lock

	if lock == nil {
		if !r.hasLowestLoadOrHasNoLoadData() {
			return nil
		}

		// 试着拿锁
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()
		var err error
		lock, err = r.lockClient.Lock(ctx, r.key, r.timeout,
			// 每隔 100ms 重试一次，每次重试的超时时间是 1s
			&rlock.FixIntervalRetry{
				Interval: time.Millisecond * 100,
				Max:      3,
			}, time.Second)
		if err != nil {
			r.localLock.Unlock()
			// 这边不需要返回 error，因为这时候可能是别的节点一直占着锁
			return nil
		}
		r.lock = lock
		r.localLock.Unlock()
		// 自动续约，也就是延长分布式锁的过期时间
		// r.timeout 的一半作为刷新间隔。你这边可以设置为几秒钟，因为访问 Redis 是很快的
		// 每次续约 r.timeout 的时间（也就是分布式锁的过期时间重置为 r.timeout
		go func() {
			if !r.hasLowestLoadOrHasNoLoadData() {
				return
			}
			err = lock.AutoRefresh(r.timeout/2, r.timeout)
			// 续约失败
			// 有几种可能，自己和 Redis 失去了连接
			if err != nil {
				// 这边最多等一段时间就能拿到 localLock
				r.localLock.Lock()
				r.lock = nil
				r.localLock.Unlock()
			}
		}()
	}
	return r.run()
}

func (r *RankingJob) Close() error {
	// v1 用
	r.localLock.Lock()
	lock := r.lock
	r.localLock.Unlock()
	// 释放锁
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	// 释放锁失败，但是也不需要作什么，因为这个分布式锁会在过期时间之后自动释放
	// unlock 的时候就会触发退出 AutoRefresh
	// 这个时候是否把 r.lock 置为 nil 都可以了
	return lock.Unlock(ctx)
}

func (r *RankingJob) RunV0() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	// 加锁本身，我们使用一个ctx
	// 本身我们这里设计的就是要在 r.timeout 内计算完成
	// 刚好也做成分布式锁的超时时间
	lock, err := r.lockClient.Lock(ctx, r.key, r.timeout,
		// 每隔 100ms 重试一次，每次重试的超时时间是 1s
		&rlock.FixIntervalRetry{
			Interval: time.Millisecond * 100,
			Max:      3,
		}, time.Second)
	// 我们这里不需要处理 error，因为大部分情况下，可以相信别的节点会继续拿锁
	if err != nil {
		return err
	}

	defer func() {
		// 释放锁，再来一个独立的 ctx
		ctx, cancel = context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()
		// 释放锁失败，但是也不需要作什么，因为这个分布式锁会在过期时间之后自动释放
		err = lock.Unlock(ctx)
		if err != nil {
			r.l.Error("释放分布式锁失败",
				logger.Error(err),
				logger.String("name", r.Name()))
		}
	}()
	return r.run()
}

func (r *RankingJob) run() error {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()
	return r.svc.RankTopN(ctx)
}

func (r *RankingJob) hasLowestLoadOrHasNoLoadData() bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	if isLowest, err := r.loadSvc.IsLowestLoad(ctx, r.loadBiz, r.instanceId); err == service.ErrLoadDataNotFound {
		r.l.Info("没有负载数据，可以搶鎖")
		return true
	} else if err != nil {
		r.l.Error("检查负载失败", logger.Error(err))
		return false
	} else if !isLowest {
		r.l.Info("不是最低负载节点，跳过")
		return false
	}
	return true
}

var _ Job = (*RankingJob)(nil)
