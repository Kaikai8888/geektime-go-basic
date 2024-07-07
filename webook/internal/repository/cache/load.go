package cache

import (
	"context"
	"fmt"
	"time"

	"gitee.com/geekbang/basic-go/webook/pkg/logger"
	"github.com/redis/go-redis/v9"
)

var (
	ErrLoadDataNotFound = fmt.Errorf("load data not found")
)

type LoadCache interface {
	Set(ctx context.Context, biz string, id string, load float64) error
	Rank(ctx context.Context, biz string, id string) (int64, error) // in descending order
}

type loadRedisCache struct {
	client         redis.Cmdable
	keyPrefix      string
	sampleInterval time.Duration
	l              logger.LoggerV1
}

func NewRedisLoadCache(client redis.Cmdable, l logger.LoggerV1) LoadCache {
	return &loadRedisCache{
		client:         client,
		keyPrefix:      "load",
		sampleInterval: time.Second * 15, // 与report load cronjob 的执行区间相同
		l:              l,
	}
}

func (c *loadRedisCache) Set(ctx context.Context, biz, id string, load float64) error {
	now := time.Now()
	startOfSampleInterval := now.Truncate(c.sampleInterval)
	key := c.getKey(biz, startOfSampleInterval)
	c.l.Debug("key", logger.String("key", key), logger.String("sampleInterval", c.sampleInterval.String()), logger.Int64("now", now.UnixNano()), logger.Int64("startOfSampleInterval", now.UnixNano()))

	setMember := redis.Z{Score: load, Member: id}
	if _, err := c.client.ZAdd(ctx, key, setMember).Result(); err != nil {
		return err
	}
	return nil
}

func (c *loadRedisCache) Rank(ctx context.Context, biz, id string) (int64, error) {
	now := time.Now()
	startOfSampleInterval := now.Truncate(c.sampleInterval).Add(-1 * c.sampleInterval) // 查询上一个完整sample interval的排名
	key := c.getKey(biz, startOfSampleInterval)
	c.l.Debug("key", logger.String("key", key), logger.String("sampleInterval", c.sampleInterval.String()), logger.Int64("now", now.UnixNano()), logger.Int64("startOfSampleInterval", now.UnixNano()))

	cmd := c.client.ZRank(ctx, key, id)

	if cmd.Err() == redis.Nil {
		return -1, ErrLoadDataNotFound
	}

	return cmd.Result()
}

func (c *loadRedisCache) getKey(biz string, startOfSampleInterval time.Time) string {
	return fmt.Sprintf("%s:%s:%d", c.keyPrefix, biz, startOfSampleInterval.UnixNano())
}
