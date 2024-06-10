package cache

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type LoadCache interface {
	Set(ctx context.Context, biz string, id string, load float64) error
	Rank(ctx context.Context, biz string, id string) (int64, error) // in descending order
}

type loadRedisCache struct {
	client    redis.Cmdable
	keyPrefix string
}

func NewRedisLoadCache(client redis.Cmdable) LoadCache {
	return &loadRedisCache{
		client:    client,
		keyPrefix: "load",
	}
}

func (c *loadRedisCache) Set(ctx context.Context, biz, id string, load float64) error {
	key := c.getKey(biz)
	setMember := redis.Z{Score: load, Member: id}
	if _, err := c.client.ZAdd(ctx, key, setMember).Result(); err != nil {
		return err
	}
	return nil
}

func (c *loadRedisCache) Rank(ctx context.Context, biz, id string) (int64, error) {
	key := c.getKey(biz)
	return c.client.ZRank(ctx, key, id).Result()
}

func (c *loadRedisCache) getKey(biz string) string {
	return c.keyPrefix + ":" + biz
}
