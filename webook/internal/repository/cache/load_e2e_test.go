package cache

import (
	"context"
	"testing"
	"time"

	"gitee.com/geekbang/basic-go/webook/pkg/logger"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func InitLog() logger.LoggerV1 {
	return logger.NewNoOpLogger()
}

func InitRedis() redis.Cmdable {
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	for err := redisClient.Ping(context.Background()).Err(); err != nil; {
		panic(err)
	}
	return redisClient
}

// go test -timeout 1m  -run TestLoadCache_e2e ./webook/internal/repository/cache/... -v
func TestLoadCache_e2e(t *testing.T) {
	client := InitRedis()
	l := InitLog()

	loadCache := NewRedisLoadCache(client, l)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15*4)
	defer cancel()

	biz := "your-biz"
	id := "your-id"

	id2 := "your-id2"

	// within 1st sample interval
	// get rank, before any data is set
	rank, err := loadCache.Rank(ctx, biz, id)
	assert.Error(t, err)
	assert.Equal(t, int64(-1), rank)

	// set data (load2 is the lowest)
	load := 1.23
	load2 := 1.1

	err = loadCache.Set(ctx, biz, id, load)
	assert.NoError(t, err)
	err = loadCache.Set(ctx, biz, id2, load2)
	assert.NoError(t, err)

	// within the 2nd sample interval
	time.Sleep(time.Second * 15)
	// set load (load is the lowest)
	load = 1.0
	load2 = 1.2
	err = loadCache.Set(ctx, biz, id, load)
	assert.NoError(t, err)
	err = loadCache.Set(ctx, biz, id2, load2)
	assert.NoError(t, err)

	// get rank -> will get the rank in the 1st sample interval
	rank, err = loadCache.Rank(ctx, biz, id)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), rank)

	rank2, err := loadCache.Rank(ctx, biz, id2)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), rank2)

	// within the 3rd sample interval --> get the rank in the 2nd sample interval
	time.Sleep(time.Second * 15)
	rank, err = loadCache.Rank(ctx, biz, id)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), rank)

	rank2, err = loadCache.Rank(ctx, biz, id2)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), rank2)
}
