package ioc

import (
	"context"
	"log"

	cache "gitee.com/geekbang/basic-go/webook/internal/repository/local_cache"
	"github.com/allegro/bigcache/v3"
)

func InitLocalCache() cache.LocalCache {
	config := bigcache.Config{
		Shards:      1024,
		LifeWindow:  cache.Expiration,
		CleanWindow: cache.Expiration / 2,
	}
	cache, err := bigcache.New(context.Background(), config)

	if err != nil {
		log.Fatal(err)
	}

	return cache
}
