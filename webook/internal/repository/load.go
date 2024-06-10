package repository

import (
	"context"

	"gitee.com/geekbang/basic-go/webook/internal/repository/cache"
)

type LoadRepository interface {
	Record(ctx context.Context, biz, id string, load float64) error
	IsLowest(ctx context.Context, biz, id string) (bool, error)
}

type loadRepository struct {
	cache cache.LoadCache
}

func NewLoadRepository(cache cache.LoadCache) LoadRepository {
	return &loadRepository{
		cache: cache,
	}
}

func (r *loadRepository) Record(ctx context.Context, biz, id string, load float64) error {
	return r.cache.Set(ctx, biz, id, load)
}
func (r *loadRepository) IsLowest(ctx context.Context, biz, id string) (bool, error) {
	rank, err := r.cache.Rank(ctx, biz, id)
	if err != nil {
		return false, err
	}
	return rank == 0, nil
}
