package reliabilitylab

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// Cache demonstrates eventual consistency: transactional invalidation intents
// survive process failure; TTL bounds a late stale refill after the final delete.
type Cache struct {
	DB        *pgxpool.Pool
	Redis     *redis.Client
	TTL       time.Duration
	afterLoad func()
}

func (c *Cache) Read(ctx context.Context, key string) (string, error) {
	if value, err := c.Redis.Get(ctx, key).Result(); err == nil {
		return value, nil
	}
	var value string
	if err := c.DB.QueryRow(ctx, `SELECT value FROM cache_values WHERE key=$1`, key).Scan(&value); err != nil {
		return "", err
	}
	if c.afterLoad != nil {
		c.afterLoad()
	}
	// Cache failure does not discard a successful authoritative read.
	if c.TTL > 0 {
		_ = c.Redis.Set(ctx, key, value, c.TTL).Err()
	}
	return value, nil
}

func (c *Cache) Update(ctx context.Context, key, value string) error {
	_, err := c.DB.Exec(ctx, `WITH updated AS (
        INSERT INTO cache_values VALUES ($1,$2,1)
        ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value,version=cache_values.version+1
        RETURNING key,version)
        INSERT INTO cache_invalidations SELECT key,version FROM updated
        ON CONFLICT(key) DO UPDATE SET version=EXCLUDED.version`, key, value)
	return err
}

func (c *Cache) Invalidate(ctx context.Context, key string) error {
	var version int64
	if err := c.DB.QueryRow(ctx, `SELECT version FROM cache_invalidations WHERE key=$1`, key).Scan(&version); err != nil {
		return err
	}
	if err := c.Redis.Del(ctx, key).Err(); err != nil {
		return err
	}
	// Do not acknowledge a newer invalidation which arrived during DEL.
	_, err := c.DB.Exec(ctx, `DELETE FROM cache_invalidations WHERE key=$1 AND version=$2`, key, version)
	return err
}
