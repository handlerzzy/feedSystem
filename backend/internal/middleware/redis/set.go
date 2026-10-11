package redis

import (
	"context"
	"errors"
	"time"
)

// SAdd 将一个或多个成员添加到 Set 集合中。
func (c *Client) SAdd(ctx context.Context, key string, members ...any) error {
	if c == nil || c.rdb == nil || len(members) == 0 {
		return nil
	}
	return c.rdb.SAdd(ctx, key, members...).Err()
}

// SMembers 获取 Set 集合中的所有成员。
func (c *Client) SMembers(ctx context.Context, key string) ([]string, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("redis client not initialized")
	}
	return c.rdb.SMembers(ctx, key).Result()
}

// SIsMember 判断 member 是否是 Set 集合的成员。
func (c *Client) SIsMember(ctx context.Context, key string, member any) (bool, error) {
	if c == nil || c.rdb == nil {
		return false, errors.New("redis client not initialized")
	}
	return c.rdb.SIsMember(ctx, key, member).Result()
}

// SMIsMember 批量判断多个 member 是否是 Set 集合的成员。
func (c *Client) SMIsMember(ctx context.Context, key string, members ...any) ([]bool, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("redis client not initialized")
	}
	return c.rdb.SMIsMember(ctx, key, members...).Result()
}

// SAddWithExpire 将一个或多个成员添加到 Set 集合中，并通过 Pipeline 设置滑动过期时间。
func (c *Client) SAddWithExpire(ctx context.Context, key string, ttl time.Duration, members ...any) error {
	if c == nil || c.rdb == nil || len(members) == 0 {
		return nil
	}
	pipe := c.rdb.Pipeline()
	pipe.SAdd(ctx, key, members...)
	if ttl > 0 {
		pipe.Expire(ctx, key, ttl)
	}
	_, err := pipe.Exec(ctx)
	return err
}
