package redis

import (
	"context"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

func TestIncrementWithExpireSetsTTLWithoutExtendingWindow(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	client := &Client{
		rdb: goredis.NewClient(&goredis.Options{Addr: mr.Addr()}),
	}
	defer client.Close()

	ctx := context.Background()
	key := "feedflow:ratelimit:test"
	expire := 30 * time.Second

	count, err := client.IncrementWithExpire(ctx, key, expire)
	if err != nil {
		t.Fatalf("first increment: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected count 1, got %d", count)
	}

	firstTTL := mr.TTL(key)
	if firstTTL <= 0 || firstTTL > expire {
		t.Fatalf("expected ttl in (0, %s], got %s", expire, firstTTL)
	}

	mr.FastForward(5 * time.Second)
	ttlBeforeSecond := mr.TTL(key)

	count, err = client.IncrementWithExpire(ctx, key, expire)
	if err != nil {
		t.Fatalf("second increment: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected count 2, got %d", count)
	}

	ttlAfterSecond := mr.TTL(key)
	if ttlAfterSecond != ttlBeforeSecond {
		t.Fatalf("expected ttl to stay at %s, got %s", ttlBeforeSecond, ttlAfterSecond)
	}
}

func TestClient_ZScoreZRemGetInt64(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	client := NewClient(goredis.NewClient(&goredis.Options{Addr: mr.Addr()}), "test:")
	defer client.Close()

	ctx := context.Background()

	// 1. GetInt64 未命中返回 redis.Nil
	_, err = client.GetInt64(ctx, client.Key("counter"))
	if !IsMiss(err) {
		t.Fatalf("expected miss for non-existent key, got %v", err)
	}

	// 设置并读取
	_ = client.RDB().Set(ctx, client.Key("counter"), 123, 0).Err()
	val, err := client.GetInt64(ctx, client.Key("counter"))
	if err != nil || val != 123 {
		t.Fatalf("expected 123, got val=%d err=%v", val, err)
	}

	// 2. ZScore & ZRem
	zKey := client.Key("zset")
	_ = client.ZincrBy(ctx, zKey, "item1", 10.5)

	score, err := client.ZScore(ctx, zKey, "item1")
	if err != nil || score != 10.5 {
		t.Fatalf("expected score 10.5, got score=%f err=%v", score, err)
	}

	_, err = client.ZScore(ctx, zKey, "item2")
	if !IsMiss(err) {
		t.Fatalf("expected miss for item2, got %v", err)
	}

	// ZRem
	if err := client.ZRem(ctx, zKey, "item1"); err != nil {
		t.Fatalf("ZRem failed: %v", err)
	}
	_, err = client.ZScore(ctx, zKey, "item1")
	if !IsMiss(err) {
		t.Fatalf("expected item1 removed, got err=%v", err)
	}

	// Pipeline
	if client.Pipeline() == nil {
		t.Fatalf("expected non-nil pipeline")
	}
}

