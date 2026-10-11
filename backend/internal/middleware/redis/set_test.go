package redis

import (
	"context"
	"sort"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

func TestSetOperationsAndSlidingExpiration(t *testing.T) {
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
	key := "test:exposed:set"
	ttl := 15 * time.Minute

	// 1. SAddWithExpire: 添加成员并设置 TTL
	if err := client.SAddWithExpire(ctx, key, ttl, "10", "20", "30"); err != nil {
		t.Fatalf("SAddWithExpire failed: %v", err)
	}

	members, err := client.SMembers(ctx, key)
	if err != nil {
		t.Fatalf("SMembers failed: %v", err)
	}
	sort.Strings(members)
	if len(members) != 3 || members[0] != "10" || members[1] != "20" || members[2] != "30" {
		t.Fatalf("unexpected members: %v", members)
	}

	initialTTL := mr.TTL(key)
	if initialTTL <= 0 || initialTTL > ttl {
		t.Fatalf("expected ttl in (0, %v], got %v", ttl, initialTTL)
	}

	// 2. 模拟时间推进并再次追加成员（验证滑动过期重置）
	mr.FastForward(5 * time.Minute)

	if err := client.SAddWithExpire(ctx, key, ttl, "40"); err != nil {
		t.Fatalf("second SAddWithExpire failed: %v", err)
	}

	members, err = client.SMembers(ctx, key)
	if err != nil {
		t.Fatalf("SMembers after append failed: %v", err)
	}
	sort.Strings(members)
	if len(members) != 4 || members[3] != "40" {
		t.Fatalf("unexpected members after append: %v", members)
	}

	// 追加后 TTL 应该被刷新回接近 ttl
	refreshedTTL := mr.TTL(key)
	if refreshedTTL <= 10*time.Minute {
		t.Fatalf("expected refreshed ttl > 10m, got %v", refreshedTTL)
	}

	// 3. SIsMember
	isMem, err := client.SIsMember(ctx, key, "20")
	if err != nil || !isMem {
		t.Fatalf("expected 20 to be member, got %v, err=%v", isMem, err)
	}
	isNotMem, err := client.SIsMember(ctx, key, "999")
	if err != nil || isNotMem {
		t.Fatalf("expected 999 to not be member, got %v, err=%v", isNotMem, err)
	}

	// 4. 自然过期重置
	mr.FastForward(16 * time.Minute)
	if mr.Exists(key) {
		t.Fatalf("expected key to expire, but still exists")
	}
	expiredMembers, err := client.SMembers(ctx, key)
	if err != nil {
		t.Fatalf("SMembers on expired key failed: %v", err)
	}
	if len(expiredMembers) != 0 {
		t.Fatalf("expected 0 members on expired key, got %v", expiredMembers)
	}
}
