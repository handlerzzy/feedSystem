package feed

import (
	"context"
	"strconv"
	"time"

	"github.com/handlerzzy/feedsystem/internal/logging"

	"go.uber.org/zap"
)

// DefaultExposedFilterTTL 是用户曝光过滤集合的默认滑动窗口过期时间（15~30 分钟，依 Spec §4.3 取 30 分钟）。
const DefaultExposedFilterTTL = 30 * time.Minute

// WithExposedFilterTTL 设置曝光过滤集合的滑动 TTL。
func (f *FeedService) WithExposedFilterTTL(ttl time.Duration) *FeedService {
	if ttl > 0 {
		f.exposedFilterTTL = ttl
	}
	return f
}

// ExposedFilterTTL 返回当前配置的曝光过滤滑动 TTL。
func (f *FeedService) ExposedFilterTTL() time.Duration {
	if f.exposedFilterTTL <= 0 {
		return DefaultExposedFilterTTL
	}
	return f.exposedFilterTTL
}

// RecordExposed 将视频 ID 批量追加写入用户的 Redis 曝光过滤集合，并刷新滑动过期时间。
func (f *FeedService) RecordExposed(ctx context.Context, accountID uint, videoIDs []uint) error {
	if f.rediscache == nil || accountID == 0 || len(videoIDs) == 0 {
		return nil
	}
	key := f.rediscache.Key("feed:exposed:%d", accountID)
	members := make([]any, len(videoIDs))
	for i, id := range videoIDs {
		members[i] = strconv.FormatUint(uint64(id), 10)
	}
	ttl := f.ExposedFilterTTL()
	return f.rediscache.SAddWithExpire(ctx, key, ttl, members...)
}

// recordExposedAsync 异步向 Redis 追加写入曝光视频 ID 并刷新滑动过期时间（不阻塞核心读路径）。
func (f *FeedService) recordExposedAsync(accountID uint, videoIDs []uint) {
	if f.rediscache == nil || accountID == 0 || len(videoIDs) == 0 {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		if err := f.RecordExposed(ctx, accountID, videoIDs); err != nil {
			logging.Ctx(ctx).Warn("记录用户曝光过滤集合失败（不影响主流程）",
				zap.Uint("account_id", accountID),
				zap.Error(err))
		}
		if f.onExposedRecorded != nil {
			f.onExposedRecorded(accountID, videoIDs)
		}
	}()
}

// getExposedFilterSet 从 Redis 批量读取用户的短时滑动曝光过滤集合。
//
// 任何失败（Redis 异常、超时、未配置）均静默降级并返回 nil，语义召回照常执行（不阻断 Feed 读链路）。
func (f *FeedService) getExposedFilterSet(ctx context.Context, accountID uint) map[uint]bool {
	if f.rediscache == nil || accountID == 0 {
		return nil
	}
	key := f.rediscache.Key("feed:exposed:%d", accountID)
	members, err := f.rediscache.SMembers(ctx, key)
	if err != nil {
		logging.Ctx(ctx).Warn("读取用户曝光过滤集合失败，本次不进行曝光过滤（不影响 Feed 结果）",
			zap.Uint("account_id", accountID),
			zap.Error(err))
		return nil
	}
	if len(members) == 0 {
		return nil
	}
	set := make(map[uint]bool, len(members))
	for _, m := range members {
		if id, err := strconv.ParseUint(m, 10, 64); err == nil {
			set[uint(id)] = true
		}
	}
	return set
}
