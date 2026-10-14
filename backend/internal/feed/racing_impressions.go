package feed

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/handlerzzy/feedsystem/internal/logging"
	rediscache "github.com/handlerzzy/feedsystem/internal/middleware/redis"
	redis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	// DefaultRacingUserTTL 是单用户对同一视频赛马防重的滑动窗口期（24 小时）。
	DefaultRacingUserTTL = 24 * time.Hour

	// RacingCandidatesKey 是活跃赛马候选队列的 Redis ZSet 键名（无前缀）。
	RacingCandidatesKey = "feed:racing:candidates"
)

// WithRacingUserTTL 设置赛马防重集合的滑动 TTL。
func (f *FeedService) WithRacingUserTTL(ttl time.Duration) *FeedService {
	if ttl > 0 {
		f.racingUserTTL = ttl
	}
	return f
}

// RacingUserTTL 返回当前配置的赛马防重滑动 TTL。
func (f *FeedService) RacingUserTTL() time.Duration {
	if f.racingUserTTL <= 0 {
		return DefaultRacingUserTTL
	}
	return f.racingUserTTL
}

// recordRacingScript 是单视频赛马曝光原子判重与计数累加的 Lua 脚本：
// KEYS[1]: 用户的防重集合键 feed:video:racing_users:{video_id}
// KEYS[2]: 视频赛马曝光计数键 feed:video:racing_impressions:{video_id}
// KEYS[3]: 赛马活跃候选池键 feed:racing:candidates
// ARGV[1]: 用户 ID
// ARGV[2]: 防重集合过期秒数 (int64)
// ARGV[3]: 视频 ID (string)
//
// 返回值：1 表示首刷成功计入有效曝光，0 表示重复刷已幂等去重。
const recordRacingScriptSource = `
local added = redis.call("SADD", KEYS[1], ARGV[1])
if added == 1 then
    redis.call("EXPIRE", KEYS[1], ARGV[2])
    redis.call("INCR", KEYS[2])
    redis.call("ZINCRBY", KEYS[3], 1, ARGV[3])
    return 1
else
    return 0
end
`

// RecordRacingImpression 为单视频记录单用户的赛马曝光。
// 若用户在 24 小时滑动窗口内首次曝光该视频，返回 (true, nil)，并原子递增曝光计数与候选池 Score；
// 若为重复曝光，返回 (false, nil)，不递增计数与 Score。
func (f *FeedService) RecordRacingImpression(ctx context.Context, accountID uint, videoID uint) (bool, error) {
	if f.rediscache == nil || accountID == 0 || videoID == 0 {
		return false, nil
	}
	results, err := f.RecordRacingImpressions(ctx, accountID, []uint{videoID})
	if err != nil {
		return false, err
	}
	if len(results) > 0 {
		return results[0], nil
	}
	return false, nil
}

// RecordRacingImpressions 采用 Redis Pipeline 批量为用户记录多个视频的赛马曝光。
// 对每个视频独立通过 Lua 脚本原子执行防重、曝光计数递增与 ZSet 候选池更新，1 次网络往返（RTT）完成。
// 返回的 bool 切片与 videoIDs 一一对应，true 表示首次有效曝光，false 表示幂等去重。
func (f *FeedService) RecordRacingImpressions(ctx context.Context, accountID uint, videoIDs []uint) ([]bool, error) {
	if f.rediscache == nil || accountID == 0 || len(videoIDs) == 0 {
		return make([]bool, len(videoIDs)), nil
	}

	pipe := f.rediscache.Pipeline()
	if pipe == nil {
		return make([]bool, len(videoIDs)), nil
	}

	ttlSeconds := int64(f.RacingUserTTL() / time.Second)
	if ttlSeconds <= 0 {
		ttlSeconds = 86400
	}
	accountStr := strconv.FormatUint(uint64(accountID), 10)
	candidatesKey := f.rediscache.Key(RacingCandidatesKey)

	cmds := make([]*redis.Cmd, len(videoIDs))

	for i, vid := range videoIDs {
		usersKey := f.rediscache.Key("feed:video:racing_users:%d", vid)
		impressionsKey := f.rediscache.Key("feed:video:racing_impressions:%d", vid)
		videoStr := strconv.FormatUint(uint64(vid), 10)

		cmds[i] = pipe.Eval(ctx, recordRacingScriptSource,
			[]string{usersKey, impressionsKey, candidatesKey},
			accountStr, ttlSeconds, videoStr,
		)
	}

	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("execute racing impressions pipeline: %w", err)
	}

	results := make([]bool, len(videoIDs))
	for i, cmd := range cmds {
		val, err := cmd.Int64()
		if err == nil && val == 1 {
			results[i] = true
		} else {
			results[i] = false
		}
	}

	return results, nil
}

// recordRacingImpressionsAsync 异步向 Redis 追加记录赛马曝光并更新活跃候选池（不阻塞 Feed 读主链路）。
func (f *FeedService) recordRacingImpressionsAsync(accountID uint, videoIDs []uint) {
	if f.rediscache == nil || accountID == 0 || len(videoIDs) == 0 {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		if _, err := f.RecordRacingImpressions(ctx, accountID, videoIDs); err != nil {
			logging.Ctx(ctx).Warn("记录视频赛马曝光失败（不影响主流程）",
				zap.Uint("account_id", accountID),
				zap.Error(err))
		}
		if f.onRacingRecorded != nil {
			f.onRacingRecorded(accountID, videoIDs)
		}
	}()
}

// GetRacingImpressions 获取指定视频当前的累计有效赛马曝光量。
func (f *FeedService) GetRacingImpressions(ctx context.Context, videoID uint) (int64, error) {
	if f.rediscache == nil || videoID == 0 {
		return 0, nil
	}
	key := f.rediscache.Key("feed:video:racing_impressions:%d", videoID)
	impr, err := f.rediscache.GetInt64(ctx, key)
	if err != nil {
		if rediscache.IsMiss(err) {
			return 0, nil
		}
		return 0, err
	}
	return impr, nil
}

// GetCandidateScore 获取视频在赛马候选队列（feed:racing:candidates）中的当前 Score。
// 若视频不在候选队列中，返回 (0, false, nil)。
func (f *FeedService) GetCandidateScore(ctx context.Context, videoID uint) (float64, bool, error) {
	if f.rediscache == nil || videoID == 0 {
		return 0, false, nil
	}
	key := f.rediscache.Key(RacingCandidatesKey)
	member := strconv.FormatUint(uint64(videoID), 10)
	score, err := f.rediscache.ZScore(ctx, key, member)
	if err != nil {
		if rediscache.IsMiss(err) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return score, true, nil
}

// RemoveCandidate 将视频从赛马候选队列中移除（例如在赛马结算跃迁完成后移出活跃池）。
func (f *FeedService) RemoveCandidate(ctx context.Context, videoID uint) error {
	if f.rediscache == nil || videoID == 0 {
		return nil
	}
	key := f.rediscache.Key(RacingCandidatesKey)
	member := strconv.FormatUint(uint64(videoID), 10)
	return f.rediscache.ZRem(ctx, key, member)
}
