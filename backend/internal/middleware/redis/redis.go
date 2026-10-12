package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"github.com/handlerzzy/feedsystem/internal/config"
	"fmt"
	"strconv"
	"time"

	redis "github.com/redis/go-redis/v9"
)

type Client struct {
	rdb       *redis.Client
	keyPrefix string
}

const DefaultKeyPrefix = "v1:"
const AccountKeyPattern = "account:%d"
const FeedGlobalTimelineKey = "feed:global_timeline"

// AccountKey 返回默认前缀下的用户 Token 缓存键
func AccountKey(accountID uint) string {
	return DefaultKeyPrefix + fmt.Sprintf(AccountKeyPattern, accountID)
}

// GlobalTimelineKey 返回默认前缀下的全局时间线缓存键
func GlobalTimelineKey() string {
	return DefaultKeyPrefix + FeedGlobalTimelineKey
}

func NewClient(rdb *redis.Client, keyPrefix string) *Client {
	return &Client{rdb: rdb, keyPrefix: keyPrefix}
}

func NewFromEnv(cfg *config.RedisConfig) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Host + ":" + strconv.Itoa(cfg.Port),
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	return &Client{rdb: rdb, keyPrefix: DefaultKeyPrefix}, nil
}

func (c *Client) Close() error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.Close()
}

func (c *Client) Ping(ctx context.Context) error {
	if c == nil || c.rdb == nil {
		return errors.New("redis client not initialized")
	}
	return c.rdb.Ping(ctx).Err()
}

func IsMiss(err error) bool {
	return err == redis.Nil
}

func (c *Client) Key(format string, args ...any) string {
	prefix := ""
	if c != nil {
		prefix = c.keyPrefix
	}
	return prefix + fmt.Sprintf(format, args...)
}

// AccountKey 基于当前客户端配置的 keyPrefix 返回账号缓存键
func (c *Client) AccountKey(accountID uint) string {
	return c.Key(AccountKeyPattern, accountID)
}

// GlobalTimelineKey 基于当前客户端配置的 keyPrefix 返回全局时间线缓存键
func (c *Client) GlobalTimelineKey() string {
	return c.Key(FeedGlobalTimelineKey)
}

func randToken(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func (c *Client) Lock(ctx context.Context, key string, ttl time.Duration) (token string, ok bool, err error) {
	if c == nil || c.rdb == nil {
		return "", false, nil
	}
	token, err = randToken(16)
	if err != nil {
		return "", false, err
	}
	ok, err = c.rdb.SetNX(ctx, key, token, ttl).Result()
	return token, ok, err
}

var unlockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end
`)

var incrementWithExpireScript = redis.NewScript(`
local count = redis.call("INCR", KEYS[1])
if count == 1 then
  redis.call("PEXPIRE", KEYS[1], ARGV[1])
end
return count
`)

func (c *Client) Unlock(ctx context.Context, key string, token string) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	_, err := unlockScript.Run(ctx, c.rdb, []string{key}, token).Result()
	return err
}

func (c *Client) IncrementWithExpire(ctx context.Context, key string, expire time.Duration) (int64, error) {
	if c == nil || c.rdb == nil {
		return 0, nil
	}
	return incrementWithExpireScript.Run(
		ctx,
		c.rdb,
		[]string{key},
		expire.Milliseconds(),
	).Int64()
}
