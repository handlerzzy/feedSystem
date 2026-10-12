package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/handlerzzy/feedsystem/internal/account"
	"github.com/handlerzzy/feedsystem/internal/auth"
	"github.com/handlerzzy/feedsystem/internal/embed"
	rediscache "github.com/handlerzzy/feedsystem/internal/middleware/redis"
	"github.com/handlerzzy/feedsystem/internal/video"
	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

type TokenItem struct {
	AccountID uint   `json:"account_id"`
	Username  string `json:"username"`
	Token     string `json:"token"`
}

var sampleVideoTemplates = []struct {
	Title       string
	Description string
	Tags        []string
}{
	{"Go 微服务高并发架构演进与全链路压测实战", "深入解析分布式短视频系统的设计与性能优化", []string{"架构", "Go", "高并发"}},
	{"Redis 缓存穿透与曝光过滤最佳实践", "基于滑动窗口与分块集合的去重过滤技巧", []string{"Redis", "架构", "数据库"}},
	{"短视频个性化召回与向量检索实战教程", "从零理解用户兴趣画像计算与向量相似度召回", []string{"推荐系统", "AI", "向量池"}},
	{"从零搭建 Docker 容器化自动化压测看板", "使用 k6 与 Grafana 构建指标可视化监控", []string{"DevOps", "Docker", "测试"}},
	{"周末沉浸式露营日记与户外美食制作", "逃离城市计划，在山野间煮一杯手冲咖啡", []string{"生活", "美食", "露营"}},
	{"摄影入门必看：手机也能拍出电影感大片", "掌握构图与光影，随手拍出大片质感", []string{"摄影", "技巧", "生活"}},
	{"超下饭家常红烧肉做法，肥而不腻秘诀公开", "大厨教你家常红烧肉，简单几步就搞定", []string{"美食", "烹饪", "家常菜"}},
	{"硬核拆解最新旗舰手机：内部做工用料评测", "高刷屏幕与散热模组全方位极限实测", []string{"科技", "数码", "手机"}},
	{"零基础吉他弹唱教学：十分钟学会经典名曲", "新手入门吉他指法与和弦转换干货分享", []string{"音乐", "吉他", "艺术"}},
	{"猫咪日常搞笑迷惑行为大赏合集", "家里的猫主子又在做什么奇怪的事情呢", []string{"宠物", "萌宠", "搞笑"}},
}

func main() {
	defaultMySQL := os.Getenv("MYSQL_DSN")
	if defaultMySQL == "" {
		defaultMySQL = "root:123456@tcp(mysql:3306)/feedflow?charset=utf8mb4&parseTime=True&loc=Local"
	}
	defaultRedis := os.Getenv("REDIS_ADDR")
	if defaultRedis == "" {
		defaultRedis = "redis:6379"
	}
	defaultSecret := os.Getenv("JWT_SECRET")
	if defaultSecret == "" {
		defaultSecret = "feedflow-dev-secret-key"
	}

	mysqlDSN := flag.String("mysql", defaultMySQL, "MySQL DSN")
	redisAddr := flag.String("redis", defaultRedis, "Redis server address")
	redisPass := flag.String("redis-pass", "123456", "Redis password")
	jwtSecret := flag.String("jwt-secret", defaultSecret, "JWT secret")
	userCount := flag.Int("users", 500, "Number of test users to ensure")
	targetVideos := flag.Int("videos", 100, "Target number of videos in candidate pool")
	outPath := flag.String("out", "test/load/tokens.json", "Path to write tokens.json")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	fmt.Printf("[seedload] 连接 MySQL: %s\n", *mysqlDSN)
	db, err := gorm.Open(mysql.Open(*mysqlDSN), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Warn),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "连接 MySQL 失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("[seedload] 连接 Redis: %s\n", *redisAddr)
	rdb := redis.NewClient(&redis.Options{
		Addr:     *redisAddr,
		Password: *redisPass,
		DB:       0,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		fmt.Fprintf(os.Stderr, "连接 Redis 失败: %v\n", err)
		os.Exit(1)
	}
	defer rdb.Close()

	if err := auth.SetSecret(*jwtSecret); err != nil {
		fmt.Fprintf(os.Stderr, "初始化 JWT secret 失败: %v\n", err)
		os.Exit(1)
	}
	_ = auth.SetAccessTokenTTL(24 * time.Hour)

	// 1. 确保候选视频池与向量数据充沛
	videoIDs := seedVideosAndEmbeddings(ctx, db, rdb, *targetVideos)
	fmt.Printf("[seedload] 候选视频池准备就绪，共 %d 条可用视频\n", len(videoIDs))

	// 2. 批量预置压测账号、JWT 凭据与历史点赞记录
	tokens := seedUsersAndLikes(ctx, db, rdb, *userCount, videoIDs)
	fmt.Printf("[seedload] 成功预置 %d 个带有真实点赞历史的压测账号\n", len(tokens))

	// 3. 清理限流键，确保压测链路畅通
	clearRateLimits(ctx, rdb)

	// 4. 写出 tokens.json
	if err := os.MkdirAll(filepath.Dir(*outPath), 0755); err != nil {
		fmt.Fprintf(os.Stderr, "创建输出目录失败: %v\n", err)
		os.Exit(1)
	}
	tokenData, err := json.MarshalIndent(tokens, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "序列化 tokens 失败: %v\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(*outPath, tokenData, 0644); err != nil {
		fmt.Fprintf(os.Stderr, "写入 %s 失败: %v\n", *outPath, err)
		os.Exit(1)
	}
	fmt.Printf("[seedload] ✅ 压测凭据已保存到: %s\n", *outPath)
}

func seedVideosAndEmbeddings(ctx context.Context, db *gorm.DB, rdb *redis.Client, targetCount int) []uint {
	var existingVideos []video.Video
	if err := db.WithContext(ctx).Select("id", "title", "description", "create_time").Find(&existingVideos).Error; err != nil {
		fmt.Fprintf(os.Stderr, "查询既有视频失败: %v\n", err)
	}

	existingMap := make(map[uint]video.Video)
	var videoIDs []uint
	for _, v := range existingVideos {
		existingMap[v.ID] = v
		videoIDs = append(videoIDs, v.ID)
	}

	needed := targetCount - len(existingVideos)
	if needed > 0 {
		fmt.Printf("[seedload] 当前视频库仅 %d 条，自动补齐至 %d 条...\n", len(existingVideos), targetCount)
		now := time.Now()
		var newVideos []video.Video
		for i := 0; i < needed; i++ {
			tpl := sampleVideoTemplates[i%len(sampleVideoTemplates)]
			v := video.Video{
				AuthorID:    1,
				Username:    "system_creator",
				Title:       fmt.Sprintf("%s (%d)", tpl.Title, i+1),
				Description: tpl.Description,
				PlayURL:     "https://example.com/video/sample.mp4",
				CoverURL:    "https://example.com/cover/sample.jpg",
				CreateTime:  now.Add(-time.Duration(i*30) * time.Minute),
				LikesCount:  int64(rand.Intn(500) + 10),
				Popularity:  int64(rand.Intn(1000) + 50),
			}
			newVideos = append(newVideos, v)
		}
		if err := db.WithContext(ctx).Create(&newVideos).Error; err != nil {
			fmt.Fprintf(os.Stderr, "批量创建视频失败: %v\n", err)
		} else {
			for _, v := range newVideos {
				existingMap[v.ID] = v
				videoIDs = append(videoIDs, v.ID)
			}
		}
	}

	// 为所有视频补齐向量与全局时间线缓存
	var embeddings []video.VideoEmbedding
	for _, v := range existingMap {
		content := video.EmbeddingText(v.Title, v.Description)
		vec := embed.Encode(content)
		normVec := video.L2Normalize(vec)
		encoded, err := video.EncodeVector(normVec)
		if err != nil {
			continue
		}
		hash := video.ContentHash(v.Title, v.Description)

		// 注入 text-embedding-3-small 与 faux-lexical 双模型空间
		models := []string{"text-embedding-3-small", "faux-lexical"}
		for _, m := range models {
			embeddings = append(embeddings, video.VideoEmbedding{
				VideoID:     v.ID,
				Model:       m,
				Dim:         1536,
				ContentHash: hash,
				Normalized:  true,
				Vector:      encoded,
			})
		}

		// 同步注入 Redis 全局时间线
		score := float64(v.CreateTime.UnixMilli())
		_ = rdb.ZAdd(ctx, rediscache.GlobalTimelineKey(), redis.Z{
			Score:  score,
			Member: strconv.FormatUint(uint64(v.ID), 10),
		}).Err()
	}

	if len(embeddings) > 0 {
		// 批量分块 Upsert 向量
		chunkSize := 100
		for i := 0; i < len(embeddings); i += chunkSize {
			end := i + chunkSize
			if end > len(embeddings) {
				end = len(embeddings)
			}
			chunk := embeddings[i:end]
			if err := db.WithContext(ctx).Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "video_id"}, {Name: "model"}, {Name: "dim"}},
				DoUpdates: clause.AssignmentColumns([]string{"content_hash", "normalized", "vec", "updated_at"}),
			}).Create(&chunk).Error; err != nil {
				fmt.Fprintf(os.Stderr, "Upsert 向量块失败: %v\n", err)
			}
		}
	}

	return videoIDs
}

// syncUserToken 为指定用户生成 JWT 并同步持久化到 MySQL 与写入 Redis 缓存（使用领域 Key 格式）
func syncUserToken(ctx context.Context, db *gorm.DB, rdb *redis.Client, accountID uint, username string) (string, error) {
	token, err := auth.GenerateToken(accountID, username)
	if err != nil {
		return "", err
	}
	if err := db.WithContext(ctx).Model(&account.Account{}).Where("id = ?", accountID).Update("token", token).Error; err != nil {
		return "", err
	}
	// 使用领域 Redis Key 格式化规范
	redisKey := rediscache.AccountKey(accountID)
	if err := rdb.Set(ctx, redisKey, token, 24*time.Hour).Err(); err != nil {
		return "", err
	}
	return token, nil
}

func seedUsersAndLikes(ctx context.Context, db *gorm.DB, rdb *redis.Client, count int, videoIDs []uint) []TokenItem {
	if count <= 0 {
		count = 500
	}
	passwordHash, err := bcrypt.GenerateFromPassword([]byte("LoadTest@123"), bcrypt.DefaultCost)
	if err != nil {
		fmt.Fprintf(os.Stderr, "生成 bcrypt 密码 hash 失败: %v\n", err)
		os.Exit(1)
	}

	var tokens []TokenItem
	var accountsToCreate []account.Account
	now := time.Now()

	// 1. 查询已存在的 loadtest 账号
	var existingAccounts []account.Account
	db.WithContext(ctx).Where("username LIKE 'loadtest_user_%'").Find(&existingAccounts)
	existingUserMap := make(map[string]account.Account)
	for _, acc := range existingAccounts {
		existingUserMap[acc.Username] = acc
	}

	var usersNeedingLikes []uint

	recordUserToken := func(token string, acc account.Account) {
		tokens = append(tokens, TokenItem{
			AccountID: acc.ID,
			Username:  acc.Username,
			Token:     token,
		})
		usersNeedingLikes = append(usersNeedingLikes, acc.ID)
	}

	// 2. 生成/补齐账号
	for i := 1; i <= count; i++ {
		uname := fmt.Sprintf("loadtest_user_%04d", i)
		if existing, ok := existingUserMap[uname]; ok {
			token, err := syncUserToken(ctx, db, rdb, existing.ID, existing.Username)
			if err != nil {
				continue
			}
			recordUserToken(token, existing)
		} else {
			accountsToCreate = append(accountsToCreate, account.Account{
				Username: uname,
				Password: string(passwordHash),
				Bio:      "Automated load test agent",
			})
		}
	}

	if len(accountsToCreate) > 0 {
		chunkSize := 100
		for i := 0; i < len(accountsToCreate); i += chunkSize {
			end := i + chunkSize
			if end > len(accountsToCreate) {
				end = len(accountsToCreate)
			}
			chunk := accountsToCreate[i:end]
			if err := db.WithContext(ctx).Create(&chunk).Error; err != nil {
				fmt.Fprintf(os.Stderr, "批量创建用户失败: %v\n", err)
			}
			for _, created := range chunk {
				token, err := syncUserToken(ctx, db, rdb, created.ID, created.Username)
				if err != nil {
					continue
				}
				recordUserToken(token, created)
			}
		}
	}

	// 3. 为所有压测用户注入历史点赞互动行为（每人 5~15 条点赞）
	if len(videoIDs) > 0 {
		var likesToInsert []video.Like
		for _, uid := range usersNeedingLikes {
			// 随机挑 5 ~ 15 个不同的视频进行点赞
			numLikes := rand.Intn(11) + 5
			if numLikes > len(videoIDs) {
				numLikes = len(videoIDs)
			}
			perm := rand.Perm(len(videoIDs))
			for j := 0; j < numLikes; j++ {
				vid := videoIDs[perm[j]]
				likesToInsert = append(likesToInsert, video.Like{
					VideoID:   vid,
					AccountID: uid,
					CreatedAt: now.Add(-time.Duration(j*2) * time.Hour),
				})
			}
		}

		if len(likesToInsert) > 0 {
			chunkSize := 200
			for i := 0; i < len(likesToInsert); i += chunkSize {
				end := i + chunkSize
				if end > len(likesToInsert) {
					end = len(likesToInsert)
				}
				chunk := likesToInsert[i:end]
				_ = db.WithContext(ctx).Clauses(clause.OnConflict{
					DoNothing: true,
				}).Create(&chunk).Error
			}
		}
	}

	return tokens
}

func clearRateLimits(ctx context.Context, rdb *redis.Client) {
	patterns := []string{"feedflow:ratelimit:*", "v1:ratelimit:*", "*ratelimit*"}
	totalCleaned := 0
	for _, p := range patterns {
		iter := rdb.Scan(ctx, 0, p, 0).Iterator()
		var keys []string
		for iter.Next(ctx) {
			keys = append(keys, iter.Val())
		}
		if len(keys) > 0 {
			_ = rdb.Del(ctx, keys...).Err()
			totalCleaned += len(keys)
		}
	}
	if totalCleaned > 0 {
		fmt.Printf("[seedload] 清理了 %d 个限流缓存键\n", totalCleaned)
	}
}
