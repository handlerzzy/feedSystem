package db

import (
	"github.com/handlerzzy/feedsystem/internal/account"
	"github.com/handlerzzy/feedsystem/internal/config"
	"github.com/handlerzzy/feedsystem/internal/message"
	"github.com/handlerzzy/feedsystem/internal/social"
	"github.com/handlerzzy/feedsystem/internal/video"
	"github.com/handlerzzy/feedsystem/internal/worker"
	"fmt"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func NewDB(dbcfg config.DatabaseConfig) (*gorm.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		dbcfg.User, dbcfg.Password, dbcfg.Host, dbcfg.Port, dbcfg.DBName)

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		// 不用 GORM 的默认 logger：它自己写 stdout、带 ANSI 颜色，
		// 绕过 zap.RedirectStdLog，且会把每条 SQL 与参数无条件打进日志。
		Logger: newGormLogger(gormLevelForAppLog(), 200*time.Millisecond, true),
	})
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	// 配置连接池参数，防止高并发下短连接频繁创建销毁导致 TIME_WAIT 端口耗尽 (cannot assign requested address)
	sqlDB.SetMaxIdleConns(100)
	sqlDB.SetMaxOpenConns(300)
	sqlDB.SetConnMaxLifetime(time.Hour)
	sqlDB.SetConnMaxIdleTime(10 * time.Minute)

	return db, nil
}

func AutoMigrate(db *gorm.DB) error {
	return db.AutoMigrate(
		&account.Account{}, &video.Video{}, &video.Like{}, &video.Comment{},
		&social.Social{}, &video.OutboxMsg{}, &video.Tag{}, &video.VideoTag{},
		&message.Message{}, &worker.Notification{},
		// P1 新增：AI 分析结果表。
		// AutoMigrate 是**硬编码白名单**，不登记就不会建表（总览 6.2），
		// 而且它只在 API 进程启动时执行——Worker 不执行，所以本地只跑 worker
		// 是看不到这张表的。
		&video.VideoAIAnalysis{},
		// P2 前置 B3 新增：视频内容向量表。
		// 它记录 model / dim / content_hash，因此"换模型"与"内容变更"
		// 两种失效都能被查询出来，而不是靠人记着要重算。
		&video.VideoEmbedding{},
	)
}

func CloseDB(db *gorm.DB) error {
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}
