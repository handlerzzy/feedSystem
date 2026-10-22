package worker

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/handlerzzy/feedsystem/internal/video"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type dummyConnPool struct{}

func (*dummyConnPool) PrepareContext(context.Context, string) (*sql.Stmt, error) { return nil, nil }
func (*dummyConnPool) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return nil, nil
}
func (*dummyConnPool) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, nil
}
func (*dummyConnPool) QueryRowContext(context.Context, string, ...any) *sql.Row { return nil }
func (d *dummyConnPool) BeginTx(ctx context.Context, opts *sql.TxOptions) (gorm.ConnPool, error) {
	return d, nil
}
func (*dummyConnPool) Commit() error   { return nil }
func (*dummyConnPool) Rollback() error { return nil }

type sqlCaptureLogger struct {
	logger.Interface
	captured []string
}

func (l *sqlCaptureLogger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	sql, _ := fc()
	l.captured = append(l.captured, sql)
}

func TestOutboxPoller_ClaimSQLRoutingAndLocking(t *testing.T) {
	capLogger := &sqlCaptureLogger{}
	dryDB, err := gorm.Open(mysql.New(mysql.Config{
		Conn:                      &dummyConnPool{},
		SkipInitializeWithVersion: true,
	}), &gorm.Config{
		DryRun: true,
		Logger: capLogger,
	})
	if err != nil {
		t.Fatalf("open DryRun DB failed: %v", err)
	}

	poller := &outboxPoller{db: dryDB, mq: nil}

	// 直接调用生产方法 claim()，捕获真实执行的 SQL 与参数绑定
	if _, err := poller.claim(context.Background(), 100); err != nil {
		t.Fatalf("poller.claim failed: %v", err)
	}

	foundClaimSQL := false
	for _, sql := range capLogger.captured {
		if strings.Contains(sql, "outbox_msgs") {
			foundClaimSQL = true
			if !strings.Contains(sql, "status") || !strings.Contains(sql, "event_type") {
				t.Errorf("claim SQL 缺少 status 或 event_type 过滤条件: %s", sql)
			}
			if !strings.Contains(sql, "FOR UPDATE") || !strings.Contains(sql, "SKIP LOCKED") {
				t.Errorf("claim SQL 缺少 FOR UPDATE SKIP LOCKED 悲观锁: %s", sql)
			}
			if !strings.Contains(sql, video.OutboxEventTimeline) {
				t.Errorf("claim SQL 缺少 timeline 事件类型: %s", sql)
			}
		}
	}
	if !foundClaimSQL {
		t.Errorf("poller.claim 未捕获到针对 outbox_msgs 的查询 SQL: %v", capLogger.captured)
	}

	// 直接调用生产方法 requeueZombies()，捕获真实回收 SQL
	capLogger.captured = nil
	poller.requeueZombies(context.Background())

	foundRequeueSQL := false
	for _, sql := range capLogger.captured {
		if strings.Contains(sql, "outbox_msgs") && strings.Contains(sql, "UPDATE") {
			foundRequeueSQL = true
			if !strings.Contains(sql, "event_type") || !strings.Contains(sql, video.OutboxEventTimeline) {
				t.Errorf("requeueZombies SQL 缺少 timeline 路由过滤: %s", sql)
			}
		}
	}
	if !foundRequeueSQL {
		t.Errorf("poller.requeueZombies 未捕获到回收 SQL: %v", capLogger.captured)
	}
}

func openWorkerTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("TEST_MYSQL_DSN 未设置，跳过依赖真实 MySQL 的集成测试")
	}
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("连接测试数据库失败: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("获取 sql.DB 失败: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := sqlDB.Ping(); err != nil {
		t.Fatalf("数据库不可达: %v", err)
	}
	if err := db.AutoMigrate(&video.OutboxMsg{}); err != nil {
		t.Fatalf("AutoMigrate 失败: %v", err)
	}
	return db
}

func TestOutboxPoller_ClaimFiltersByEventType(t *testing.T) {
	db := openWorkerTestDB(t)

	// 清理并构造测试数据
	const testVideoID1 = 988001
	const testVideoID2 = 988002

	msgOther := video.OutboxMsg{
		VideoID:    testVideoID1,
		EventType:  "content_analysis",
		Status:     outboxStatusPending,
		CreateTime: time.Now().Add(-2 * time.Minute),
	}
	msgTimeline := video.OutboxMsg{
		VideoID:    testVideoID2,
		EventType:  video.OutboxEventTimeline, // "timeline"
		Status:     outboxStatusPending,
		CreateTime: time.Now().Add(-1 * time.Minute),
	}

	if err := db.Create(&msgOther).Error; err != nil {
		t.Fatalf("插入 msgOther 失败: %v", err)
	}
	if err := db.Create(&msgTimeline).Error; err != nil {
		t.Fatalf("插入 msgTimeline 失败: %v", err)
	}

	t.Cleanup(func() {
		db.Exec("DELETE FROM outbox_msgs WHERE id IN (?, ?)", msgOther.ID, msgTimeline.ID)
	})

	poller := &outboxPoller{db: db, mq: nil}

	claimed, err := poller.claim(context.Background(), 10)
	if err != nil {
		t.Fatalf("claim failed: %v", err)
	}

	// 验证：必须严格只抢占 event_type = 'timeline' 的消息
	foundTimeline := false
	for _, m := range claimed {
		if m.ID == msgOther.ID {
			t.Errorf("claim() 错误地抢占了非 timeline 事件 (id=%d, event_type=%s)，存在抢跑漏洞！", m.ID, m.EventType)
		}
		if m.ID == msgTimeline.ID {
			foundTimeline = true
		}
	}
	if !foundTimeline {
		t.Errorf("claim() 未能抢占到 timeline 消息 (id=%d)", msgTimeline.ID)
	}

	// 验证数据库状态：msgOther 依然必须保持 pending，没有被置为 processing
	var checkOther video.OutboxMsg
	if err := db.First(&checkOther, msgOther.ID).Error; err != nil {
		t.Fatalf("查询 msgOther 失败: %v", err)
	}
	if checkOther.Status != outboxStatusPending {
		t.Errorf("msgOther status = %s, want %s (非 timeline 事件被错误修改)", checkOther.Status, outboxStatusPending)
	}
}
