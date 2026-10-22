package feed

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/handlerzzy/feedsystem/internal/video"

	mysqldriver "github.com/go-sql-driver/mysql"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// ---------- Seam 1: Feed 仓储读隔离与种子池优先检索测试 ----------

type testConnPool struct{}

func (*testConnPool) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return nil, nil
}
func (*testConnPool) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return nil, nil
}
func (*testConnPool) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return nil, nil
}
func (*testConnPool) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return nil
}
func (*testConnPool) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return nil, nil
}
func (*testConnPool) Close() error                                { return nil }
func (*testConnPool) ResetSession(ctx context.Context) error      { return nil }
func (*testConnPool) Ping(ctx context.Context) error              { return nil }
func (*testConnPool) Driver() driver.Driver                       { return nil }
func (*testConnPool) Conn(ctx context.Context) (*sql.Conn, error) { return nil, nil }
func (*testConnPool) SetMaxIdleConns(n int)                       {}
func (*testConnPool) SetMaxOpenConns(n int)                       {}
func (*testConnPool) SetConnMaxLifetime(d time.Duration)          {}
func (*testConnPool) SetConnMaxIdleTime(d time.Duration)          {}

type testSQLCaptureLogger struct {
	logger.Interface
	captured []string
}

func (l *testSQLCaptureLogger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	sqlStr, _ := fc()
	l.captured = append(l.captured, sqlStr)
}

func newDryRunRepo(t *testing.T) (*FeedRepository, *testSQLCaptureLogger) {
	t.Helper()
	capLogger := &testSQLCaptureLogger{}
	dryDB, err := gorm.Open(mysql.New(mysql.Config{
		Conn:                      &testConnPool{},
		SkipInitializeWithVersion: true,
	}), &gorm.Config{
		DryRun: true,
		Logger: capLogger,
	})
	if err != nil {
		t.Fatalf("open DryRun DB failed: %v", err)
	}
	return NewFeedRepository(dryDB), capLogger
}

func TestFeedRepository_ReadIsolation_SQLGeneration(t *testing.T) {
	ctx := context.Background()

	t.Run("ListLatest enforces status = 'published'", func(t *testing.T) {
		repo, capLogger := newDryRunRepo(t)
		_, _ = repo.ListLatest(ctx, 10, time.Time{})

		found := false
		for _, q := range capLogger.captured {
			if strings.Contains(q, "FROM `videos`") {
				found = true
				if !strings.Contains(q, "status") || !strings.Contains(q, "published") {
					t.Errorf("ListLatest SQL query missing status = 'published' constraint: %s", q)
				}
			}
		}
		if !found {
			t.Errorf("expected ListLatest query on videos table, got: %v", capLogger.captured)
		}
	})

	t.Run("ListByPopularity enforces status = 'published'", func(t *testing.T) {
		repo, capLogger := newDryRunRepo(t)
		_, _ = repo.ListByPopularity(ctx, 10, 0, time.Time{}, 0)

		found := false
		for _, q := range capLogger.captured {
			if strings.Contains(q, "FROM `videos`") {
				found = true
				if !strings.Contains(q, "status") || !strings.Contains(q, "published") {
					t.Errorf("ListByPopularity SQL query missing status = 'published' constraint: %s", q)
				}
			}
		}
		if !found {
			t.Errorf("expected ListByPopularity query on videos table, got: %v", capLogger.captured)
		}
	})

	t.Run("ListByTag enforces status = 'published'", func(t *testing.T) {
		repo, capLogger := newDryRunRepo(t)
		_, _ = repo.ListByTag(ctx, "gaming", 10)

		found := false
		for _, q := range capLogger.captured {
			if strings.Contains(q, "video_tags") {
				found = true
				if !strings.Contains(q, "status") || !strings.Contains(q, "published") {
					t.Errorf("ListByTag SQL query missing status = 'published' constraint: %s", q)
				}
			}
		}
		if !found {
			t.Errorf("expected ListByTag query on videos table, got: %v", capLogger.captured)
		}
	})

	t.Run("ListLikesCountWithCursor enforces status = 'published'", func(t *testing.T) {
		repo, capLogger := newDryRunRepo(t)
		_, _ = repo.ListLikesCountWithCursor(ctx, 10, nil)

		found := false
		for _, q := range capLogger.captured {
			if strings.Contains(q, "FROM `videos`") {
				found = true
				if !strings.Contains(q, "status") || !strings.Contains(q, "published") {
					t.Errorf("ListLikesCountWithCursor SQL query missing status = 'published' constraint: %s", q)
				}
			}
		}
		if !found {
			t.Errorf("expected ListLikesCountWithCursor query on videos table, got: %v", capLogger.captured)
		}
	})

	t.Run("ListByFollowing enforces status = 'published'", func(t *testing.T) {
		repo, capLogger := newDryRunRepo(t)
		_, _ = repo.ListByFollowing(ctx, 10, 100, time.Time{})

		found := false
		for _, q := range capLogger.captured {
			if strings.Contains(q, "FROM `videos`") {
				found = true
				if !strings.Contains(q, "status") || !strings.Contains(q, "published") {
					t.Errorf("ListByFollowing SQL query missing status = 'published' constraint: %s", q)
				}
			}
		}
		if !found {
			t.Errorf("expected ListByFollowing query on videos table, got: %v", capLogger.captured)
		}
	})

	t.Run("ListFreshLowEngagement enforces status = 'published' and prioritizes pool_level = 1", func(t *testing.T) {
		repo, capLogger := newDryRunRepo(t)
		_, _ = repo.ListFreshLowEngagement(ctx, 10, 24*time.Hour, 5, 0)

		found := false
		for _, q := range capLogger.captured {
			if strings.Contains(q, "FROM `videos`") {
				found = true
				if !strings.Contains(q, "status") || !strings.Contains(q, "published") {
					t.Errorf("ListFreshLowEngagement SQL query missing status = 'published' constraint: %s", q)
				}
				if !strings.Contains(q, "pool_level = 1") && !strings.Contains(q, "pool_level` = 1") && !strings.Contains(q, "pool_level` = ?") {
					t.Errorf("ListFreshLowEngagement SQL query missing pool_level = 1 prioritization in ORDER BY: %s", q)
				}
			}
		}
		if !found {
			t.Errorf("expected ListFreshLowEngagement query on videos table, got: %v", capLogger.captured)
		}
	})
}

func TestFeedIntegrationDBRequiresDSN(t *testing.T) {
	t.Setenv("TEST_MYSQL_DSN", "")
	var dialCount atomic.Int32
	mysqldriver.RegisterDialContext("tcp", func(context.Context, string) (net.Conn, error) {
		dialCount.Add(1)
		return nil, errors.New("unexpected MySQL connection without TEST_MYSQL_DSN")
	})
	t.Cleanup(func() { mysqldriver.DeregisterDialContext("tcp") })
	t.Run("unconfigured", func(t *testing.T) {
		openFeedIntegrationDB(t)
		t.Fatal("integration test must skip when TEST_MYSQL_DSN is unset")
	})
	if got := dialCount.Load(); got != 0 {
		t.Fatalf("attempted %d MySQL connections without TEST_MYSQL_DSN", got)
	}
}

func openFeedIntegrationDB(t *testing.T) *gorm.DB {
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
	return db
}

func TestFeedRepository_ReadIsolation_Integration(t *testing.T) {
	db := openFeedIntegrationDB(t)
	repo := NewFeedRepository(db)
	ctx := context.Background()

	now := time.Now()
	authorID := uint(999901)

	// 构造 3 条不同状态视频
	vPublished := &video.Video{
		AuthorID:   authorID,
		Username:   "test_iso_author",
		Title:      "published_video",
		PlayURL:    "https://cdn.example.com/pub.mp4",
		CoverURL:   "https://cdn.example.com/pub.jpg",
		Status:     video.VideoStatusPublished,
		PoolLevel:  video.PoolLevelSeed,
		CreateTime: now,
		Popularity: 0,
	}
	vAuditing := &video.Video{
		AuthorID:   authorID,
		Username:   "test_iso_author",
		Title:      "auditing_video",
		PlayURL:    "https://cdn.example.com/audit.mp4",
		CoverURL:   "https://cdn.example.com/audit.jpg",
		Status:     video.VideoStatusAuditing,
		PoolLevel:  video.PoolLevelSeed,
		CreateTime: now.Add(time.Second),
		Popularity: 100,
	}
	vRejected := &video.Video{
		AuthorID:     authorID,
		Username:     "test_iso_author",
		Title:        "rejected_video",
		PlayURL:      "https://cdn.example.com/rej.mp4",
		CoverURL:     "https://cdn.example.com/rej.jpg",
		Status:       video.VideoStatusRejected,
		RejectReason: "包含违规内容",
		PoolLevel:    video.PoolLevelSunk,
		CreateTime:   now.Add(2 * time.Second),
		Popularity:   200,
	}

	if err := db.Create(vPublished).Error; err != nil {
		t.Fatalf("create published video failed: %v", err)
	}
	t.Cleanup(func() { db.Delete(vPublished) })

	if err := db.Create(vAuditing).Error; err != nil {
		t.Fatalf("create auditing video failed: %v", err)
	}
	t.Cleanup(func() { db.Delete(vAuditing) })

	if err := db.Create(vRejected).Error; err != nil {
		t.Fatalf("create rejected video failed: %v", err)
	}
	t.Cleanup(func() { db.Delete(vRejected) })

	t.Run("ListLatest only returns published", func(t *testing.T) {
		videos, err := repo.ListLatest(ctx, 50, time.Time{})
		if err != nil {
			t.Fatalf("ListLatest failed: %v", err)
		}
		for _, v := range videos {
			if v.ID == vAuditing.ID || v.ID == vRejected.ID {
				t.Fatalf("ListLatest leaked unreviewed or rejected video: id=%d status=%s", v.ID, v.Status)
			}
		}
	})

	t.Run("ListByPopularity only returns published", func(t *testing.T) {
		videos, err := repo.ListByPopularity(ctx, 50, 0, time.Time{}, 0)
		if err != nil {
			t.Fatalf("ListByPopularity failed: %v", err)
		}
		for _, v := range videos {
			if v.ID == vAuditing.ID || v.ID == vRejected.ID {
				t.Fatalf("ListByPopularity leaked unreviewed or rejected video: id=%d status=%s", v.ID, v.Status)
			}
		}
	})

	t.Run("ListFreshLowEngagement only returns published and prioritizes seed", func(t *testing.T) {
		videos, err := repo.ListFreshLowEngagement(ctx, 50, 24*time.Hour, 10, 0)
		if err != nil {
			t.Fatalf("ListFreshLowEngagement failed: %v", err)
		}
		for _, v := range videos {
			if v.ID == vAuditing.ID || v.ID == vRejected.ID {
				t.Fatalf("ListFreshLowEngagement leaked unreviewed or rejected video: id=%d status=%s", v.ID, v.Status)
			}
		}
	})
}
