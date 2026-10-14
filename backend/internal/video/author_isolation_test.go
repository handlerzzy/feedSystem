package video_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/handlerzzy/feedsystem/internal/video"
	"github.com/handlerzzy/feedsystem/internal/video/mock"

	"github.com/gin-gonic/gin"
	"go.uber.org/mock/gomock"
)

// ---------- Seam 3: 作者主页作品列表访问者身份隔离测试 ----------

func TestVideoService_ListByAuthorID_IdentityIsolation(t *testing.T) {
	authorID := uint(1001)
	visitorID := uint(2002)

	vAuditing := video.Video{ID: 1, AuthorID: authorID, Title: "auditing", Status: video.VideoStatusAuditing}
	vPublished := video.Video{ID: 2, AuthorID: authorID, Title: "published", Status: video.VideoStatusPublished}
	vRejected := video.Video{ID: 3, AuthorID: authorID, Title: "rejected", Status: video.VideoStatusRejected}

	allVideos := []video.Video{vAuditing, vPublished, vRejected}
	publishedOnly := []video.Video{vPublished}

	t.Run("作者本人访问可见全部状态 (auditing/published/rejected)", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		store := mock.NewMockVideoStore(ctrl)
		svc := video.NewVideoService(store, nil, nil)

		store.EXPECT().
			ListByAuthorID(gomock.Any(), int64(authorID)).
			Return(allVideos, nil)

		got, err := svc.ListByAuthorID(context.Background(), authorID, authorID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 3 {
			t.Fatalf("author should see all 3 videos, got %d", len(got))
		}
	})

	t.Run("外部访客访问仅可见 published", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		store := mock.NewMockVideoStore(ctrl)
		svc := video.NewVideoService(store, nil, nil)

		store.EXPECT().
			ListPublishedByAuthorID(gomock.Any(), int64(authorID)).
			Return(publishedOnly, nil)

		got, err := svc.ListByAuthorID(context.Background(), authorID, visitorID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 1 || got[0].ID != 2 {
			t.Fatalf("external visitor should only see published video 2, got: %+v", got)
		}
	})

	t.Run("未登录游客 (viewerID=0) 仅可见 published", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		store := mock.NewMockVideoStore(ctrl)
		svc := video.NewVideoService(store, nil, nil)

		store.EXPECT().
			ListPublishedByAuthorID(gomock.Any(), int64(authorID)).
			Return(publishedOnly, nil)

		got, err := svc.ListByAuthorID(context.Background(), authorID, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 1 || got[0].ID != 2 {
			t.Fatalf("anonymous visitor should only see published video 2, got: %+v", got)
		}
	})

	t.Run("缺省 viewerID 参数时安全收敛至 published 列表", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		store := mock.NewMockVideoStore(ctrl)
		svc := video.NewVideoService(store, nil, nil)

		store.EXPECT().
			ListPublishedByAuthorID(gomock.Any(), int64(authorID)).
			Return(publishedOnly, nil)

		got, err := svc.ListByAuthorID(context.Background(), authorID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 1 || got[0].ID != 2 {
			t.Fatalf("default should only see published video 2, got: %+v", got)
		}
	})

	t.Run("authorID 为 0 时即便 viewerID 同为 0 仍严格防绕过收敛至 published", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		store := mock.NewMockVideoStore(ctrl)
		svc := video.NewVideoService(store, nil, nil)

		store.EXPECT().
			ListPublishedByAuthorID(gomock.Any(), int64(0)).
			Return([]video.Video{}, nil)

		got, err := svc.ListByAuthorID(context.Background(), 0, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("zero authorID should fail closed, got: %+v", got)
		}
	})
}

func TestVideoHandler_ListByAuthorID_HTTPIdentityIsolation(t *testing.T) {
	gin.SetMode(gin.TestMode)

	authorID := uint(1001)
	visitorID := uint(2002)

	vAuditing := video.Video{ID: 1, AuthorID: authorID, Title: "auditing", Status: video.VideoStatusAuditing}
	vPublished := video.Video{ID: 2, AuthorID: authorID, Title: "published", Status: video.VideoStatusPublished}
	vRejected := video.Video{ID: 3, AuthorID: authorID, Title: "rejected", Status: video.VideoStatusRejected}

	allVideos := []video.Video{vAuditing, vPublished, vRejected}
	publishedOnly := []video.Video{vPublished}

	t.Run("HTTP 作者本人查看返回全部状态", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		store := mock.NewMockVideoStore(ctrl)
		store.EXPECT().
			ListByAuthorID(gomock.Any(), int64(authorID)).
			Return(allVideos, nil)

		svc := video.NewVideoService(store, nil, nil)
		handler := video.NewVideoHandler(svc, nil)

		r := gin.New()
		r.POST("/video/listByAuthorID", func(c *gin.Context) {
			// 模拟 SoftJWTAuth 解析出作者本人身份
			c.Set("accountID", authorID)
			handler.ListByAuthorID(c)
		})

		body, _ := json.Marshal(video.ListByAuthorIDRequest{AuthorID: authorID})
		req := httptest.NewRequest(http.MethodPost, "/video/listByAuthorID", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Code)
		}
		var resp []video.Video
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal response failed: %v", err)
		}
		if len(resp) != 3 {
			t.Fatalf("author should see 3 videos in HTTP response, got %d", len(resp))
		}
	})

	t.Run("HTTP 外部访客查看仅返回 published", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		store := mock.NewMockVideoStore(ctrl)
		store.EXPECT().
			ListPublishedByAuthorID(gomock.Any(), int64(authorID)).
			Return(publishedOnly, nil)

		svc := video.NewVideoService(store, nil, nil)
		handler := video.NewVideoHandler(svc, nil)

		r := gin.New()
		r.POST("/video/listByAuthorID", func(c *gin.Context) {
			// 模拟 SoftJWTAuth 解析出外部访客身份
			c.Set("accountID", visitorID)
			handler.ListByAuthorID(c)
		})

		body, _ := json.Marshal(video.ListByAuthorIDRequest{AuthorID: authorID})
		req := httptest.NewRequest(http.MethodPost, "/video/listByAuthorID", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Code)
		}
		var resp []video.Video
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal response failed: %v", err)
		}
		if len(resp) != 1 || resp[0].ID != 2 {
			t.Fatalf("visitor should only see published video 2 in HTTP response, got %+v", resp)
		}
	})

	t.Run("HTTP 未登录匿名游客查看仅返回 published", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		store := mock.NewMockVideoStore(ctrl)
		store.EXPECT().
			ListPublishedByAuthorID(gomock.Any(), int64(authorID)).
			Return(publishedOnly, nil)

		svc := video.NewVideoService(store, nil, nil)
		handler := video.NewVideoHandler(svc, nil)

		r := gin.New()
		r.POST("/video/listByAuthorID", func(c *gin.Context) {
			// 未携带 token，无 accountID
			handler.ListByAuthorID(c)
		})

		body, _ := json.Marshal(video.ListByAuthorIDRequest{AuthorID: authorID})
		req := httptest.NewRequest(http.MethodPost, "/video/listByAuthorID", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Code)
		}
		var resp []video.Video
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal response failed: %v", err)
		}
		if len(resp) != 1 || resp[0].ID != 2 {
			t.Fatalf("anonymous visitor should only see published video 2 in HTTP response, got %+v", resp)
		}
	})
}
