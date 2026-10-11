package video_test

import (
	"context"
	"errors"
	"testing"

	"github.com/handlerzzy/feedsystem/internal/video"
	"github.com/handlerzzy/feedsystem/internal/video/mock"
	"go.uber.org/mock/gomock"
)

// 本文件守 P2 视频发布异步触发即时向量化的契约：
// 1. 视频成功发布落库后，即时异步触发单条向量计算并同步入池。
// 2. 向量化触发失败绝不影响视频发布主流程（best-effort 隔离）。
// 3. DB 写库失败时绝不触发向量化。
// 4. 未装配触发器时静默跳过。

func TestPublishTriggersAsyncEmbedding(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mock.NewMockVideoStore(ctrl)
	store.EXPECT().PublishWithOutbox(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, v *video.Video) error {
			v.ID = 42
			return nil
		})

	trigger := mock.NewMockEmbeddingTrigger(ctrl)
	trigger.EXPECT().TriggerEmbedding(gomock.Any(), uint(42)).Return(nil)

	svc := video.NewVideoService(store, nil, nil).WithEmbeddingTrigger(trigger)
	v := &video.Video{AuthorID: 1, Title: "标题", PlayURL: "http://x/p.mp4", CoverURL: "http://x/c.jpg"}
	if err := svc.Publish(context.Background(), v); err != nil {
		t.Fatalf("发布失败: %v", err)
	}
}

func TestPublishSucceedsWhenEmbeddingTriggerFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mock.NewMockVideoStore(ctrl)
	store.EXPECT().PublishWithOutbox(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, v *video.Video) error {
			v.ID = 7
			return nil
		})

	trigger := mock.NewMockEmbeddingTrigger(ctrl)
	trigger.EXPECT().TriggerEmbedding(gomock.Any(), uint(7)).Return(errors.New("trigger failed"))

	svc := video.NewVideoService(store, nil, nil).WithEmbeddingTrigger(trigger)
	v := &video.Video{AuthorID: 1, Title: "标题", PlayURL: "http://x/p.mp4", CoverURL: "http://x/c.jpg"}
	if err := svc.Publish(context.Background(), v); err != nil {
		t.Fatalf("向量触发失败绝不应导致视频发布失败，实际: %v", err)
	}
}

func TestPublishDoesNotTriggerEmbeddingWhenDBWriteFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mock.NewMockVideoStore(ctrl)
	store.EXPECT().PublishWithOutbox(gomock.Any(), gomock.Any()).Return(errors.New("db down"))

	trigger := mock.NewMockEmbeddingTrigger(ctrl)
	// trigger 不应被调用

	svc := video.NewVideoService(store, nil, nil).WithEmbeddingTrigger(trigger)
	v := &video.Video{AuthorID: 1, Title: "标题", PlayURL: "http://x/p.mp4", CoverURL: "http://x/c.jpg"}
	if err := svc.Publish(context.Background(), v); err == nil {
		t.Fatal("DB 写库失败时 Publish 必须返回错误")
	}
}

func TestPublishDoesNotCallEmbeddingWhenNotWired(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mock.NewMockVideoStore(ctrl)
	store.EXPECT().PublishWithOutbox(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, v *video.Video) error {
			v.ID = 10
			return nil
		})

	svc := video.NewVideoService(store, nil, nil) // 未注入 WithEmbeddingTrigger
	v := &video.Video{AuthorID: 1, Title: "标题", PlayURL: "http://x/p.mp4", CoverURL: "http://x/c.jpg"}
	if err := svc.Publish(context.Background(), v); err != nil {
		t.Fatalf("发布失败: %v", err)
	}
}
