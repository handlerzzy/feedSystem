package video

import "time"

type VideoStatus string

const (
	VideoStatusAuditing  VideoStatus = "auditing"
	VideoStatusPublished VideoStatus = "published"
	VideoStatusRejected  VideoStatus = "rejected"
)

type PoolLevel int

const (
	PoolLevelSunk     PoolLevel = 0 // 沉底池 / 退出探索 (Sunk Pool)
	PoolLevelSeed     PoolLevel = 1 // 种子池 / 冷启动扶持 (Seed Pool)
	PoolLevelAdvanced PoolLevel = 2 // 进阶池 / 热门候选 (Advanced Pool)

	// 向后兼容别名 (Deprecated: use PoolLevelSunk instead)
	PoolLevelSettled = PoolLevelSunk
)

const (
	OutboxEventTimeline = "timeline"
)

type Video struct {
	ID                uint        `gorm:"primaryKey;index:idx_videos_likes_count_id,priority:2,sort:desc;index:idx_videos_popularity_time_id,priority:3,sort:desc" json:"id"`
	AuthorID          uint        `gorm:"index;index:idx_videos_author_time,priority:1;not null" json:"author_id"`
	Username          string      `gorm:"type:varchar(255);not null" json:"username"`
	Title             string      `gorm:"type:varchar(255);not null" json:"title"`
	Description       string      `gorm:"type:varchar(255);" json:"description,omitempty"`
	PlayURL           string      `gorm:"type:varchar(255);not null" json:"play_url"`
	CoverURL          string      `gorm:"type:varchar(255);not null" json:"cover_url"`
	CreateTime        time.Time   `gorm:"autoCreateTime;index:idx_videos_create_time,sort:desc;index:idx_videos_popularity_time_id,priority:2,sort:desc;index:idx_videos_author_time,priority:2,sort:desc" json:"create_time"`
	UpdateTime        time.Time   `gorm:"autoUpdateTime;index:idx_videos_update_time,sort:desc" json:"update_time"`
	LikesCount        int64       `gorm:"column:likes_count;not null;default:0;index:idx_videos_likes_count_id,priority:1,sort:desc" json:"likes_count"`
	Popularity        int64       `gorm:"column:popularity;not null;default:0;index:idx_videos_popularity_time_id,priority:1,sort:desc" json:"popularity"`
	Status            VideoStatus `gorm:"type:varchar(32);not null;default:'published';index:idx_videos_pool_status,priority:2" json:"status"`
	PoolLevel         PoolLevel   `gorm:"column:pool_level;not null;index:idx_videos_pool_status,priority:1" json:"pool_level"`
	RejectReason      string      `gorm:"type:varchar(255);" json:"reject_reason,omitempty"`
	RacingImpressions int64       `gorm:"-" json:"racing_impressions"`
}

type PublishVideoRequest struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	PlayURL     string `json:"play_url"`
	CoverURL    string `json:"cover_url"`
}

type ListByAuthorIDRequest struct {
	AuthorID uint `json:"author_id"`
}

type GetDetailRequest struct {
	ID uint `json:"id"`
}

type OutboxMsg struct {
	ID         uint      `gorm:"primaryKey"`
	VideoID    uint      `gorm:"index"`
	EventType  string    `gorm:"type:varchar(50);index:idx_outbox_msgs_status_type_time,priority:2"`
	CreateTime time.Time `gorm:"autoCreateTime;index:idx_outbox_msgs_status_type_time,priority:3,sort:asc"`
	Status     string    `gorm:"type:varchar(50);index:idx_outbox_msgs_status_type_time,priority:1"`
}

