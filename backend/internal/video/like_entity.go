package video

import "time"

type Like struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	VideoID   uint      `gorm:"uniqueIndex:idx_like_video_account;not null" json:"video_id"`
	AccountID uint      `gorm:"uniqueIndex:idx_like_video_account;index:idx_like_account_created,priority:1;not null" json:"account_id"`
	CreatedAt time.Time `gorm:"index:idx_like_account_created,priority:2" json:"created_at"`
}

type LikeRequest struct {
	VideoID uint `json:"video_id"`
}
