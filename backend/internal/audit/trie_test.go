package audit_test

import (
	"testing"

	"github.com/handlerzzy/feedsystem/internal/audit"
)

func TestTrieAuditor_Basic(t *testing.T) {
	auditor := audit.NewTrieAuditor([]string{"赌博", "色情", "badword"})

	t.Run("合规文本通过机审", func(t *testing.T) {
		pass, hitWord := auditor.AuditText("这是一个完全合规的技术教学视频")
		if !pass {
			t.Errorf("expected pass, got hitWord=%s", hitWord)
		}
		if hitWord != "" {
			t.Errorf("expected empty hitWord, got %s", hitWord)
		}
	})

	t.Run("包含违规敏感词被拦截", func(t *testing.T) {
		pass, hitWord := auditor.AuditText("这是一个包含赌博内容的视频")
		if pass {
			t.Error("expected violation, got pass")
		}
		if hitWord != "赌博" {
			t.Errorf("expected hitWord '赌博', got %s", hitWord)
		}
	})

	t.Run("英文敏感词命中", func(t *testing.T) {
		pass, hitWord := auditor.AuditText("This contains badword in sentence")
		if pass {
			t.Error("expected violation, got pass")
		}
		if hitWord != "badword" {
			t.Errorf("expected hitWord 'badword', got %s", hitWord)
		}
	})
}

func TestTrieAuditor_Robustness(t *testing.T) {
	auditor := audit.NewTrieAuditor([]string{"badword", "敏感词"})

	t.Run("大小写混淆拦截", func(t *testing.T) {
		cases := []string{"BADWORD", "BadWord", "bAdWoRd"}
		for _, c := range cases {
			pass, hitWord := auditor.AuditText("hello " + c + " world")
			if pass {
				t.Errorf("expected %s to be blocked", c)
			}
			if hitWord != "badword" {
				t.Errorf("expected hitWord 'badword', got %s", hitWord)
			}
		}
	})

	t.Run("空白符混淆拦截", func(t *testing.T) {
		cases := []string{"b a d w o r d", "b  a  d  w  o  r  d", "敏 感 词", "敏  感  词"}
		for _, c := range cases {
			pass, _ := auditor.AuditText(c)
			if pass {
				t.Errorf("expected %q to be blocked", c)
			}
		}
	})

	t.Run("特殊字符与标点符号混淆拦截", func(t *testing.T) {
		cases := []string{"b*a*d*w*o*r*d", "b-a-d_w-o-r-d", "敏*感*词", "敏@感#词", "敏---感___词"}
		for _, c := range cases {
			pass, _ := auditor.AuditText(c)
			if pass {
				t.Errorf("expected %q to be blocked", c)
			}
		}
	})
}

func TestTrieAuditor_DefaultAuditor(t *testing.T) {
	auditor := audit.NewDefaultTrieAuditor()
	pass, hit := auditor.AuditText("涉恐涉暴与枪支弹药违规")
	if pass {
		t.Error("expected default auditor to block sensitive words")
	}
	if hit == "" {
		t.Error("expected non-empty hit word")
	}
}

func BenchmarkTrieAuditor(b *testing.B) {
	auditor := audit.NewDefaultTrieAuditor()
	sampleText := "这是一个长文本标题用于评测性能：今天带大家体验最新的 Go 语言微服务架构，探索高效高并发 Feed 流系统的设计与实现！"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		auditor.AuditText(sampleText)
	}
}
