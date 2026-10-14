package audit

import (
	"strings"
	"unicode"
)

// ContentAuditor 定义了内容机审的前置拦截契约。
type ContentAuditor interface {
	AuditText(text string) (pass bool, hitWord string)
}

// DefaultSensitiveWords 预置的基础敏感词库。
var DefaultSensitiveWords = []string{
	"赌博",
	"色情",
	"毒品",
	"暴恐",
	"枪支",
	"违规敏感词",
	"badword",
	"gambling",
}

type trieNode struct {
	children map[rune]*trieNode
	isEnd    bool
	word     string
}

func newTrieNode() *trieNode {
	return &trieNode{
		children: make(map[rune]*trieNode),
	}
}

// TrieAuditor 基于内存前缀树的敏感词机审实现。
// 满足单次检测耗时 <0.2ms，支持大小写混淆、空白符与特殊字符间隔混淆过滤。
type TrieAuditor struct {
	root *trieNode
}

// NewTrieAuditor 根据指定的敏感词列表构建 TrieAuditor。
func NewTrieAuditor(words []string) *TrieAuditor {
	auditor := &TrieAuditor{
		root: newTrieNode(),
	}
	for _, word := range words {
		auditor.insert(word)
	}
	return auditor
}

// NewDefaultTrieAuditor 构建包含默认敏感词库的 TrieAuditor。
func NewDefaultTrieAuditor() *TrieAuditor {
	return NewTrieAuditor(DefaultSensitiveWords)
}

func (a *TrieAuditor) insert(word string) {
	word = strings.TrimSpace(word)
	if word == "" {
		return
	}
	curr := a.root
	for _, r := range word {
		// 敏感词构建阶段统一转为小写，且跳过标点和空白
		if isNoise(r) {
			continue
		}
		rLower := unicode.ToLower(r)
		child, exists := curr.children[rLower]
		if !exists {
			child = newTrieNode()
			curr.children[rLower] = child
		}
		curr = child
	}
	curr.isEnd = true
	curr.word = word
}

// AuditText 检测给定文本是否包含敏感词。
// 返回 pass=true 表示合规通过；pass=false 表示违规，hitWord 为命中的敏感词。
func (a *TrieAuditor) AuditText(text string) (bool, string) {
	if a == nil || a.root == nil || text == "" {
		return true, ""
	}

	runes := []rune(text)
	n := len(runes)

	for i := 0; i < n; i++ {
		if isNoise(runes[i]) {
			continue
		}

		curr := a.root
		for j := i; j < n; j++ {
			r := runes[j]
			if isNoise(r) {
				continue
			}

			child, match := curr.children[unicode.ToLower(r)]
			if !match {
				break
			}

			curr = child
			if curr.isEnd {
				return false, curr.word
			}
		}
	}

	return true, ""
}

// isNoise 判断是否为空白符、标点符号或混淆特殊字符。
func isNoise(r rune) bool {
	return unicode.IsSpace(r) || unicode.IsPunct(r) || unicode.IsSymbol(r)
}
