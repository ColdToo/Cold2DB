package wal

import (
	"fmt"
	"testing"
)

func TestSegmentFileName(t *testing.T) {
	var id int64
	segmentName := SegmentFileName("tmp", ".SEG", 1)
	_, err := fmt.Sscanf(segmentName, "%d.SEG", &id)
	if err != nil {
		fmt.Println("提取失败:", err)
	}
	fmt.Println(segmentName)
	fmt.Println(id)
}
