package Flake

import (
	"github.com/sony/sonyflake"
	"log"
)

// 通过雪花算法生成一个全局唯一的ID
// TODO 因为协议层开始采用-1当做初始值，但后来改为uint64，所以把初始值改为零，这样可能出现问题，虽然概率不大
// TODO 简单的解决方案就是生成flake的时候判断一下是零就重新生成，后面有机会可以重写flake，
func GenSonyflake() uint64 {
	flake := sonyflake.NewSonyflake(sonyflake.Settings{})
	id, err := flake.NextID()
	if err != nil {
		log.Fatalf("flake.NextID() failed with %s\n", err)
	}
	return id
}