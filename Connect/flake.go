package Connect

import (
	"github.com/sony/sonyflake"
	"log"
)

// 通过雪花算法生成一个全局唯一的ID
func GenSonyflake() uint64 {
	flake := sonyflake.NewSonyflake(sonyflake.Settings{})
	id, err := flake.NextID()
	if err != nil {
		log.Fatalf("flake.NextID() failed with %s\n", err)
	}
	return id
}