package main

import (
	"ChubbyGo/BaseServer"
	_ "ChubbyGo/Config"
	"ChubbyGo/Connect"
	"fmt"
	"log"
)

// 测试CAS操作的基础功能，分别为正常CAS，指定边界和跨度的递增和递减。

func main() {
	n := 10 // n大于零
	Sem := make(Connect.Semaphore, n-1)
	SemNumber := 0

	clientConfigs := make([]*Connect.ClientConfig, n)
	for i := 0; i < n; i++ {
		clientConfigs[i] = Connect.CreateClient()
		err := clientConfigs[i].StartClient()
		if err != nil {
			log.Println(err.Error())
		} else {
			clientConfigs[i].SetUniqueFlake(uint64(i + n*30))
		}
	}

	// 秒杀时一个标准的计数器创建方法
	clientConfigs[0].Put("lizhaolong", "8")

	log.Println("开始递减CAS操作.")

	// 有一个协程会运行失败
	for i := 1; i < n; i++ {
		SemNumber++
		go func(index int) {
			defer Sem.P(1)
			// 对键为"lizhaolong"的值在大于old(0)时进行递减，每次递减New(1)，
			ok := clientConfigs[index].CompareAndSwap("lizhaolong", 0, 1, BaseServer.Sub)
			if ok {
				fmt.Printf("递减 index[%d] CAS sucessful.\n",index)
			} else {
				fmt.Printf("递减 index[%d] CAS failture.\n",index)
			}
		}(i)
	}

	Sem.V(SemNumber)

	log.Println("开始递增CAS操作.")
	SemNumber = 0

	// 一个协程会递增失败
	for i := 1; i < n; i++ {
		SemNumber++
		go func(index int) {
			defer Sem.P(1)
			// 对键为"lizhaolong"的值在小于old(8)时进行递增，每次递增new(1)，
			ok := clientConfigs[index].CompareAndSwap("lizhaolong", 8, 1, BaseServer.Add)
			if ok {
				fmt.Printf("递增 index[%d] CAS sucessful.\n",index)
			} else {
				fmt.Printf("递增 index[%d] CAS failture.\n",index)
			}
		}(i)
	}

	Sem.V(SemNumber)

	log.Println("开始正常CAS操作.")

	SemNumber = 0

	// 只有一个线程可以执行成功
	for i := 1; i < n; i++{
		SemNumber++
		go func(index int) {
			defer Sem.P(1)
			// 如果旧值是old(8)，便替换为New(10)
			ok := clientConfigs[index].CompareAndSwap("lizhaolong", 8, 10, BaseServer.Cas)
			if ok {
				fmt.Printf("正常CAS index[%d] CAS sucessful.\n",index)
			} else {
				fmt.Printf("正常CAS index[%d] CAS failture.\n",index)
			}
		}(i)
	}

	Sem.V(SemNumber)

	res := clientConfigs[0].Get("lizhaolong")

	// 应该得到10
	fmt.Printf("res : %s.\n", res)
}