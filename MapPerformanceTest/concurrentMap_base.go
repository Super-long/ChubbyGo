package main

import (
	"ChubbyGo/BaseServer"
	"fmt"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
)

func main() {
	GoroutinueNumber := 1
	// 可选的,SyncMap或者ConcurrentMap
	entry := BaseServer.NewChubbyGoMap(BaseServer.SyncMap)


	wg := sync.WaitGroup{}
	wg.Add(GoroutinueNumber)
	var ErrorNumber uint32 = 0

	for j := 0; j < GoroutinueNumber; j++ {
		go func(number int) {
			for i := 0; i < 10; i++ {
				if i&1 == 0 {
					temp := "x " + strconv.Itoa(number) + " " + strconv.Itoa(i) + " y"
					entry.ChubbyGoMapSet(strconv.Itoa(number), temp)
				} else {
					res, ok := entry.ChubbyGoMapGet(strconv.Itoa(number))
					temp := "x " + strconv.Itoa(number) + " " + strconv.Itoa(i-1) + " y"
					if ok && res != temp{
						log.Printf("Error : res(%s) expected(%s).\n", res, temp)
						atomic.AddUint32(&ErrorNumber, 1)
					}
				}
			}
			wg.Done()
		}(j)
	}

	wg.Wait()

	if atomic.LoadUint32(&ErrorNumber) == 0{
		fmt.Println("PASS.")
	}
}