package main

import (
	"ChubbyGo/BaseServer"
	"strconv"
	"sync"
	"testing"
)

/*
 * @brief: 这个文件是为了对BaseMap, sync.map, concurrentMap, 以及ChubbyGoMap的两种类型
 * 的Put,Get,并发插入读取混合,删除四种操作分别做性能比较, 为客户提供不同场景下最佳的选择.
 */

/*
 * 循环次数;每次执行平均花费时间;每次执行堆上分配内存总数;每次执行堆上分配内存次数;
 */

const ThreadNumber = 10

/*1.--------------测试Put操作--------------*/

/*[1]--------------插入不存在的key--------------*/

// 在此测试中冲突键较多,对并发的map并不有利

//(BaseMap)
func BenchmarkSinglePutKeyNoExist_BaseMap(b *testing.B){
	Map := BaseServer.NewBaseMap()

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	b.ResetTimer()

	for j:=0; j < ThreadNumber; j++{
		go func(){
			for i := 0; i < b.N; i++ {
				Map.BaseStore(strconv.Itoa(i), "lizhaolong")
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

//(ConcurrentMap)
func BenchmarkSinglePutKeyNoExist_ConcurrentMap(b *testing.B){
	Map := BaseServer.NewConcurrentMap()

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	b.ResetTimer()

	for j:=0; j < ThreadNumber; j++{
		go func(){
			for i := 0; i < b.N; i++ {
				Map.Set(strconv.Itoa(i), "lizhaolong")
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

// (Sync.map)
func BenchmarkSinglePutKeyNoExist_SyncMap(b *testing.B){
	syncMap := &sync.Map{}

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	b.ResetTimer()

	for j:=0; j < ThreadNumber; j++{
		go func(){
			for i := 0; i < b.N; i++ {
				syncMap.Store(strconv.Itoa(i), "lizhaolong")
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

// (chubbyGoMap.ConcurrentMap)
func BenchmarkSinglePutKeyNoExist_ChubbyGo_ConcurrentMap(b *testing.B){
	chubbyGoMap := BaseServer.NewChubbyGoMap(BaseServer.ConcurrentMap)

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	b.ResetTimer()

	for j:=0; j < ThreadNumber; j++{
		go func(){
			for i := 0; i < b.N; i++ {
				chubbyGoMap.ChubbyGoMapSet(strconv.Itoa(i), "lizhaolong")
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

// (chubbyGoMap.Sync.map)
func BenchmarkSinglePutKeyNoExist_ChubbyGo_SyncMap(b *testing.B){
	chubbyGoMap := BaseServer.NewChubbyGoMap(BaseServer.SyncMap)

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	b.ResetTimer()

	for j:=0; j < ThreadNumber; j++{
		go func(){
			for i := 0; i < b.N; i++ {
				chubbyGoMap.ChubbyGoMapSet(strconv.Itoa(i), "lizhaolong")
			}
			groups.Done()
		}()
	}
	groups.Wait()
}


/*[1]--------------插入存在的key--------------*/