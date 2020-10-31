package main

import (
	"ChubbyGo/BaseServer"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
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

// 在此测试中冲突键较多,对并发的map并不有利

//(BaseMap)
func BenchmarkPutKeyNoExist_BaseMap(b *testing.B){
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
func BenchmarkPutKeyNoExist_ConcurrentMap(b *testing.B){
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
func BenchmarkPutKeyNoExist_SyncMap(b *testing.B){
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
func BenchmarkPutKeyNoExist_ChubbyGo_ConcurrentMap(b *testing.B){
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
func BenchmarkPutKeyNoExist_ChubbyGo_SyncMap(b *testing.B){
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

/*2.--------------测试Get操作--------------*/
// 读取一个存在的key

//(BaseMap)
func BenchmarkGetKey_BaseMap(b *testing.B){
	Map := BaseServer.NewBaseMap()

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	rand.Seed(time.Now().Unix())
	// 可能产生(64,128)个键
	KeyNumber := rand.Intn(63)
	KeyNumber += 64
	for i:=0; i<KeyNumber; i++{
		TempWord := "x " + strconv.Itoa(i) + " y"
		Map.BaseStore(TempWord, "lizhaolong")
	}

	b.ResetTimer()

	for j:=0; j < ThreadNumber; j++{
		go func(){
			for i := 0; i < b.N; i++ {
				TempWord := "x " + strconv.Itoa(i%KeyNumber) + " y"
				Map.BaseGet(TempWord)
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

//(ConcurrentMap)
func BenchmarkGetKey_ConcurrentMap(b *testing.B){
	Map := BaseServer.NewConcurrentMap()

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	rand.Seed(time.Now().Unix())
	// 可能产生(64,128)个键
	KeyNumber := rand.Intn(63)
	KeyNumber += 64
	for i:=0; i<KeyNumber; i++{
		TempWord := "x " + strconv.Itoa(i) + " y"
		Map.Set(TempWord, "lizhaolong")
	}

	b.ResetTimer()

	for j:=0; j < ThreadNumber; j++{
		go func(){
			for i := 0; i < b.N; i++ {
				TempWord := "x " + strconv.Itoa(i%KeyNumber) + " y"
				Map.Get(TempWord)
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

// (Sync.map)
func BenchmarkGetKey_SyncMap(b *testing.B){
	syncMap := &sync.Map{}

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	rand.Seed(time.Now().Unix())
	// 可能产生(64,128)个键
	KeyNumber := rand.Intn(63)
	KeyNumber += 64
	for i:=0; i<KeyNumber; i++{
		TempWord := "x " + strconv.Itoa(i) + " y"
		syncMap.Store(TempWord, "lizhaolong")
	}

	b.ResetTimer()

	for j:=0; j < ThreadNumber; j++{
		go func(){
			for i := 0; i < b.N; i++ {
				TempWord := "x " + strconv.Itoa(i%KeyNumber) + " y"
				syncMap.Load(TempWord)
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

// (chubbyGoMap.ConcurrentMap)
func BenchmarkGetKey_ChubbyGo_ConcurrentMap(b *testing.B){
	chubbyGoMap := BaseServer.NewChubbyGoMap(BaseServer.ConcurrentMap)

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	rand.Seed(time.Now().Unix())
	// 可能产生(64,128)个键
	KeyNumber := rand.Intn(63)
	KeyNumber += 64
	for i:=0; i<KeyNumber; i++{
		TempWord := "x " + strconv.Itoa(i) + " y"
		chubbyGoMap.ChubbyGoMapSet(TempWord, "lizhaolong")
	}

	b.ResetTimer()

	for j:=0; j < ThreadNumber; j++{
		go func(){
			for i := 0; i < b.N; i++ {
				TempWord := "x " + strconv.Itoa(i%KeyNumber) + " y"
				chubbyGoMap.ChubbyGoMapGet(TempWord)
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

// (chubbyGoMap.Sync.map)
func BenchmarkGetKey_ChubbyGo_SyncMap(b *testing.B){
	chubbyGoMap := BaseServer.NewChubbyGoMap(BaseServer.SyncMap)

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)
	rand.Seed(time.Now().Unix())
	// 可能产生(64,128)个键
	KeyNumber := rand.Intn(63)
	KeyNumber += 64
	for i:=0; i<KeyNumber; i++{
		TempWord := "x " + strconv.Itoa(i) + " y"
		chubbyGoMap.ChubbyGoMapSet(TempWord, "lizhaolong")
	}

	b.ResetTimer()

	for j:=0; j < ThreadNumber; j++{
		go func(){
			for i := 0; i < b.N; i++ {
				TempWord := "x " + strconv.Itoa(i%KeyNumber) + " y"
				chubbyGoMap.ChubbyGoMapGet(TempWord)
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

/*3.--------------测试并发Get,put操作--------------*/

//(BaseMap)
func BenchmarkPutGet_BaseMap(b *testing.B){
	Map := BaseServer.NewBaseMap()

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	b.ResetTimer()

	for j:=0; j < ThreadNumber; j++{
		go func(){
			for i := 0; i < b.N; i++ {
				Map.BaseStore(strconv.Itoa(i), "lizhaolong")
				Map.BaseGet(strconv.Itoa(i-1))
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

//(ConcurrentMap)
func BenchmarkPutGet_ConcurrentMap(b *testing.B){
	Map := BaseServer.NewConcurrentMap()

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	b.ResetTimer()

	for j:=0; j < ThreadNumber; j++{
		go func(){
			for i := 0; i < b.N; i++ {
				Map.Set(strconv.Itoa(i), "lizhaolong")
				Map.Get(strconv.Itoa(i-1))
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

// (Sync.map)
func BenchmarkPutGet_SyncMap(b *testing.B){
	syncMap := &sync.Map{}

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	b.ResetTimer()

	for j:=0; j < ThreadNumber; j++{
		go func(){
			for i := 0; i < b.N; i++ {
				syncMap.Store(strconv.Itoa(i), "lizhaolong")
				syncMap.Load(strconv.Itoa(i-1))
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

// (chubbyGoMap.ConcurrentMap)
func BenchmarkPutGet_ChubbyGo_ConcurrentMap(b *testing.B){
	chubbyGoMap := BaseServer.NewChubbyGoMap(BaseServer.ConcurrentMap)

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	b.ResetTimer()

	for j:=0; j < ThreadNumber; j++{
		go func(){
			for i := 0; i < b.N; i++ {
				chubbyGoMap.ChubbyGoMapSet(strconv.Itoa(i), "lizhaolong")
				chubbyGoMap.ChubbyGoMapGet(strconv.Itoa(i-1))
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

// (chubbyGoMap.Sync.map)
func BenchmarkPutGet_ChubbyGo_SyncMap(b *testing.B){
	chubbyGoMap := BaseServer.NewChubbyGoMap(BaseServer.SyncMap)

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	b.ResetTimer()

	for j:=0; j < ThreadNumber; j++{
		go func(){
			for i := 0; i < b.N; i++ {
				chubbyGoMap.ChubbyGoMapSet(strconv.Itoa(i), "lizhaolong")
				chubbyGoMap.ChubbyGoMapGet(strconv.Itoa(i-1))
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

/*3.--------------测试并发Delete操作--------------*/

func BenchmarkDelete_BaseMap(b *testing.B) {
	Map := BaseServer.NewBaseMap()

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	b.ResetTimer()

	for j:=0; j < ThreadNumber; j++{
		go func(){
			for i := 0; i < b.N; i++ {
				Map.BaseStore(strconv.Itoa(i), "lizhaolong")
				Map.BaseDelete(strconv.Itoa(i-1))
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

//(ConcurrentMap)
func BenchmarkDelete_ConcurrentMap(b *testing.B){
	Map := BaseServer.NewConcurrentMap()

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	b.ResetTimer()

	for j:=0; j < ThreadNumber; j++{
		go func(){
			for i := 0; i < b.N; i++ {
				Map.Set(strconv.Itoa(i), "lizhaolong")
				Map.Remove(strconv.Itoa(i-1))
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

// (Sync.map)
func BenchmarkDelete_SyncMap(b *testing.B){
	syncMap := &sync.Map{}

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	b.ResetTimer()

	for j:=0; j < ThreadNumber; j++{
		go func(){
			for i := 0; i < b.N; i++ {
				syncMap.Store(strconv.Itoa(i), "lizhaolong")
				syncMap.Delete(strconv.Itoa(i-1))
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

// (chubbyGoMap.ConcurrentMap)
func BenchmarkDelete_ChubbyGo_ConcurrentMap(b *testing.B){
	chubbyGoMap := BaseServer.NewChubbyGoMap(BaseServer.ConcurrentMap)

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	b.ResetTimer()

	for j:=0; j < ThreadNumber; j++{
		go func(){
			for i := 0; i < b.N; i++ {
				chubbyGoMap.ChubbyGoMapSet(strconv.Itoa(i), "lizhaolong")
				chubbyGoMap.ChubbyGoMapDelete(strconv.Itoa(i-1))
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

// (chubbyGoMap.Sync.map)
func BenchmarkDelete_ChubbyGo_SyncMap(b *testing.B){
	chubbyGoMap := BaseServer.NewChubbyGoMap(BaseServer.SyncMap)

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	b.ResetTimer()

	for j:=0; j < ThreadNumber; j++{
		go func(){
			for i := 0; i < b.N; i++ {
				chubbyGoMap.ChubbyGoMapSet(strconv.Itoa(i), "lizhaolong")
				chubbyGoMap.ChubbyGoMapDelete(strconv.Itoa(i-1))
			}
			groups.Done()
		}()
	}
	groups.Wait()
}