/**
 * Copyright lizhaolong(https://github.com/Super-long)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* Code comment are all encoded in UTF-8.*/

/*
 * 这个代码的原型来自于: https://github.com/halfrost/Halfrost-Field/blob/master/contents/Go/go_map_bench_test/concurrent-map/concurrent_map.go
 * 我修改了其中的哈希算法;
 */

package BaseServer

import (
	"encoding/json"
	"github.com/xxhash"
	"sync"
	"unsafe"
)

/*
 * @notes: 重写一遍是为了防止环状引用
 */
func str2sbyte(s string) (b []byte) {
	*(*string)(unsafe.Pointer(&b)) = s                                                  // 把s的地址付给b
	*(*int)(unsafe.Pointer(uintptr(unsafe.Pointer(&b)) + 2*unsafe.Sizeof(&b))) = len(s) // 修改容量为长度
	return
}

// TODO 桶的数量应该可以自己配置,因为xxhash的性能很优秀,所以越大的数据越适合更多的桶
var SHARD_COUNT = 32


type ConcurrentHashMap struct {
	ThreadSafeHashMap []*ConcurrentMapShared
	NowKeyNumber uint64
	BucketNumber uint32
}


type ConcurrentMapShared struct {
	items        map[string]interface{}
	sync.RWMutex
}

/*
 * @brief: 创建一个新的map
 */
func NewConcurrentMap() *ConcurrentHashMap {
	Map := ConcurrentHashMap{}
	Map.ThreadSafeHashMap = make([]*ConcurrentMapShared, SHARD_COUNT)
	for i := 0; i < SHARD_COUNT; i++ {
		Map.ThreadSafeHashMap[i] = &ConcurrentMapShared{items: make(map[string]interface{})}
	}
	Map.NowKeyNumber = 0
	Map.BucketNumber = uint32(SHARD_COUNT)

	return &Map
}

/*
 * @brief: 利用key得到哈希表内的桶
 */
func (m ConcurrentHashMap) GetShard(key string) *ConcurrentMapShared {
	return m.ThreadSafeHashMap[uint(xxhash_key(key))%uint(m.BucketNumber)]
}

func (m ConcurrentHashMap) MSet(data map[string]interface{}) {
	for key, value := range data {
		shard := m.GetShard(key)
		shard.Lock()
		shard.items[key] = value
		shard.Unlock()
	}
}

// Sets the given value under the specified key.
func (m ConcurrentHashMap) Set(key string, value interface{}) {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Lock()
	shard.items[key] = value
	shard.Unlock()
}

// Callback to return new element to be inserted into the map
// It is called while lock is held, therefore it MUST NOT
// try to access other keys in same map, as it can lead to deadlock since
// Go sync.RWLock is not reentrant
// 回调返回待插入到 map 中的新元素
// 这个函数当且仅当在读写锁被锁定的时候才会被调用，因此一定不允许再去尝试读取同一个 map 中的其他 key 值。因为这样会导致线程死锁。死锁的原因是 Go 中 sync.RWLock 是不可重入的。
type UpsertCb func(exist bool, valueInMap interface{}, newValue interface{}) interface{}

// Insert or Update - updates existing element or inserts a new one using UpsertCb
func (m ConcurrentHashMap) Upsert(key string, value interface{}, cb UpsertCb) (res interface{}) {
	shard := m.GetShard(key)
	shard.Lock()
	v, ok := shard.items[key]
	res = cb(ok, v, value)
	shard.items[key] = res
	shard.Unlock()
	return res
}

// Sets the given value under the specified key if no value was associated with it.
func (m ConcurrentHashMap) SetIfAbsent(key string, value interface{}) bool {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Lock()
	_, ok := shard.items[key]
	if !ok {
		shard.items[key] = value
	}
	shard.Unlock()
	return !ok
}

// Retrieves an element from map under given key.
func (m ConcurrentHashMap) Get(key string) (interface{}, bool) {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// Get item from shard.
	val, ok := shard.items[key]
	shard.RUnlock()
	return val, ok
}

// Returns the number of elements within the map.
// 返回目前map中的数据总量
func (m ConcurrentHashMap) Count() uint64 {
	return m.NowKeyNumber
}

// Looks up an item under specified key
func (m ConcurrentHashMap) Has(key string) bool {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// See if element is within shard.
	_, ok := shard.items[key]
	shard.RUnlock()
	return ok
}

// Removes an element from the map.
func (m ConcurrentHashMap) Remove(key string) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	delete(shard.items, key)
	shard.Unlock()
}

// Removes an element from the map and returns it
func (m ConcurrentHashMap) Pop(key string) (v interface{}, exists bool) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	v, exists = shard.items[key]
	delete(shard.items, key)
	shard.Unlock()
	return v, exists
}

// Checks if map is empty.
func (m ConcurrentHashMap) IsEmpty() bool {
	return m.Count() == 0
}

// Used by the Iter & IterBuffered functions to wrap two variables together over a channel,
type Tuple struct {
	Key string
	Val interface{}
}

// Returns an iterator which could be used in a for range loop.
//
// Deprecated: using IterBuffered() will get a better performence
func (m ConcurrentHashMap) Iter() <-chan Tuple {
	chans := snapshot(m)
	ch := make(chan Tuple)
	go fanIn(chans, ch)
	return ch
}

// Returns a buffered iterator which could be used in a for range loop.
// 相当于所有的值都缓存在channel中
func (m ConcurrentHashMap) IterBuffered() <-chan Tuple {
	chans := snapshot(m)
	total := 0
	for _, c := range chans {
		total += cap(c)
	}
	ch := make(chan Tuple, total)
	go fanIn(chans, ch)
	return ch
}

// Returns a array of channels that contains elements in each shard,
// which likely takes a snapshot of `m`.
// It returns once the size of each buffered channel is determined,
// before all the channels are populated using goroutines.
// 返回值是SHARD_COUNT个缓冲channel
func snapshot(m ConcurrentHashMap) (chans []chan Tuple) {
	chans = make([]chan Tuple, SHARD_COUNT)
	wg := sync.WaitGroup{}
	wg.Add(SHARD_COUNT)
	// Foreach shard.
	for index, shard := range m.ThreadSafeHashMap {
		go func(index int, shard *ConcurrentMapShared) {
			// Foreach key, value pair.
			shard.RLock()
			chans[index] = make(chan Tuple, len(shard.items))
			wg.Done()
			for key, val := range shard.items {
				chans[index] <- Tuple{key, val}
			}
			shard.RUnlock()
			close(chans[index])
		}(index, shard)
	}
	wg.Wait()
	return chans
}

// fanIn reads elements from channels `chans` into channel `out`
func fanIn(chans []chan Tuple, out chan Tuple) {
	wg := sync.WaitGroup{}
	wg.Add(len(chans))
	for _, ch := range chans {
		go func(ch chan Tuple) {
			for t := range ch {
				out <- t
			}
			wg.Done()
		}(ch)
	}
	wg.Wait()
	close(out)
}

// Returns all items as map[string]interface{}
func (m ConcurrentHashMap) Items() map[string]interface{} {
	tmp := make(map[string]interface{})

	// Insert items to temporary map.
	for item := range m.IterBuffered() {
		tmp[item.Key] = item.Val
	}

	return tmp
}

// Iterator callback,called for every key,value found in
// maps. RLock is held for all calls for a given shard
// therefore callback sess consistent view of a shard,
// but not across the shards
type IterCb func(key string, v interface{})

// Callback based iterator, cheapest way to read
// all elements in a map.
// 传入一个回调函数，目前map中的每个值都会调用这个回调
func (m ConcurrentHashMap) IterCb(fn IterCb) {
	for idx := range m.ThreadSafeHashMap {
		shard := (m.ThreadSafeHashMap)[idx]
		shard.RLock()
		for key, value := range shard.items {
			fn(key, value)
		}
		shard.RUnlock()
	}
}

// Return all keys as []string
// 这个貌似不对 可能channel会阻塞，因为count结束以后可能会传入值，导致这里channel阻塞
func (m ConcurrentHashMap) Keys() []string {
	count := m.Count()
	ch := make(chan string, count)
	go func() {
		// 遍历所有的 shard.
		wg := sync.WaitGroup{}
		wg.Add(SHARD_COUNT)
		for _, shard := range m.ThreadSafeHashMap {
			go func(shard *ConcurrentMapShared) {
				// 遍历所有的 key, value 键值对.
				shard.RLock()
				for key := range shard.items {
					ch <- key
				}
				shard.RUnlock()
				wg.Done()
			}(shard)
		}
		wg.Wait()
		close(ch)
	}()

	// 生成 keys 数组，存储所有的 key
	keys := make([]string, 0, count)
	for k := range ch {
		keys = append(keys, k)
	}
	return keys
}

//Reviles ConcurrentMap "private" variables to json marshal.
// 可以用这个持久化
func (m ConcurrentHashMap) MarshalJSON() ([]byte, error) {
	// Create a temporary map, which will hold all item spread across shards.
	tmp := make(map[string]interface{})

	// Insert items to temporary map.
	for item := range m.IterBuffered() {
		tmp[item.Key] = item.Val
	}
	return json.Marshal(tmp)
}

/*
 * @param: 性能极其优异的哈希算法
 */
func xxhash_key(key string) uint32 {
	xxh := xxhash.New32()
	xxh.Write(str2sbyte(key))
	return xxh.Sum32()
}

/*
 * @brief: 用于与一般ChubbyGoMap作比较
 */
type BaseMap struct {
	sync.Mutex
	m map[string]interface{}
}

func NewBaseMap() *BaseMap{
	return &BaseMap{
		m: make(map[string]interface{}, 100),
	}
}

func (myMap *BaseMap) BaseStore(k string, v interface{}) {
	myMap.Lock()
	defer myMap.Unlock()
	myMap.m[k] = v
}

func (myMap *BaseMap) BaseGet(k string) interface{} {
	myMap.Lock()
	defer myMap.Unlock()
	if v, ok := myMap.m[k]; !ok {
		return -1
	} else {
		return v
	}
}

func (myMap *BaseMap) BaseDelete(k string) {
	myMap.Lock()
	defer myMap.Unlock()
	if _, ok := myMap.m[k]; !ok {
		return
	} else {
		delete(myMap.m, k)
	}
}
