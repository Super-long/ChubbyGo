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

package BaseServer

import (
	"log"
	"reflect"
	"sync"
)

/*
 * @brief: ChubbyGoConcurrentMap写成这样的原因是sync.map和concurrentMap各有优劣,用户可以根据不同的需求来配置
 * @notes: 有一个重要的问题就是反射的效率太低,可以在测试中看出ChubbyGo版本的比原版本效率低了不少,内存也消耗了不少
 */

const (
	SyncMap = iota
	ConcurrentMap
)

type ChubbyGoConcurrentMap struct {
	MapEntry reflect.Value
	Flag     uint32
}

func NewChubbyGoMap(flag uint32) *ChubbyGoConcurrentMap{
	entry := ChubbyGoConcurrentMap{}
	entry.Flag = flag

	if entry.Flag == SyncMap {
		entry.MapEntry = reflect.ValueOf(&sync.Map{}).Elem()
	} else if entry.Flag == ConcurrentMap {
		// NewConcurrentMap返回的是指针
		entry.MapEntry = reflect.ValueOf(NewConcurrentMap()).Elem()
	}

	return &entry
}

func (hs *ChubbyGoConcurrentMap) ChubbyGoMapGet(key string) (string, bool) {
	if hs.Flag == SyncMap {
		Map := hs.MapEntry.Addr().Interface().(*sync.Map)

		res, IsOk := Map.Load(key)
		if IsOk {
			return res.(string), true
		} else {
			return "", false
		}
	} else if hs.Flag == ConcurrentMap {
		Map := hs.MapEntry.Addr().Interface().(*ConcurrentHashMap)

		res, IsOk := Map.Get(key)
		if IsOk {
			return res.(string), true
		} else {
			return "", false
		}
	} else {
		log.Println("ERROR : ChubbyGoMapSet -> No such situation.")
		return "",false
	}
}

func (hs *ChubbyGoConcurrentMap) ChubbyGoMapSet(key string, value string) {
	// sync.map在拷贝以后失效
	if hs.Flag == SyncMap {

		Map := hs.MapEntry.Addr().Interface().(*sync.Map)
		Map.Store(key, value)

	} else if hs.Flag == ConcurrentMap {

		Map := hs.MapEntry.Addr().Interface().(*ConcurrentHashMap)
		Map.Set(key, value)

	} else {
		log.Println("ERROR : ChubbyGoMapSet -> No such situation.")
	}
}