package BaseServer

import (
	"log"
	"sync"
)

/*
 * @brief: ChubbyGoConcurrentMap写成这样的原因是sync.map和concurrentMap各有优劣,用户可以根据不同的需求来配置
 */

const (
	SyncMap = iota
	ConcurrentMap
)

type ChubbyGoConcurrentMap struct {
	MapEntry interface{}
	Flag     uint32
}

func NewChubbyGoMap(flag uint32) *ChubbyGoConcurrentMap{
	entry := ChubbyGoConcurrentMap{}
	entry.Flag = flag

	if entry.Flag == SyncMap {
		entry.MapEntry = sync.Map{}
	} else if entry.Flag == ConcurrentMap {
		entry.MapEntry = NewConcurrentMap()
	}

	return &entry
}

func (hs *ChubbyGoConcurrentMap) ChubbyGoMapGet(key string) (string, bool) {
	if hs.Flag == SyncMap {
		Map, ok := hs.MapEntry.(sync.Map)
		if ok {
			res, IsOk := Map.Load(key)
			if IsOk {
				return res.(string), true
			} else {
				return "", false
			}
		} else {
			log.Println("INFO : ChubbyGoMapGet predicate failture.")
			return "", false
		}
	} else if hs.Flag == ConcurrentMap {
		Map, ok := hs.MapEntry.(ConcurrentHashMap)
		if ok {
			res, IsOk := Map.Get(key)
			if IsOk {
				return res.(string), true
			} else {
				return "", false
			}
		} else {
			log.Println("INFO : ChubbyGoMapGet predicate failture.")
			return "", false
		}
	} else {
		log.Println("ERROR : ChubbyGoMapSet -> No such situation.")
		return "", false
	}
}

func (hs *ChubbyGoConcurrentMap) ChubbyGoMapSet(key string, value string) {
	if hs.Flag == SyncMap {
		Map, ok := hs.MapEntry.(sync.Map)
		if ok {
			Map.Store(key, value)
		} else {
			log.Println("INFO : ChubbyGoMapSet predicate failture.")
		}
		hs.MapEntry = Map
	} else if hs.Flag == ConcurrentMap {
		Map, ok := hs.MapEntry.(ConcurrentHashMap)
		if ok {
			Map.Set(key, value)
		} else {
			log.Println("INFO : ChubbyGoMapSet predicate failture.")
		}
		hs.MapEntry = Map
	} else {
		log.Println("ERROR : ChubbyGoMapSet -> No such situation.")
	}
}