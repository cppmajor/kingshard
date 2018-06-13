/*
LRUCache is a simple LRU cache. It is based on the LRU implementation in groupcache:
https://github.com/golang/groupcache/tree/master/lru
*/

package sqlmonitor

import "container/list"
import (
	"sync"
)

// LRUCache is an LRU cache. It is not safe for concurrent access.
type LRUCache struct {
	// MaxEntries is the maximum number of cache entries before
	// an item is evicted. Zero means no limit.
	MaxEntries int

	// OnEvicted optionally specificies a callback function to be
	// executed when an entry is purged from the cache.
	OnEvicted func(key Key, value interface{})

	// OnBeforeAdded optionally specificies a callback function to be
	// executed when an entry is added to the cache.
	OnValueUpdate func(key Key, valueOld interface{}, valueNew interface{}) interface{}

	lock  sync.RWMutex
	ll    *list.List
	cache map[interface{}]*list.Element
}

// A Key may be any value that is comparable. See http://golang.org/ref/spec#Comparison_operators
type Key interface{}

type entry struct {
	key   Key
	value interface{}
}

// New creates a new LRUCache.
// If maxEntries is zero, the cache has no limit and it's assumed
// that eviction is done by the caller.
func NewLRUCache(maxEntries int) *LRUCache {
	return &LRUCache{
		MaxEntries: maxEntries,
		ll:         list.New(),
		cache:      make(map[interface{}]*list.Element),
	}
}

// Update MaxEntries
func (c *LRUCache) SetMaxEntries(maxEntries int) {
	cutSize := c.Len() - maxEntries

	c.MaxEntries = maxEntries
	if cutSize > 0 {
		for i := 0; i < cutSize; i++ {
			c.RemoveOldest()
		}
	}
}

// Add adds a value to the cache.
func (c *LRUCache) Add(key Key, value interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.cache == nil {
		c.cache = make(map[interface{}]*list.Element)
		c.ll = list.New()
	}
	if ee, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ee)
		if nil == c.OnValueUpdate {
			ee.Value.(*entry).value = value
		} else {
			ee.Value.(*entry).value = c.OnValueUpdate(key, ee.Value.(*entry).value, value)
		}
		return
	}

	if nil != c.OnValueUpdate {
		value = c.OnValueUpdate(key, nil, value)
	}

	ele := c.ll.PushFront(&entry{key, value})
	c.cache[key] = ele
	if c.MaxEntries != 0 && c.ll.Len() > c.MaxEntries {
		ele := c.ll.Back()
		if ele != nil {
			c.removeElement(ele)
		}
	}
}

// Get looks up a key's value from the cache.
func (c *LRUCache) Get(key Key) (value interface{}, ok bool) {
	if c.cache == nil {
		return
	}

	c.lock.RLock()
	defer c.lock.RUnlock()
	if ele, hit := c.cache[key]; hit {
		//c.ll.MoveToFront(ele)
		return ele.Value.(*entry).value, true
	}
	return
}

// Remove removes the provided key from the cache.
func (c *LRUCache) Remove(key Key) {
	if c.cache == nil {
		return
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	if ele, hit := c.cache[key]; hit {
		c.removeElement(ele)
	}
}

// RemoveOldest removes the oldest item from the cache.
func (c *LRUCache) RemoveOldest() interface{} {
	if c.cache == nil {
		return nil
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	ele := c.ll.Back()
	if ele != nil {
		c.removeElement(ele)
		return ele.Value.(*entry).value
	}

	return nil
}

func (c *LRUCache) removeElement(e *list.Element) {
	c.ll.Remove(e)
	kv := e.Value.(*entry)
	delete(c.cache, kv.key)
	if c.OnEvicted != nil {
		c.OnEvicted(kv.key, kv.value)
	}
}

// Len returns the number of items in the cache.
func (c *LRUCache) Len() int {
	if c.cache == nil {
		return 0
	}

	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.ll.Len()
}

// Clear purges all stored items from the cache.
func (c *LRUCache) Clear() {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.OnEvicted != nil {
		for _, e := range c.cache {
			kv := e.Value.(*entry)
			c.OnEvicted(kv.key, kv.value)
		}
	}
	c.ll = nil
	c.cache = nil
}

func (c *LRUCache) GetVals(limit int, offset int) (vals []interface{}) {
	if c.cache == nil {
		return
	}

	elem := c.getElementAt(offset)
	if nil == elem {
		return
	}

	for i := 0; i < limit && nil != elem; i++ {
		vals = append(vals, elem.Value.(*entry).value)
		elem = elem.Next()
	}

	return vals
}

func (c *LRUCache) getElementAt(index int) *list.Element {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if index >= c.ll.Len() {
		return nil
	}

	if index > c.ll.Len()/2 {
		i := 0
		reverseIndex := c.ll.Len() - index - 1
		for val := c.ll.Back(); nil != val; val = val.Prev() {
			i++
			if i <= reverseIndex {
				continue
			}

			return val
		}
	} else {
		i := 0
		for val := c.ll.Front(); nil != val; val = val.Next() {
			i++
			if i <= index {
				continue
			}

			return val
		}
	}

	return nil
}
