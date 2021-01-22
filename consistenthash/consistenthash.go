/*
Copyright 2013 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package consistenthash provides an implementation of a ring hash.
//一致性哈希算法，从单点走向分布式
package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

type Hash func(data []byte) uint32

type Map struct {
	hash     Hash
	replicas int   // (替代品) 虚拟节点数量
	keys     []int // Sorted
	// 虚拟节点key也就是hash值 到  真实节点key的映射
	// 真实的key 不会 入环
	hashMap map[int]string
}

func New(replicas int, fn Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash:     fn,
		hashMap:  make(map[int]string),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// IsEmpty returns true if there are no items available.
func (m *Map) IsEmpty() bool {
	return len(m.keys) == 0
}

// Add adds some keys to the hash.
//新增节点，每个真实节点会对应几个虚拟节点。保证环不会偏移
func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		for i := 0; i < m.replicas; i++ {
			hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
			m.keys = append(m.keys, hash) // 入环
			m.hashMap[hash] = key         // 虚拟节点key也就是hash值 到  真实节点key的映射
		}
	}
	//对 虚拟节点的哈希值（key）排序
	sort.Ints(m.keys)
}

// Get gets the closest item in the hash to the provided key.
func (m *Map) Get(key string) string {
	if m.IsEmpty() {
		return ""
	}

	// 1. 去当前key 的 哈希值
	hash := int(m.hash([]byte(key)))

	// Binary search for appropriate replica.
	// 2. 通过二分查找 找到最接近key 的 虚拟节点(哈希值接近)
	idx := sort.Search(len(m.keys), func(i int) bool { return m.keys[i] >= hash })

	// Means we have cycled back to the first replica.
	// 如果在链的末尾  就存到第一个，构成环状
	if idx == len(m.keys) {
		idx = 0
	}
	// 3. 通过 虚拟节点到真实节点的映射  找到真实节点，即要当前key要存储的节点
	return m.hashMap[m.keys[idx]]
}
