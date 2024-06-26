package iteration

import (
	"github.com/spirit-labs/tektite/common"
)

type ChainingIterator struct {
	iterators []SimplerIterator
	pos       int
}

func NewChainingIteratorFromSimple(its []SimplerIterator) *ChainingIterator {
	return &ChainingIterator{iterators: its}
}

func (c *ChainingIterator) Next() (bool, common.KV) {
	for ; c.pos < len(c.iterators); c.pos++ {
		valid, kv := c.iterators[c.pos].Next()
		if valid {
			return true, kv
		}
	}
	return false, common.KV{}
}

func (c *ChainingIterator) Close() {
}
