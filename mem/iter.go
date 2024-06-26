package mem

import (
	"bytes"
	"github.com/spirit-labs/tektite/arenaskl"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/iteration"
)

type MemtableIterator struct {
	it                         *arenaskl.Iterator
	prevIt                     *arenaskl.Iterator
	keyStart                   []byte
	keyEnd                     []byte
	initialSeek                bool
	hasIterReturnedFirstResult bool
}

func (m *Memtable) NewIterator(keyStart []byte, keyEnd []byte) iteration.SimplerIterator {
	var it arenaskl.Iterator
	it.Init(m.sl)
	iter := &MemtableIterator{
		it:       &it,
		keyStart: keyStart,
		keyEnd:   keyEnd,
	}
	iter.doInitialSeek()
	return iter
}

func (m *MemtableIterator) copyAndForwardIter() {
	// we make a copy of the iter before advancing in case we advance off the end (invalid) and later // more records arrive
	prevCopy := *m.it
	m.it.Next()
	m.prevIt = &prevCopy
}

func (m *MemtableIterator) Next() (bool, common.KV) {
	if !m.initialSeek {
		m.doInitialSeek()
	}

	if m.it.Valid() && m.hasIterReturnedFirstResult {
		m.copyAndForwardIter()
	}

	if !m.it.Valid() {
		// Check the previous iter in case new entries were added
		if m.prevIt != nil {
			cp := *m.prevIt
			m.prevIt.Next()
			if m.prevIt.Valid() && (m.keyEnd == nil || bytes.Compare(m.prevIt.Key(), m.keyEnd) < 0) {
				// There are new entries - reset the iterator to prev.next
				m.it = m.prevIt
				m.prevIt = nil
				curr := common.KV{
					Key:   m.it.Key(),
					Value: m.it.Value(),
				}
				// m.copyAndForwardIter()
				return true, curr
			} else {
				// Put the prevIter back - still not valid
				m.prevIt = &cp
				return false, common.KV{}
			}
		}
		return false, common.KV{}
	}

	if m.keyEnd == nil || bytes.Compare(m.it.Key(), m.keyEnd) < 0 {
		curr := common.KV{
			Key:   m.it.Key(),
			Value: m.it.Value(),
		}
		m.hasIterReturnedFirstResult = true
		return true, curr
	}

	return false, common.KV{}
}

func (m *MemtableIterator) doInitialSeek() {
	if m.keyStart == nil {
		m.it.SeekToFirst()
	} else {
		m.it.Seek(m.keyStart)
	}
	if m.it.Valid() && (m.keyEnd == nil || bytes.Compare(m.it.Key(), m.keyEnd) < 0) {
		// We found a key in range, so we won't need to perform the initial seek again
		// If we didn't find a key in range we will try again the next time IsValid() is called - this allows for the
		// case where an iterator is created but no data in range, then data is written with data in range - this
		// should make the iterator valid
		m.initialSeek = true
	}
}

func (m *MemtableIterator) Close() {
}
