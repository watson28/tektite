package iteration

import (
	"errors"

	"github.com/spirit-labs/tektite/common"
)

type Iterator interface {
	Current() common.KV
	Next() error
	IsValid() (bool, error)
	Close()
}

type SimplerIterator interface {
	Next() (bool, common.KV)
	Close()
}

type IteratorAdapter struct {
	ok          bool
	current     common.KV
	SimplerIter SimplerIterator
}

func NewIteratorAdapter(iter SimplerIterator) *IteratorAdapter {
	return &IteratorAdapter{SimplerIter: iter, ok: true}
}

func (i *IteratorAdapter) Current() common.KV {
	return i.current
}

func (i *IteratorAdapter) Next() error {
	ok, kv := i.SimplerIter.Next()
	i.ok = ok
	if !ok {
		return errors.New("invalid state")
	}
	i.current = kv
	return nil
}

func (i *IteratorAdapter) IsValid() (bool, error) {
	return i.ok, nil
}

func (i *IteratorAdapter) Close() {
	i.SimplerIter.Close()
}
