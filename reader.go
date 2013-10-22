package sortedpairs

import (
	"bytes"
	"io"
	"log"
)

type Reader struct {
	r        io.Reader
	nextPair *pair
	nextErr  error
}

func NewReader(r io.Reader) *Reader {
	return &Reader{
		r: r,
	}
}

func (r *Reader) Peek() (k, v []byte, err error) {
	if r.nextPair != nil {
		return r.nextPair[0], r.nextPair[1], r.nextErr
	}

	r.nextPair, r.nextErr = readPair(r.r)
	if r.nextErr != nil {
		return nil, nil, r.nextErr
	}
	return r.nextPair[0], r.nextPair[1], nil
}

func (r *Reader) Next() (k, v []byte, err error) {
	k, v, err = r.Peek()
	r.nextPair = nil
	r.nextErr = nil
	return
}

type MergedReader struct {
	rs []*Reader
}

func NewMergedReader(rs ...*Reader) *MergedReader {
	return &MergedReader{rs}
}

func (mr *MergedReader) Next() (k, v []byte, err error) {
	if len(mr.rs) == 0 {
		return nil, nil, io.EOF
	}

	var minK, minV []byte
	var minReader *Reader
	for i, r := range mr.rs {
		k, v, err = r.Peek()
		if err != nil {
			if err == io.EOF {
				mr.rs = append(mr.rs[:i], mr.rs[i+1:]...)
				continue
			} else {
				return nil, nil, err
			}
		}
		if minK == nil || bytes.Compare(k, minK) == -1 {
			minK = k
			minV = v
			minReader = r
		}
	}

	if minReader != nil {
		minReader.Next()
	}

	if minK == nil && len(mr.rs) == 0 {
		return minK, minV, io.EOF
	}

	return minK, minV, nil
}

func (mr *MergedReader) peek(i int) (k, v []byte, err error) {
	k, v, err = mr.rs[i].Peek()
	if err != nil {
		log.Printf("removing reader %d", i)
	}
	return
}
