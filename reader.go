package sortedpairs

import (
	"io"
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

	k, v, err = mr.rs[0].Peek()
	if err != nil {
		return
	}

	return nil, nil, nil
}
