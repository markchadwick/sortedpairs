package sortedpairs

import (
	"bytes"
	"encoding/binary"
	"io"
	"sort"
)

type length int32

// ----------------------------------------------------------------------------
// Pair

type pair [2][]byte

func readPair(r io.Reader) (p *pair, err error) {
	p = new(pair)
	if err = p.read(r, 0); err != nil {
		return
	}
	err = p.read(r, 1)
	return
}

func (p *pair) read(r io.Reader, i int) (err error) {
	var l length
	if err = binary.Read(r, binary.BigEndian, &l); err != nil {
		return
	}
	p[i] = make([]byte, l)
	_, err = io.ReadFull(r, p[i])
	return
}

func (p *pair) Write(w io.Writer) (n int, err error) {
	var j int
	if j, err = p.write(w, 0); err != nil {
		return
	}
	n += j

	j, err = p.write(w, 1)
	n += j
	return
}

func (p *pair) Length() int {
	return len(p[0]) + len(p[1])
}

func (p *pair) write(w io.Writer, i int) (n int, err error) {
	bs := p[i]
	if err = binary.Write(w, binary.BigEndian, length(len(bs))); err != nil {
		return
	}
	n += 4

	if i, err := w.Write(bs); err != nil {
		return n, err
	} else {
		n += i
	}
	return
}

// ----------------------------------------------------------------------------
// Pairs

type pairs []*pair

func (p pairs) Len() int {
	return len(p)
}

func (p pairs) Less(i, j int) bool {
	return bytes.Compare(p[i][0], p[j][0]) < 0
}

func (p pairs) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p pairs) Sort() {
	sort.Sort(p)
}

func (p pairs) Write(w io.Writer) (n int, err error) {
	var j int
	for _, pair := range p {
		if j, err = pair.Write(w); err != nil {
			return
		}
		n += j
	}
	return
}
