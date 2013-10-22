package sortedpairs

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
)

type Writer struct {
	capacity   int
	w          WriteHandler
	pending    pairs
	pendingLen int
	workdir    string
	spillNo    int
	spilled    []string
}

type WriteHandler interface {
	Write(k, v []byte) error
}

func NewWriter(w WriteHandler, capacity int) (writer *Writer, err error) {
	workdir, err := ioutil.TempDir("", "sorted-pairs-")
	if err != nil {
		return nil, err
	}

	writer = &Writer{
		capacity:   capacity,
		w:          w,
		pending:    make([]*pair, 0),
		pendingLen: 0,
		workdir:    workdir,
		spilled:    make([]string, 0),
	}
	return
}

func (w *Writer) Close() (err error) {
	if err = w.Spill(); err != nil {
		return
	}
	readers := make([]*Reader, len(w.spilled))
	for i, fname := range w.spilled {
		file, err := os.Open(fname)
		if err != nil {
			return err
		}
		defer file.Close()
		readers[i] = NewReader(file)
	}

	mr := NewMergedReader(readers...)
	var k, v []byte
	for {
		k, v, err = mr.Next()
		if err != nil {
			if err != io.EOF {
				return err
			}
			return nil
		}
		if err = w.w.Write(k, v); err != nil {
			return err
		}
	}

	return os.RemoveAll(w.workdir)
}

func (w *Writer) Write(p0, p1 []byte) (err error) {
	pair := &pair{p0, p1}
	w.pending = append(w.pending, pair)
	w.pendingLen += pair.Length()
	if w.pendingLen > w.capacity {
		err = w.Spill()
	}
	return
}

// Sort and spill the pending pairs to disk
func (w *Writer) Spill() error {
	if len(w.pending) == 0 {
		return nil
	}

	fname := fmt.Sprintf("spill-%07d", w.spillNo)
	fpath := path.Join(w.workdir, fname)
	f, err := os.Create(fpath)
	if err != nil {
		return err
	}
	defer f.Close()

	w.pending.Sort()
	_, err = w.pending.Write(f)

	w.spilled = append(w.spilled, fpath)
	w.pending = make(pairs, 0)
	w.pendingLen = 0
	w.spillNo++
	return nil
}
