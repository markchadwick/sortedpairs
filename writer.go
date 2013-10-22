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
	w          io.Writer
	pending    pairs
	pendingLen int
	workdir    string
	spillNo    int
}

func NewWriter(w io.Writer, capacity int) (writer *Writer, err error) {
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
	}
	return
}

func (w *Writer) Close() (err error) {
	if err = w.Spill(); err != nil {
		return
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
	f, err := os.Create(path.Join(w.workdir, fname))
	if err != nil {
		return err
	}
	defer f.Close()

	w.pending.Sort()
	_, err = w.pending.Write(f)

	w.pending = make(pairs, 0)
	w.pendingLen = 0
	w.spillNo++
	return nil
}
