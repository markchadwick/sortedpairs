package sortedpairs

import (
	"bytes"
	"fmt"
	"github.com/markchadwick/spec"
	"io"
	"math/rand"
)

type BufWriter struct {
	b *bytes.Buffer
}

func (bw *BufWriter) Write(k, v []byte) error {
	p := pair{k, v}
	_, err := p.Write(bw.b)
	return err
}

var _ = spec.Suite("Writer", func(c *spec.C) {
	buf := &BufWriter{new(bytes.Buffer)}
	w, err := NewWriter(buf, 20)
	c.Assert(err).IsNil()
	defer w.Close()

	c.It("should write a pair", func(c *spec.C) {
		err := w.Write([]byte("hello"), []byte("world"))
		c.Assert(err).IsNil()

		c.Assert(w.pending).HasLen(1)
		c.Assert(w.pendingLen).Equals(len("hello") + len("world"))
	})

	c.It("should write a bunch to disk", func(c *spec.C) {
		for i := 0; i < 25; i++ {
			key := fmt.Sprintf("key-%05d", rand.Int31n(100))
			val := fmt.Sprintf("key-%05d", i)
			err := w.Write([]byte(key), []byte(val))
			c.Assert(err).IsNil()
		}

		c.Assert(buf.b.Bytes()).HasLen(0)

		err := w.Close()
		c.Assert(err).IsNil()

		length := ((25 * 4) + (25 * 9) + (25 * 4) + (25 * 9))
		c.Assert(buf.b.Bytes()).HasLen(length)

		r := NewReader(buf.b)
		var lastKey []byte
		for {
			k, v, err := r.Next()
			if err != nil {
				if err != io.EOF {
					c.Assert(err).IsNil()
				}
				return
			}
			c.Assert(v).NotNil()
			if lastKey != nil {
				cmp := bytes.Compare(lastKey, k)
				c.Assert(cmp <= 0).IsTrue()
			}
			lastKey = k
		}
	})
})
