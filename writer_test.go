package sortedpairs

import (
	"bytes"
	"fmt"
	"github.com/markchadwick/spec"
	"math/rand"
)

var _ = spec.Suite("Writer", func(c *spec.C) {
	buf := new(bytes.Buffer)
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

		c.Assert(buf.Bytes()).HasLen(0)

		err := w.Close()
		c.Assert(err).IsNil()

		length := ((25 * 4) + (25 * 9) + (25 * 4) + (25 * 9))
		c.Skip("pending")
		c.Assert(buf.Bytes()).HasLen(length)
	})
})
