package sortedpairs

import (
	"bytes"
	"github.com/markchadwick/spec"
	"io"
)

var _ = spec.Suite("Reader", func(c *spec.C) {
	buf := new(bytes.Buffer)

	c.It("should read a simple pair", func(c *spec.C) {
		p := pair{
			[]byte("hello"),
			[]byte("world"),
		}
		_, err := p.Write(buf)
		c.Assert(err).IsNil()

		r := NewReader(buf)
		k, v, err := r.Next()
		c.Assert(err).IsNil()
		c.Assert(string(k)).Equals("hello")
		c.Assert(string(v)).Equals("world")

		k, v, err = r.Next()
		c.Assert(err).NotNil().Equals(io.EOF)
	})

	c.It("should merge multiple readers", func(c *spec.C) {
		buf0 := new(bytes.Buffer)
		buf1 := new(bytes.Buffer)
		buf2 := new(bytes.Buffer)

		r0 := NewReader(buf0)
		r1 := NewReader(buf1)
		r2 := NewReader(buf2)

		p := pair{[]byte("p-001"), []byte("buf0")}
		p.Write(buf0)

		p = pair{[]byte("p-003"), []byte("buf0")}
		p.Write(buf0)

		p = pair{[]byte("p-002"), []byte("buf1")}
		p.Write(buf1)

		mr := NewMergedReader(r0, r1, r2)

		k, v, err := mr.Next()
		c.Assert(err).IsNil()
		c.Assert(string(k)).Equals("p-001")
		c.Assert(string(v)).Equals("buf0")

		// k, v, err = mr.Next()
		// c.Assert(err).IsNil()
		// c.Assert(string(k)).Equals("p-002")
		// c.Assert(string(v)).Equals("buf1")

		// k, v, err = mr.Next()
		// c.Assert(err).IsNil()
		// c.Assert(string(k)).Equals("p-003")
		// c.Assert(string(v)).Equals("buf0")

		// k, v, err = mr.Next()
		// c.Assert(err).NotNil().Equals(io.EOF)
	})
})
