package sortedpairs

import (
	"bytes"
	"github.com/markchadwick/spec"
	"io"
)

var _ = spec.Suite("Pair", func(c *spec.C) {
	buf := new(bytes.Buffer)

	c.It("should write", func(c *spec.C) {
		var p pair = [2][]byte{
			[]byte("hello"),
			[]byte("world"),
		}
		n, err := p.Write(buf)
		c.Assert(err).IsNil()

		length := 4 + len("hello") + 4 + len("world")
		c.Assert(n).Equals(length)
		c.Assert(buf.Bytes()).HasLen(length)
	})

	c.It("should read", func(c *spec.C) {
		var p pair = [2][]byte{
			[]byte("hello"),
			[]byte("world"),
		}
		p.Write(buf)

		p0, err := readPair(buf)
		c.Assert(err).IsNil()
		c.Assert(string(p0[0])).Equals("hello")
		c.Assert(string(p[1])).Equals("world")

		_, err = readPair(buf)
		c.Assert(err).NotNil()
		c.Assert(err).Equals(io.EOF)
	})

	c.It("should know its size", func(c *spec.C) {
		var p pair = [2][]byte{
			[]byte("hello"),
			[]byte("world"),
		}
		c.Assert(p.Length()).Equals(len("hello") + len("world"))
	})
})

var _ = spec.Suite("Pairs", func(c *spec.C) {
	c.It("should sort by key", func(c *spec.C) {
		ps := pairs{
			&pair{[]byte("pair-3"), []byte("v")},
			&pair{[]byte("pair-1"), []byte("v")},
			&pair{[]byte("pair-2"), []byte("v")},
		}
		ps.Sort()
		c.Assert(string(ps[0][0])).Equals("pair-1")
		c.Assert(string(ps[1][0])).Equals("pair-2")
		c.Assert(string(ps[2][0])).Equals("pair-3")
	})
})
