package buffer

import (
	"regexp"

	influxdb "github.com/influxdb/influxdb/client"
)

type (
	Buffer struct {
		fn       func(series []*influxdb.Series)
		in       chan *influxdb.Series
		series   map[string]*influxdb.Series
		size     int
		capacity int
	}
)

// `New` is an alias for a `NewBuffer` function
func New(capacity int, fn func(series []*influxdb.Series)) *Buffer {
	return NewBuffer(capacity, fn)
}

// Creates a new buffer with given capacity and flushing function
// It also starts aggregation function in a coroutine
func NewBuffer(capacity int, fn func(series []*influxdb.Series)) *Buffer {
	b := &Buffer{
		fn:       fn,
		in:       make(chan *influxdb.Series),
		series:   make(map[string]*influxdb.Series),
		capacity: capacity,
	}
	go b.aggregate()

	return b
}

// Returns buffer size
// 0 <= size <= capacity
func (b *Buffer) Size() int {
	return b.size
}

// Adds one or multiple series to buffer
func (b *Buffer) Add(series ...*influxdb.Series) {
	for _, item := range series {
		b.in <- item
	}
}

// Flushes aggregated series into database
func (b *Buffer) Flush() {
	sbuffer := []*influxdb.Series{}
	for _, item := range b.series {
		sbuffer = append(sbuffer, item)
	}

	go b.fn(sbuffer)

	b.series = make(map[string]*influxdb.Series)
	b.size = 0
}

// Searches for points of given series that matches provided conditions
// All resulting series MUST have the same set of columns in the same order
func (b *Buffer) Lookup(pattern string, conds map[string]interface{}) (res map[string]*influxdb.Series, err error) {
	res = make(map[string]*influxdb.Series)

	sers, err := b.matchSeries(pattern)
	if err != nil || len(sers) == 0 {
		return
	}

	// Building reversed column index
	colind := make(map[string]int)
	for i, name := range sers[0].Columns {
		colind[name] = i
	}

	for _, s := range sers {
		for _, row := range s.Points {
			good := true
			for key, val := range conds {
				ki, _ := colind[key]
				if row[ki] != val {
					good = false
				}
			}
			if good {
				_, ok := res[s.Name]
				if !ok {
					res[s.Name] = &influxdb.Series{
						Name:    s.Name,
						Columns: s.Columns,
						Points:  [][]interface{}{},
					}
				}
				res[s.Name].Points = append(res[s.Name].Points, row)
			}
		}
	}

	return
}

// Closes buffer income channel and flushes all remaining series into database
// It also terminates its aggregation coroutine
func (b *Buffer) Close() {
	close(b.in)
	b.Flush()
}

func (b *Buffer) aggregate() {
	for {
		select {
		case item, open := <-b.in:
			if !open {
				return
			}

			_, ok := b.series[item.Name]
			if ok {
				b.series[item.Name].Points = append(b.series[item.Name].Points, item.Points...)
			} else {
				b.series[item.Name] = item
			}
		}

		b.size += 1
		if b.size == b.capacity {
			b.Flush()
		}
	}
}

func (b *Buffer) matchSeries(pattern string) (res []*influxdb.Series, err error) {
	if len(pattern) > 2 && pattern[:1] == "/" && pattern[len(pattern)-1:] == "/" {
		var reg *regexp.Regexp

		reg, err = regexp.Compile(pattern[1 : len(pattern)-1])
		if err != nil {
			return
		}

		for name, s := range b.series {
			ok := reg.MatchString(name)
			if ok {
				res = append(res, s)
			}
		}
	} else {
		s, ok := b.series[pattern]
		if ok {
			res = append(res, s)
		}
	}

	return
}
