package buffer

import (
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
func (b *Buffer) Lookup(series string, conds map[string]interface{}) (res *influxdb.Series) {
	s, ok := b.series[series]
	if !ok {
		return
	}

	// Building reversed column index
	colind := make(map[string]int)
	for i, name := range s.Columns {
		colind[name] = i
	}

	for _, row := range s.Points {
		good := true
		for key, val := range conds {
			ki, _ := colind[key]
			if row[ki] != val {
				good = false
			}
		}
		if good {
			// We need to return nil if there are no series/rows that matches condition
			if res == nil {
				res = &influxdb.Series{
					Name:    s.Name,
					Columns: s.Columns,
					Points:  [][]interface{}{},
				}
			}
			res.Points = append(res.Points, row)
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
