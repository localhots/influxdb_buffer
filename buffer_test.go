package buffer

import (
	"testing"
	"time"

	influxdb "github.com/influxdb/influxdb/client"
)

func TestNewBuffer(t *testing.T) {
	fn := func(series []*influxdb.Series) {}
	b := NewBuffer(10, fn)

	if b == nil {
		t.Error("Failed to instantiate buffer with `NewBuffer` function")
	}
}

func TestAdd(t *testing.T) {
	fn := func(series []*influxdb.Series) {}
	b := NewBuffer(10, fn)

	if b.Size() != 0 {
		t.Error("Freshly created buffer is not empty")
	}

	b.Add(&influxdb.Series{})

	if b.Size() != 1 {
		t.Error("Adding series to buffer does not increment size")
	}
}

func TestFlush(t *testing.T) {
	res := make(chan []*influxdb.Series, 1)
	fn := func(series []*influxdb.Series) {
		res <- series
	}
	b := NewBuffer(1, fn)
	b.Add(&influxdb.Series{})

	timer := time.NewTimer(time.Second)

	select {
	case <-res:
	case <-timer.C:
		t.Error("Flushing did not happen")
	}

	if b.Size() != 0 {
		t.Error("Flushing buffer does not make it empty again")
	}
}

func TestLookup(t *testing.T) {
	fn := func(series []*influxdb.Series) {}
	b := NewBuffer(10, fn)
	b.Add(&influxdb.Series{
		Name:    "foo",
		Columns: []string{"a", "b", "c"},
		Points:  [][]interface{}{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}},
	})

	var res *influxdb.Series

	// Should not match inexistent series
	res = b.Lookup("bar", map[string]interface{}{})
	if res != nil {
		t.Error("Expected nil result, got non-nil")
	}

	// Should not match existent series with false condition
	res = b.Lookup("bar", map[string]interface{}{"a": 2})
	if res != nil {
		t.Error("Expected nil result, got non-nil")
	}

	// Should match series with empty condition
	res = b.Lookup("foo", map[string]interface{}{})
	if res == nil {
		t.Error("Expected non-nil result, got nil")
	}
	if len(res.Points) != 3 {
		t.Errorf("Expected 3 resulting rows, got %d", len(res.Points))
	}

	// Should match series with true condition
	res = b.Lookup("foo", map[string]interface{}{"a": 1})
	if res == nil {
		t.Error("Expected non-nil result, got nil")
	}
	if len(res.Points) != 1 {
		t.Errorf("Expected 1 resulting rows, got %d", len(res.Points))
	}
}

func TestClose(t *testing.T) {
	fn := func(series []*influxdb.Series) {}
	b := NewBuffer(10, fn)
	b.Add(&influxdb.Series{})

	b.Close()

	if b.Size() != 0 {
		t.Error("Buffer was not flushed before closing")
	}

	defer func() {
		if recover() == nil {
			t.Error("No panic was caused by adding series to a closed buffer")
		}
	}()
	b.Add(&influxdb.Series{})
}

func TestAggregate(t *testing.T) {
	res := make(chan []*influxdb.Series, 1)
	fn := func(series []*influxdb.Series) {
		res <- series
	}
	b := NewBuffer(3, fn)
	b.Add(
		&influxdb.Series{
			Name:    "foo",
			Columns: []string{"bar", "baz"},
			Points:  [][]interface{}{{1, 2}},
		},
		&influxdb.Series{
			Name:    "banana",
			Columns: []string{"inevitable", "sadness"},
			Points:  [][]interface{}{{"every", "day"}},
		},
		&influxdb.Series{
			Name:    "foo",
			Columns: []string{"bar", "baz"},
			Points:  [][]interface{}{{3, 4}},
		},
	)

	timer := time.NewTimer(time.Second)

	var series []*influxdb.Series
	select {
	case series = <-res:
	case <-timer.C:
		t.Error("Flushing did not happen")
	}

	if len(series) != 2 {
		t.Errorf("Expected to recieve 2 aggregated series, not %d", len(series))
	}
	for _, ser := range series {
		switch ser.Name {
		case "foo":
			if len(ser.Points) != 2 {
				t.Errorf("Expected to recieve 2 aggregated points for series `foo`, not %d", len(ser.Points))
			}
		case "banana":
			if len(ser.Points) != 1 {
				t.Errorf("Expected to recieve 1 aggregated points for series `banana`, not %d", len(ser.Points))
			}
		default:
			t.Errorf("Unexpected series name: %s", ser.Name)
		}
	}
}
