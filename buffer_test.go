package buffer

import (
	"testing"
	"time"

	influxdb "github.com/influxdb/influxdb/client"
)

func TestNewBuffer(t *testing.T) {
	fn := func(series []*influxdb.Series) {}
	b := NewBuffer(10, fn)
	defer b.Close()

	if b == nil {
		t.Error("Failed to instantiate buffer with `NewBuffer` function")
	}
}

func TestAdd(t *testing.T) {
	fn := func(series []*influxdb.Series) {}
	b := NewBuffer(10, fn)
	defer b.Close()

	if b.Size() != 0 {
		t.Error("Freshly created buffer is not empty")
	}

	b.Add(&influxdb.Series{})

	if b.Size() != 1 {
		t.Error("Adding series to buffer does not increment size")
	}
}

func TestFlush(t *testing.T) {
	fn := func(series []*influxdb.Series) {}
	b := NewBuffer(1, fn)
	defer b.Close()
	b.Add(&influxdb.Series{})

	if b.Size() != 0 {
		t.Error("Flushing buffer does not make it empty again")
	}
}

func TestClear(t *testing.T) {
	fn := func(series []*influxdb.Series) {}
	b := NewBuffer(10, fn)
	defer b.Close()
	b.Add(&influxdb.Series{})

	if b.Size() != 1 {
		t.Error("Expected buffer to contain 1 series before clearing, got %d", b.Size())
	}
	b.Clear()
	if b.Size() != 0 {
		t.Error("Expected buffer to be empty after clearing, got %d series", b.Size())
	}
}

func TestLookup(t *testing.T) {
	fn := func(series []*influxdb.Series) {}
	b := NewBuffer(10, fn)
	defer b.Close()
	b.Add(
		&influxdb.Series{
			Name:    "foo",
			Columns: []string{"a", "b", "c"},
			Points:  [][]interface{}{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}},
		},
		&influxdb.Series{
			Name:    "banana",
			Columns: []string{"a", "b", "c"},
			Points:  [][]interface{}{{11, 22, 33}},
		},
	)

	// Got to wait for aggragation coroutine to finish
	// XXX: Sleep is no good for testing. Need to come up with a better solution
	time.Sleep(10 * time.Millisecond)

	var (
		res map[string]*influxdb.Series
		err error
	)

	// Should not match inexistent series
	res, err = b.Lookup("bar", map[string]interface{}{})
	if err != nil {
		t.Error("Unexpected error")
	}
	if len(res) > 0 {
		t.Errorf("Expected empty result, got %d series", len(res))
	}

	// Should not match existent series with false condition
	res, err = b.Lookup("bar", map[string]interface{}{"a": 2})
	if err != nil {
		t.Error("Unexpected error")
	}
	if len(res) > 0 {
		t.Errorf("Expected empty result, got %d series", len(res))
	}

	// Should match series with empty condition
	res, err = b.Lookup("foo", map[string]interface{}{})
	if err != nil {
		t.Error("Unexpected error")
	}
	if len(res) != 1 {
		t.Errorf("Expected result with 1 series, got %d", len(res))
	}
	if len(res["foo"].Points) != 3 {
		t.Errorf("Expected 3 points, got %d", len(res["foo"].Points))
	}

	// Should match series with true condition
	res, err = b.Lookup("foo", map[string]interface{}{"a": 1})
	if err != nil {
		t.Error("Unexpected error")
	}
	if len(res) != 1 {
		t.Errorf("Expected result with 1 series, got %d", len(res))
	}
	if len(res["foo"].Points) != 1 {
		t.Errorf("Expected 1 point, got %d", len(res["foo"].Points))
	}

	// Should match series with regexp
	res, err = b.Lookup("/\\/", map[string]interface{}{})
	if err == nil {
		t.Error("Expected error, got nil")
	}
	if len(res) > 0 {
		t.Errorf("Expected empty result, got %d series", len(res))
	}

	res, err = b.Lookup("/oo$/", map[string]interface{}{})
	if err != nil {
		t.Error("Unexpected error")
	}
	if len(res) != 1 {
		t.Errorf("Expected result with 1 series, got %d", len(res))
	}

	res, err = b.Lookup("/.*/", map[string]interface{}{})
	if err != nil {
		t.Error("Unexpected error")
	}
	if len(res) != 2 {
		t.Errorf("Expected result with 2 series, got %d", len(res))
	}

	res, err = b.Lookup("/nothing/", map[string]interface{}{})
	if err != nil {
		t.Error("Unexpected error")
	}
	if len(res) != 0 {
		t.Errorf("Expected empty result, got %d series", len(res))
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
	fn := func(series []*influxdb.Series) { res <- series }
	b := NewBuffer(3, fn)
	defer b.Close()
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
