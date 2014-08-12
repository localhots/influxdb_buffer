# InfluxDB aggregated buffer

In order to achieve maximum writing performance to InfluxDB an application needs to write series with as many points per series, as many series per batch as possible.

This buffer does exactly that: it aggregates multiple points into single series and multiple series into single batch. When buffer gets full it flushes series into database.

## Example:

```go
package main

import (
	"net/http"

	influxdb "github.com/influxdb/influxdb/client"
	"github.com/localhots/influxdb_buffer"
)

func handleMetrics(rw http.ResponseWriter, req *http.Request) {
	ua := parseUserAgent(req.UserAgent())
	geo := fetchGeoInfo(req.RemoteAddr)

	buf.Add(
        &influxdb.Series{
            Name:    "app.browsers",
            Columns: []string{"vendor", "version", "mobile"},
            Points:  [][]interface{}{{ua.Vendor, ua.Version, ua.IsMobile}},
        },
        &influxdb.Series{
            Name:    "app.visitors.geo",
            Columns: []string{"ip", "country", "city", "isp"},
            Points:  [][]interface{}{{ip, geo.Country, geo.City, geo.Isp}},
        },
    )
}

func writeMetrics(series []*influxdb.Series) {
	influxdb.WriteSeriesWithTimePrecision(series, "s")
}

var (
	buf *buffer.Buffer
)

func main() {
	buf = buffer.New(100, writeMetrics)
    defer buf.Close()

	http.HandleFunc("/metrics", handleMetrics)
	http.ListenAndServe(":8080", nil)
}

```
