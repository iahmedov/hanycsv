package hanycsv

import (
	"io"
	"strconv"
)

type dummyLocationIterator struct {
	idx, count int
	delta      float64
	target     *Location
}

func (d *dummyLocationIterator) Next() (*Location, error) {
	if d.idx < d.count {
		d.idx++
		return &Location{
			ID:  strconv.Itoa(d.idx),
			Lat: d.target.Lat + (float64(d.idx) * d.delta),
			Lng: d.target.Lng + (float64(d.idx) * d.delta),
		}, nil
	}

	return nil, io.EOF
}

func NewDummyLocationIterator(target *Location, count int, delta float64) LocationIterator {
	return &dummyLocationIterator{
		idx:    0,
		count:  count,
		delta:  delta,
		target: target,
	}
}
