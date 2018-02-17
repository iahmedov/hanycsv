package hanycsv

import (
	"fmt"
)

type LocationID = string

type Location struct {
	ID       LocationID
	Lat, Lng float64
}

func (l *Location) String() string {
	return fmt.Sprintf("Location(%s, %f, %f)", l.ID, l.Lat, l.Lng)
}
