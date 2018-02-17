package hanycsv

import (
	"math"
	"testing"
)

func TestGreatCircleDistance(t *testing.T) {
	target := Location{"-", 51.925146, 4.478617}
	_ = target
	tests := []struct {
		loc      Location
		expected float64
	}{
		// http://www.onlineconversion.com/map_greatcircle_distance.htm
		{Location{"", 37.1768672, -3.608897}, 1758080.61312},
		{Location{"", 52.36461880000000235, 4.93169289999999982}, 57825.50791},
		{Location{"", 51.9245615, 4.492032399999999}, 922.23121},
		{Location{"", -1, -1}, 5907469.6376},
		{Location{"", 1, 1}, 5671935.66733},
	}
	_ = tests

	for _, test := range tests {
		d := GreatCircleDistance(&target, &test.loc)
		if math.Abs(d.Distance-test.expected) > 0.0001 {
			t.Fatalf("expected (%.8f), received (%.8f)", test.expected, d.Distance)
		}
	}
}
