package hanycsv

import (
	"io"
	"strconv"
	"testing"
)

var (
	target          = Location{"-", 0.0, 0.0}
	generationCount = 1000000
)

func getTops(iter LocationIterator, dlist DistanceList) []*Distance {
	for {
		loc, err := iter.Next()
		if err != nil {
			break
		}
		dlist.Insert(loc)
	}

	if closer, ok := dlist.(io.Closer); ok {
		closer.Close()
	}

	return dlist.Top()
}

func BenchmarkSorted(b *testing.B) {
	for n := 0; n < b.N; n++ {
		reader := NewDummyLocationIterator(&target, generationCount, 0.0001)
		topList := NewSortedDistanceList(
			GreatCircleDistance,
			&Location{"-", 0, 0},
			10,
			MaxDistanceComparator)

		getTops(reader, topList)
	}
}

func BenchmarkPQueue(b *testing.B) {
	for n := 0; n < b.N; n++ {
		reader := NewDummyLocationIterator(&target, generationCount, 0.0001)
		topList := NewPQueueDistanceList(
			GreatCircleDistance,
			&Location{"-", 0, 0},
			10,
			MaxDistanceComparator)

		getTops(reader, topList)
	}
}

func BenchmarkParallel1(b *testing.B) {
	for n := 0; n < b.N; n++ {
		reader := NewDummyLocationIterator(&target, generationCount, 0.0001)
		topList := NewParallelSortedDistanceList(
			1,
			GreatCircleDistance,
			&Location{"-", 0, 0},
			10,
			MaxDistanceComparator)

		getTops(reader, topList)
	}
}

func BenchmarkParallel4(b *testing.B) {
	for n := 0; n < b.N; n++ {
		reader := NewDummyLocationIterator(&target, generationCount, 0.0001)
		topList := NewParallelSortedDistanceList(
			6,
			GreatCircleDistance,
			&Location{"-", 0, 0},
			10,
			MaxDistanceComparator)

		getTops(reader, topList)
	}
}

func TestTopsAreSame(t *testing.T) {
	locationCount := 100000
	topCount := 10
	distanceCalculator := GreatCircleDistance

	readerP1 := NewDummyLocationIterator(&target, locationCount, 0.0001)
	topListP1 := NewParallelSortedDistanceList(
		1,
		distanceCalculator,
		&target,
		topCount,
		MinDistanceComparator)
	topP1 := getTops(readerP1, topListP1)

	readerP4 := NewDummyLocationIterator(&target, locationCount, 0.0001)
	topListP4 := NewParallelSortedDistanceList(
		4,
		distanceCalculator,
		&target,
		topCount,
		MinDistanceComparator)
	topP4 := getTops(readerP4, topListP4)

	readerS := NewDummyLocationIterator(&target, locationCount, 0.0001)
	topListS := NewSortedDistanceList(
		distanceCalculator,
		&target,
		topCount,
		MinDistanceComparator)
	topS := getTops(readerS, topListS)

	readerPQ := NewDummyLocationIterator(&target, locationCount, 0.0001)
	topListPQ := NewPQueueDistanceList(
		distanceCalculator,
		&target,
		topCount,
		MinDistanceComparator)
	topPQ := getTops(readerPQ, topListPQ)

	if len(topPQ) != topCount || len(topS) != topCount ||
		len(topP1) != topCount || len(topP4) != topCount {
		t.Fatalf("number of elements do not match")
	}

	topIDs := map[string]interface{}{}
	for i := 0; i < topCount; i++ {
		topIDs[strconv.Itoa(i+1)] = struct{}{}
	}

	for _, tops := range [][]*Distance{topPQ, topS, topP1, topP4} {
		for _, el := range tops {
			if _, ok := topIDs[el.P2.ID]; !ok {
				t.Fatalf("unexpected top element: %s", el.P2.ID)
			}
		}
	}
}

func TestTopInvalidValues(t *testing.T) {
	tests := []struct {
		locationCount int
		topCount      int
		expectedCount int
	}{
		{10, 10, 10},
		{10, 100, 10},
		{1, 1, 1},
		{0, 0, 0},
		{0, 100, 0},
		{100, 0, 0},
	}

	for idx, test := range tests {
		distanceCalculator := GreatCircleDistance

		readerP1 := NewDummyLocationIterator(&target, test.locationCount, 0.0001)
		topListP1 := NewParallelSortedDistanceList(
			1,
			distanceCalculator,
			&target,
			test.topCount,
			MinDistanceComparator)
		topP1 := getTops(readerP1, topListP1)

		readerP4 := NewDummyLocationIterator(&target, test.locationCount, 0.0001)
		topListP4 := NewParallelSortedDistanceList(
			4,
			distanceCalculator,
			&target,
			test.topCount,
			MinDistanceComparator)
		topP4 := getTops(readerP4, topListP4)

		readerS := NewDummyLocationIterator(&target, test.locationCount, 0.0001)
		topListS := NewSortedDistanceList(
			distanceCalculator,
			&target,
			test.topCount,
			MinDistanceComparator)
		topS := getTops(readerS, topListS)

		readerPQ := NewDummyLocationIterator(&target, test.locationCount, 0.0001)
		topListPQ := NewPQueueDistanceList(
			distanceCalculator,
			&target,
			test.topCount,
			MinDistanceComparator)
		topPQ := getTops(readerPQ, topListPQ)

		for topIdx, top := range [][]*Distance{topP1, topP4, topS, topPQ} {
			if len(top) != test.expectedCount {
				t.Fatalf("(%d.%d) number of elements do not match, expected(%d), got(%d)", idx, topIdx, test.expectedCount, len(top))
			}
		}
	}
}
