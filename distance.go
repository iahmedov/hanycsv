package hanycsv

import (
	"container/heap"
	"fmt"
	"math"
)

const (
	RADIUS = 6371000
)

// Distance in meters between two given Location points
type Distance struct {
	// P1 *Location
	P2       *Location
	Distance float64
}

type DistanceComparator func(d1, d2 *Distance) bool
type DistanceCalculator func(p1, p2 *Location) *Distance
type distancePriorityQueue struct {
	distances []*Distance
	cmp       DistanceComparator
}

func (d *Distance) String() string {
	return fmt.Sprintf("Distance((%s) = %f)\n", d.P2, d.Distance)
}

func MinDistanceComparator(d1, d2 *Distance) bool {
	return d1.Distance < d2.Distance
}

func MaxDistanceComparator(d1, d2 *Distance) bool {
	return d1.Distance > d2.Distance
}

// GreatCircleDistance calculates distance between two locations great circle distance
func GreatCircleDistance(p1, p2 *Location) *Distance {
	deltaLat := (p2.Lat - p1.Lat) * (math.Pi / 180.0)
	deltaLon := (p2.Lng - p1.Lng) * (math.Pi / 180.0)

	lat1 := p1.Lat * (math.Pi / 180.0)
	lat2 := p2.Lat * (math.Pi / 180.0)

	x1 := math.Sin(deltaLat/2) * math.Sin(deltaLat/2)
	x2 := math.Sin(deltaLon/2) * math.Sin(deltaLon/2) * math.Cos(lat1) * math.Cos(lat2)

	dist := 2 * math.Asin(math.Sqrt(x1+x2))

	return &Distance{
		// P1:       p1,
		P2:       p2,
		Distance: RADIUS * dist,
	}
}

// EuclideanDistance calculates distance between two locations using euclidean distance
func EuclideanDistance(p1, p2 *Location) *Distance {
	return &Distance{
		// P1:       p1,
		P2:       p2,
		Distance: math.Sqrt(math.Pow(p1.Lat-p2.Lat, 2.0) + math.Pow(p1.Lng-p2.Lng, 2.0)),
	}
}

func (q *distancePriorityQueue) Len() int           { return len(q.distances) }
func (q *distancePriorityQueue) Less(i, j int) bool { return q.cmp(q.distances[j], q.distances[i]) }
func (q *distancePriorityQueue) Swap(i, j int) {
	q.distances[i], q.distances[j] = q.distances[j], q.distances[i]
}

func (q *distancePriorityQueue) Push(x interface{}) {
	q.distances = append(q.distances, x.(*Distance))
}

func (q *distancePriorityQueue) Pop() interface{} {
	n := len(q.distances)
	x := q.distances[n-1]
	q.distances = q.distances[0 : n-1]
	return x
}

func (q *distancePriorityQueue) Items() []*Distance {
	return q.distances
}

func newDistancePriorityQueue(capacity int, cmp DistanceComparator) *distancePriorityQueue {
	q := &distancePriorityQueue{
		distances: make([]*Distance, 0, capacity),
		cmp:       cmp,
	}
	heap.Init(q)
	return q
}
