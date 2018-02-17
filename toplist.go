package hanycsv

import (
	"container/heap"
	"io"
	"sort"
	"sync"
)

type DistanceList interface {
	Insert(loc *Location)
	Top() []*Distance
}

// SimpleDistanceList used as storage for Distances between given (target and locations)
type SimpleDistanceList struct {
	calc         DistanceCalculator
	topDistances []*Distance
	distances    []*Distance
	target       *Location
	n            int
}

// SortedDistanceList uses sort and limit for obtaining top N items
// complexity would be O(N*logN) (N - number of inserted elements)
type SortedDistanceList struct {
	*SimpleDistanceList
	cmp DistanceComparator
}

// PQueueDistanceList uses priority queue for top N items
type PQueueDistanceList struct {
	calc   DistanceCalculator
	target *Location
	queue  *distancePriorityQueue
	n      int
	cmp    DistanceComparator
}

// ParallelSortedDistanceList uses multiple workers to distribute given locations
// and obtains Top items by merging all the results from workers
// ParallelSortedDistanceList uses PQueueDistanceList as its worker
// Caution: since API for Top not thread safe, before getting Top items, call Close()
// see Top function implementation for details. DistanceList Insert/Top not syncronized
type ParallelSortedDistanceList struct {
	distanceLists []DistanceList
	wg            sync.WaitGroup
	cmp           DistanceComparator
	ch            chan *Location
	n             int
}

func NewSimpleDistanceList(calc DistanceCalculator, target *Location, topN int) DistanceList {
	return newSimpleDistanceList(calc, target, topN)
}

func NewSortedDistanceList(calc DistanceCalculator, target *Location, topN int, cmp DistanceComparator) DistanceList {
	return newSortedDistanceList(calc, target, topN, cmp)
}

func NewPQueueDistanceList(calc DistanceCalculator, target *Location, topN int, cmp DistanceComparator) DistanceList {
	return newPQueueDistanceList(calc, target, topN, cmp)
}

func NewParallelSortedDistanceList(workers int, calc DistanceCalculator, target *Location, topN int, cmp DistanceComparator) DistanceList {
	p := &ParallelSortedDistanceList{
		distanceLists: make([]DistanceList, workers),
		ch:            make(chan *Location, workers),
		n:             topN,
		cmp:           cmp,
	}
	for i := 0; i < workers; i++ {
		p.distanceLists[i] = newPQueueDistanceList(calc, target, topN, cmp)
		p.wg.Add(1)
		go func(locations <-chan *Location, dlist DistanceList) {
			defer p.wg.Done()
			for loc := range locations {
				dlist.Insert(loc)
			}
		}(p.ch, p.distanceLists[i])
	}

	return p
}

func newSimpleDistanceList(calc DistanceCalculator, target *Location, topN int) *SimpleDistanceList {
	return &SimpleDistanceList{
		topDistances: make([]*Distance, 0, topN),
		distances:    make([]*Distance, 0, topN),
		target:       target,
		n:            topN,
		calc:         calc,
	}
}

func newSortedDistanceList(calc DistanceCalculator, target *Location, topN int, cmp DistanceComparator) *SortedDistanceList {
	return &SortedDistanceList{
		SimpleDistanceList: newSimpleDistanceList(calc, target, topN),
		cmp:                cmp,
	}
}

func newPQueueDistanceList(calc DistanceCalculator, target *Location, topN int, cmp DistanceComparator) *PQueueDistanceList {
	return &PQueueDistanceList{
		calc:   calc,
		target: target,
		queue:  newDistancePriorityQueue(topN, cmp),
		n:      topN,
		cmp:    cmp,
	}
}

func (t *SimpleDistanceList) Insert(loc *Location) {
	t.topDistances = nil
	t.distances = append(t.distances, t.calc(t.target, loc))
}

// Top returns first N elements inserted to list
func (t *SimpleDistanceList) Top() []*Distance {
	return t.distances[:t.n]
}

func (t *SortedDistanceList) Insert(loc *Location) {
	t.topDistances = nil
	t.SimpleDistanceList.Insert(loc)
}

// Top returns items sorted and filtered by given Comparator
func (t *SortedDistanceList) Top() []*Distance {
	if t.topDistances != nil {
		return t.topDistances
	}

	sort.Slice(t.distances, func(i, j int) bool {
		return t.cmp(t.distances[i], t.distances[j])
	})
	t.topDistances = t.distances[:min(t.n, len(t.distances))]
	return t.topDistances
}

func (t *PQueueDistanceList) Insert(loc *Location) {
	dist := t.calc(t.target, loc)
	if t.queue.Len() < t.n {
		heap.Push(t.queue, dist)
		return
	}

	heap.Push(t.queue, dist)
	heap.Pop(t.queue)

	return
}

// Top returns items sorted and filtered by given Comparator using priority queue
func (t *PQueueDistanceList) Top() []*Distance {
	items := make([]*Distance, 0, t.queue.Len())
	for t.queue.Len() > 0 {
		items = append(items, heap.Pop(t.queue).(*Distance))
	}

	for _, item := range items {
		heap.Push(t.queue, item)
	}

	sort.Slice(items, func(i, j int) bool {
		return t.cmp(items[i], items[j])
	})
	return items
}

func (t *ParallelSortedDistanceList) Close() error {
	close(t.ch)
	t.wg.Wait()
	return nil
}

func (t *ParallelSortedDistanceList) Insert(loc *Location) {
	t.ch <- loc
}

// Top returns top items from list, call Close() before calling this API
func (t *ParallelSortedDistanceList) Top() []*Distance {
	items := make([]*Distance, 0)
	var wg sync.WaitGroup
	for _, dlist := range t.distanceLists {
		wg.Add(1)
		go func(dlist DistanceList) {
			defer wg.Done()
			dlist.Top()
		}(dlist)
	}

	wg.Wait()
	for _, dlist := range t.distanceLists {
		items = append(items, dlist.Top()...)
	}

	sort.Slice(items, func(i, j int) bool {
		return t.cmp(items[i], items[j])
	})
	return items[:min(t.n, len(items))]
}

func min(i1, i2 int) int {
	if i1 < i2 {
		return i1
	}
	return i2
}

var _ DistanceList = (*SimpleDistanceList)(nil)
var _ DistanceList = (*SortedDistanceList)(nil)
var _ DistanceList = (*ParallelSortedDistanceList)(nil)
var _ DistanceList = (*PQueueDistanceList)(nil)
var _ io.Closer = (*ParallelSortedDistanceList)(nil)
