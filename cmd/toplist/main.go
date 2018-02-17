package main

import (
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/iahmedov/hanycsv"
)

var flagFilePath string
var flagTargetLatitude float64
var flagTargetLongitude float64
var flagMethod string
var flagWorkersCount int
var flagTopN int

var (
	MethodQueue    = "queue"
	MethodSort     = "sort"
	MethodParallel = "parallel"
)

func init() {
	flag.StringVar(&flagFilePath, "path", "", "(required) file path to be analyzed")
	flag.Float64Var(&flagTargetLatitude, "lat", 0, "(default:0) Target latitude")
	flag.Float64Var(&flagTargetLongitude, "lon", 0, "(default:0) Target longitude")
	flag.IntVar(&flagTopN, "n", 5, "(default:5) Number of items to be selected as top")
	flag.StringVar(&flagMethod, "method", "", "(required) Method for top N list, values: [queue|sort|parallel]")
	flag.IntVar(&flagWorkersCount, "workers", 0, "(default:1) Worker count for `parallel` method")
}

func main() {
	flag.Parse()

	if len(flagFilePath) == 0 {
		flag.Usage()
		return
	}

	methods := map[string]interface{}{}
	methods[MethodQueue] = struct{}{}
	methods[MethodSort] = struct{}{}
	methods[MethodParallel] = struct{}{}
	flagMethod = strings.ToLower(flagMethod)

	if _, ok := methods[flagMethod]; !ok {
		flag.Usage()
		return
	}

	if flagMethod == MethodParallel && flagWorkersCount == 0 {
		flag.Usage()
		return
	}

	target := &hanycsv.Location{"-", flagTargetLatitude, flagTargetLongitude}
	fmt.Printf("Target location is: %s\n", target)
	reader := hanycsv.NewCSVLocationIterator(flagFilePath, true)
	var topMinList, topMaxList hanycsv.DistanceList

	if flagMethod == MethodQueue {
		topMinList = hanycsv.NewPQueueDistanceList(
			hanycsv.GreatCircleDistance,
			target,
			flagTopN,
			hanycsv.MinDistanceComparator)
		topMaxList = hanycsv.NewPQueueDistanceList(
			hanycsv.GreatCircleDistance,
			target,
			flagTopN,
			hanycsv.MaxDistanceComparator)
	} else if flagMethod == MethodSort {
		topMinList = hanycsv.NewSortedDistanceList(
			hanycsv.GreatCircleDistance,
			target,
			flagTopN,
			hanycsv.MinDistanceComparator)
		topMaxList = hanycsv.NewSortedDistanceList(
			hanycsv.GreatCircleDistance,
			target,
			flagTopN,
			hanycsv.MaxDistanceComparator)
	} else {
		topMinList = hanycsv.NewParallelSortedDistanceList(
			flagWorkersCount,
			hanycsv.GreatCircleDistance,
			target,
			flagTopN,
			hanycsv.MinDistanceComparator)
		topMaxList = hanycsv.NewParallelSortedDistanceList(
			flagWorkersCount,
			hanycsv.GreatCircleDistance,
			target,
			flagTopN,
			hanycsv.MaxDistanceComparator)
	}

	for {
		loc, err := reader.Next()
		if err != nil {
			if err != io.EOF {
				panic(err.Error())
			}
			break
		}
		topMinList.Insert(loc)
		topMaxList.Insert(loc)
	}

	fmt.Printf("Closest items: \n%v\n\n", topMinList.Top())
	fmt.Printf("Furthest items: \n%v\n\n", topMaxList.Top())
}
