## Location fetcher

Fetch Top closest/furthest locations fetcher from given list (csv)

## Available methods
* Simple - no logic, just fetch first N inserted elements
* Sort - Insert all elements and obtain top N elements by sorting and getting first N elements
* PriorityQueue - uses PriorityQueue for fetching top N elements
* Parallel - uses PriorityQueue and creates worker pool for distributing tasks (locations) and then merges all top elements from workers in order to create top N.

## Storage
* parses data from CSV file formatted: id, lat, lng
* If you need more storage formats implement `LocationIterator` interface

## Command line

Build cli tool

```bash
go build github.com/iahmedov/hanycsv/cmd/toplist
```

Usage:

```bash
Usage of ./toplist:
  -lat float
    	(default:0) Target latitude
  -lon float
    	(default:0) Target longitude
  -method string
    	(required) Method for top N list, values: [queue|sort|parallel]
  -n int
    	(default:5) Number of items to be selected as top (default 5)
  -path string
    	(required) file path to be analyzed
  -workers parallel
    	(default:1) Worker count for parallel method
```

Examples:

Using 4 parallel workers fetch top 10 locations towards (51.925146, 4.478617) from geodata.csv
```
./toplist -path test/geodata.csv -lat 51.925146 -lon 4.478617 -method parallel -workers 4 -n 10
```

Using priority queue fetch top 5 locations towards (51.925146, 4.478617) from geodata.csv
```
./toplist -path test/geodata.csv -lat 51.925146 -lon 4.478617 -method queue
```