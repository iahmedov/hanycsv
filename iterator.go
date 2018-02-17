package hanycsv

type LocationIterator interface {
	Next() (*Location, error)
}
