package hanycsv

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
)

type CSVLocationIterator struct {
	ignoreHeader bool
	path         string
	file         *os.File
	reader       *csv.Reader
}

func NewCSVLocationIterator(filePath string, ignoreHeader bool) LocationIterator {
	return &CSVLocationIterator{
		ignoreHeader: ignoreHeader,
		path:         filePath,
		file:         nil,
	}
}

func (c *CSVLocationIterator) Next() (*Location, error) {
	if c.file == nil {
		file, err := os.Open(c.path)
		if err != nil {
			return nil, err
		}

		c.file = file
		c.reader = csv.NewReader(c.file)
		if c.ignoreHeader {
			c.reader.Read()
		}
	}

	record, err := c.reader.Read()
	if err != nil {
		return nil, err
	}

	return c.parse(record)
}

func (c *CSVLocationIterator) parse(record []string) (*Location, error) {
	if len(record) != 3 {
		return nil, ErrorInvalidInputFormat
	}

	loc := &Location{
		ID: record[0],
	}

	lat, err := strconv.ParseFloat(record[1], 64)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse latitude (%s) with error (%s)", record[1], err.Error())
	}

	lng, err := strconv.ParseFloat(record[2], 64)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse longitude (%s) with error (%s)", record[2], err.Error())
	}

	loc.Lat = lat
	loc.Lng = lng

	return loc, nil
}

func (c *CSVLocationIterator) Close() error {
	defer func() {
		c.reader = nil
		c.file = nil
	}()
	return c.file.Close()
}

var _ LocationIterator = (*CSVLocationIterator)(nil)
var _ io.Closer = (*CSVLocationIterator)(nil)
