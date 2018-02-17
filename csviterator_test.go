package hanycsv

import (
	"io"
	"testing"
)

func TestCSVIterator(t *testing.T) {
	reader := NewCSVLocationIterator("test/geodata.csv", true)
	i := 0
	for {
		_, err := reader.Next()
		if err != nil {
			if err != io.EOF {
				t.Fatalf("Failed to fetch locations (%s)", err.Error())
			}
			break
		}
		i++
	}

	if i != 100 {
		t.Fatalf("parsed elements do not match actual count in file: %d", i)
	}
}
