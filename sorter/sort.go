package sorter

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
)

type Sorter struct {
	tmpDir string

	// limit in rows
	chunkSize int

	// all subs files for external sort merge
	subFiles []*os.File
}

// New - return new sorter struct
func New(chunkSize int) (*Sorter, error) {
	tempDir, err := os.MkdirTemp("", "*-external_merge")
	if err != nil {
		return nil, err
	}

	return &Sorter{
		tmpDir:    tempDir,
		chunkSize: chunkSize,
	}, nil
}

// Close - close all open files, then clean up garbage
func (s *Sorter) Close() error {
	for _, file := range s.subFiles {
		file.Close()
	}

	err := os.RemoveAll(s.tmpDir)
	if err != nil {
		return err
	}

	return nil
}

func (s *Sorter) splitFileToChunks(file string) ([]*bufio.Reader, error) {
	// Open the input file
	inputFile, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer inputFile.Close()

	// Create a buffered reader for the input file
	input := bufio.NewReader(inputFile)

	fCnt := 0
	for ; ; fCnt++ {
		// Read a chunk of the input file
		chunk := make([]string, 0, s.chunkSize)
		for j := 0; j < s.chunkSize; j++ {
			line, _, err := input.ReadLine()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return nil, err
			}

			chunk = append(chunk, string(line))
		}

		// If the chunk is empty, we've reached the end of the input file
		if len(chunk) == 0 {
			break
		}

		subFile, err := s.newSortedSubFile(chunk, fCnt)
		if err != nil {
			return nil, err
		}

		// store sub file
		s.subFiles = append(s.subFiles, subFile)
	}

	if fCnt >= s.chunkSize {
		//TODO смержить файлы, пока мы не придем к значению chunkSize,
		// чтобы честно читать столько строк, сколько нам разрешено
		log.Println("little problem")
	}

	return s.getSubFileReaders(), nil
}

// newSortedSubFile - sort incoming slice of strings, and save it into new temp file with specific name
func (s *Sorter) newSortedSubFile(chunk []string, subNumber int) (f *os.File, err error) {
	if len(chunk) == 0 {
		return nil, nil
	}

	// Sort the chunk
	sort.Strings(chunk)

	f, err = os.Create(
		filepath.Join(s.tmpDir, fmt.Sprintf("sub-%d.txt", subNumber)),
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			f.Close()
		}
	}()

	// Create a buffered writer for the sub-file
	sub := bufio.NewWriter(f)

	// Write the sorted chunk to the sub-file
	for _, line := range chunk {
		if _, err := sub.WriteString(line + "\n"); err != nil {
			return nil, err
		}
	}

	// Flush the buffer and add the sub-file to the slice
	if err := sub.Flush(); err != nil {
		return nil, err
	}

	// Return cursor at the start of the file
	if _, err := f.Seek(0, 0); err != nil {
		return nil, err
	}

	return f, nil
}

// getSubFileReaders - return readers for all sorted sub files
func (s *Sorter) getSubFileReaders() []*bufio.Reader {
	readers := make([]*bufio.Reader, len(s.subFiles))

	for i, subFile := range s.subFiles {
		readers[i] = bufio.NewReader(subFile)
	}

	return readers
}

// CreateSortedCountFile - sort algorithm.
// After call this one should defer Close
// Steps:
//  1. split file to chunks
//  2. read all chunks
//  3. look for a minimum string value and count it
//  4. write uniq sorted lines with deduplicate-count into new file
func (s *Sorter) CreateSortedCountFile() (*os.FileInfo, error) {
	// Read the input file in chunks and create sub-files
	readers, err := s.splitFileToChunks("input.txt")
	if err != nil {
		return nil, err
	}

	log.Printf("INFO: number of subfiles: %d", len(readers))

	outputFile, err := os.Create("output.txt")
	if err != nil {
		return nil, err
	}
	defer outputFile.Close()

	// Create a buffered writer for the output file
	output := bufio.NewWriter(outputFile)

	var (
		// number of open files
		fileCount = len(readers)

		// Create a slice to hold the current lines for each sub-file
		lines = make([]string, 0, len(readers))

		currentLineValue   = ""
		duplicateLineCount = 1
	)

	// Read the first line from each sub-file
	for _, r := range readers {
		line, _, err := r.ReadLine()
		if err != nil {
			log.Fatal(err)
		}

		lines = append(lines, string(line))
	}

	for fileCount > 0 {
		var (
			minLine  string
			minIndex int
		)

		// Find the minimum line
		for i, line := range lines {
			if line != "" && (minLine == "" || line < minLine) {
				minLine = line
				minIndex = i
			}
		}

		switch currentLineValue {
		case "":
			currentLineValue = minLine
		case minLine:
			duplicateLineCount++
		default:
			if _, err := output.WriteString(
				fmt.Sprintf("%s\t%d\n", currentLineValue, duplicateLineCount),
			); err != nil {
				return nil, err
			}

			currentLineValue = minLine
			duplicateLineCount = 1
		}

		// Read the next line from the sub-file
		line, _, err := readers[minIndex].ReadLine()
		if err != nil {
			// If the sub-file is exhausted, decrease fileCount by one
			if errors.Is(err, io.EOF) {
				fileCount--
				lines[minIndex] = ""
				continue
			}
			return nil, err
		} else {
			lines[minIndex] = string(line)
		}
	}

	// write line with deduplicate-count as one row
	if _, err := output.WriteString(
		fmt.Sprintf("%s\t%d\n", currentLineValue, duplicateLineCount),
	); err != nil {
		return nil, err
	}

	// Flush the output buffer
	if err := output.Flush(); err != nil {
		return nil, err
	}

	return nil, nil
}
