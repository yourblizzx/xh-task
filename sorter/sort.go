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

const (
	fileWithCountPrefix = "tmp-file-with-count"
	stringRowDelimiter  = "\n"
)

var (
	byteRowDelimiter = []byte(stringRowDelimiter)
)

type Sorter struct {
	tmpDir string

	// limit in rows
	chunkSize int

	// all subs files for external sort merge
	subFiles []*os.File

	subFilesWithCount map[int]*os.File
}

// New - return new sorter struct
func New(chunkSize int) (*Sorter, error) {
	tempDir, err := os.MkdirTemp("", "*-external_merge")
	if err != nil {
		return nil, err
	}

	return &Sorter{
		tmpDir:            tempDir,
		chunkSize:         chunkSize,
		subFiles:          make([]*os.File, 0, 100),
		subFilesWithCount: make(map[int]*os.File, 100),
	}, nil
}

// Close - close all open files, then clean up garbage
func (s *Sorter) Close() error {
	const op = "Sorter.Close"

	for _, file := range s.subFiles {
		if err := file.Close(); err != nil {
			log.Printf("%s:%s", op, err)
		}
	}

	for _, file := range s.subFilesWithCount {
		if err := file.Close(); err != nil {
			log.Printf("%s:%s", op, err)
		}
	}

	err := os.RemoveAll(s.tmpDir)
	if err != nil {
		return err
	}

	return nil
}

func (s *Sorter) splitFileToChunks(file string) ([]*bufio.Reader, error) {
	const op = "Sorter.splitFileToChunks"

	// Open the input file
	inputFile, err := os.Open(file)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
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
				return nil, fmt.Errorf("%s: %w", op, err)
			}

			chunk = append(chunk, string(line))
		}

		// If the chunk is empty, we've reached the end of the input file
		if len(chunk) == 0 {
			break
		}

		subFile, err := s.newSortedSubFile(chunk, fCnt)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", op, err)
		}

		// store sub file
		s.subFiles = append(s.subFiles, subFile)
	}

	return s.getSubFileReaders(), nil
}

// newSortedSubFile - sort incoming slice of strings, and save it into new temp file with specific name
func (s *Sorter) newSortedSubFile(chunk []string, subNumber int) (f *os.File, err error) {
	const op = "Sorter.newSortedSubFile"

	if len(chunk) == 0 {
		return nil, nil
	}

	// Sort the chunk
	sort.Strings(chunk)

	f, err = os.Create(
		filepath.Join(s.tmpDir, fmt.Sprintf("sub-%d.txt", subNumber)),
	)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
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
		if _, err := sub.WriteString(line + stringRowDelimiter); err != nil {
			return nil, fmt.Errorf("%s: %w", op, err)
		}
	}

	if err := sub.Flush(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	// Return cursor at the start of the file
	if _, err := f.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
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

// getSubFileWithCountReaders - return readers for all sub files with count
func (s *Sorter) getSubFileWithCountReaders() map[int]*bufio.Reader {
	readers := make(map[int]*bufio.Reader, len(s.subFilesWithCount))

	for key, file := range s.subFilesWithCount {
		readers[key] = bufio.NewReader(file)
	}

	return readers
}

func (s *Sorter) writeToFileWithCount(data string, count int) error {
	const op = "Sorter.writeToFileWithCount"

	file, exist := s.subFilesWithCount[count]
	if !exist {
		var err error
		file, err = os.Create(filepath.Join(s.tmpDir, fmt.Sprintf("%s-%d", fileWithCountPrefix, count)))
		if err != nil {
			return fmt.Errorf("%s: %w", op, err)
		}

		s.subFilesWithCount[count] = file
	}

	if _, err := file.WriteString(fmt.Sprintf("%s\t%d\n", data, count)); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

// CreateSortedCountFile - sort algorithm.
// After call this one should defer Close
// Steps:
//  1. split file to chunks
//  2. read all chunks
//  3. look for a minimum string value and count it
//  4. write uniq lines with deduplicate-count into new files which has count in a name
//  5. read this file with count in a name from biggest to lower and write line by line to output file
func (s *Sorter) CreateSortedCountFile() (os.FileInfo, error) {
	const op = "Sorter.CreateSortedCountFile"

	// Read the input file in chunks and create sub-files
	readers, err := s.splitFileToChunks("input.txt")
	if err != nil {
		return nil, fmt.Errorf("%s, %w", op, err)
	}

	log.Printf("INFO: number of subfiles: %d", len(readers))

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
			return nil, fmt.Errorf("%s, %w", op, err)
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
			if err := s.writeToFileWithCount(currentLineValue, duplicateLineCount); err != nil {
				return nil, fmt.Errorf("%s, %w", op, err)
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
			return nil, fmt.Errorf("%s, %w", op, err)
		} else {
			lines[minIndex] = string(line)
		}
	}

	// don't forget last row
	if err := s.writeToFileWithCount(currentLineValue, duplicateLineCount); err != nil {
		return nil, fmt.Errorf("%s, %w", op, err)
	}

	keys := make([]int, 0, len(s.subFilesWithCount))
	for key, file := range s.subFilesWithCount {
		keys = append(keys, key)

		if _, err := file.Seek(0, 0); err != nil {
			return nil, fmt.Errorf("%s, %w", op, err)
		}
	}

	// sort keys due to golang hash map is unsorted
	sort.Ints(keys)

	readersWithCount := s.getSubFileWithCountReaders()

	outputFile, err := os.Create("output.txt")
	if err != nil {
		return nil, fmt.Errorf("%s, %w", op, err)
	}
	defer outputFile.Close()

	// Create a buffered writer for the output file
	output := bufio.NewWriter(outputFile)

	// write from bigger to lower duplicate-counter
	for i := len(keys) - 1; i >= 0; i-- {
		for {
			line, _, err := readersWithCount[keys[i]].ReadLine()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return nil, fmt.Errorf("%s, %w", op, err)
			}

			if _, err := output.Write(append(line, byteRowDelimiter...)); err != nil {
				return nil, fmt.Errorf("%s, %w", op, err)
			}
		}
	}

	// Flush the output buffer
	if err := output.Flush(); err != nil {
		return nil, fmt.Errorf("%s, %w", op, err)
	}

	stat, err := outputFile.Stat()
	if err != nil {
		return nil, fmt.Errorf("%s, %w", op, err)
	}

	return stat, nil
}
