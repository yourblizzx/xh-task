package main

import (
	"log"
	"os"
	"strconv"
	"xh-task/sorter"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("set program argument 'chunkSize' (integer). Must be gte 2.")
	}

	chunkSize, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatal("program argument 'chunkSize' must be an integer")
	}

	if chunkSize < 2 {
		log.Fatal("program argument 'chunkSize' must be gte 2.")
	}

	s, err := sorter.New(chunkSize)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err := s.Close(); err != nil {
			log.Println(err)
		}
	}()

	if _, err := s.CreateSortedCountFile(); err != nil {
		log.Fatal(err)
	}
}
