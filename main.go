package main

import (
	"log"

	"xh-task/sorter"
)

func main() {
	s, err := sorter.New(3)
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
