package main

import (
	"log"

    "github.com/mcheviron/elise/internal/broker"
)

func main() {
	if err := broker.Run(); err != nil {
		log.Fatalf("broker exited: %v", err)
	}
}
