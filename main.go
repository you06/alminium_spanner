package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Printf("Env HELLO:%s", os.Getenv("HELLO"))
}
