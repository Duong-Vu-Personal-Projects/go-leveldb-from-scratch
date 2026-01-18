package main

import (
	"fmt"
	"log"
	"os"
)

func main() {
	dbDir := "mydb"
	err := os.RemoveAll(dbDir)
	if err != nil {
		return
	}
	db, err := NewDB(dbDir)
	if err != nil {
		log.Fatal("Failed to create DB: %v", err)
	}
	log.Println("Writing data to trigger a flush...")
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		value := []byte(fmt.Sprintf("value-%03d", i))

		if err := db.Put(key, value); err != nil {
			log.Fatalf("Failed to put key %s: %v", key, err)
		}
	}

	val, ok := db.Get([]byte("hello"))
	if ok {
		fmt.Printf("Get 'hello' -> '%s'\n", val)
	}

}
