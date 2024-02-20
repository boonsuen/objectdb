package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/boonsuen/objectdb"
)

type Address struct {
	AddressLine string `json:"addressLine"`
	Postcode    string `json:"postcode"`
}

type Restaurant struct {
	Name    string  `json:"name" objectdb:"textIndex"`
	Cuisine string  `json:"cuisine" objectdb:"textIndex"`
	Address Address `json:"address"`
}

func main() {
	// Full-text search
	db, err := objectdb.Open("db")
	if err != nil {
		log.Fatal(err)
		return
	}
	defer db.Close()

	// Clear the store
	err = db.Clear()
	if err != nil {
		log.Fatalf("error clearing the store: %v", err)
		return
	}

	newRestaurants := []interface{}{
		Restaurant{"Rebel's Pizza", "Italian", Address{"123 Main St", "10000"}},
		Restaurant{"Shanghai Baozi", "Chinese", Address{"456 Main St", "20000"}},
		Restaurant{"Mama's Pasta", "Italian", Address{"789 Main St", "30000"}},
		Restaurant{"Cafe Paris", "French", Address{"101 Main St", "40000"}},
		Restaurant{"Lucky Dumplings", "Chinese", Address{"202 Main St", "50000"}},
		Restaurant{"El Toro", "Mexican", Address{"303 Main St", "60000"}},
		Restaurant{"Pizza Palace", "Italian", Address{"404 Main St", "70000"}},
	}

	_, err = db.InsertMany("restaurants", newRestaurants)
	if err != nil {
		log.Fatalf("error inserting restaurants: %v", err)
		return
	}

	searchTerm := "italy and Pizza"
	documents, err := db.Search("restaurants", searchTerm)
	if err != nil {
		log.Fatalf("error searching restaurants: %v", err)
		return
	}

	fmt.Printf("Restaurants matched with %q:\n", searchTerm)

	for index, doc := range documents {
		restaurant := Restaurant{}
		err := objectdb.Unmarshal(doc, &restaurant)
		if err != nil {
			log.Fatalf("error unmarshalling restaurant: %v", err)
			return
		}

		prettyJson, err := json.MarshalIndent(doc, "", "  ")
		if err != nil {
			log.Fatalf(err.Error())
		}
		fmt.Printf("%d: %+v\n", index, string(prettyJson))
	}
}
