package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/boonsuen/objectdb"
)

type Restaurant struct {
	Name    string `json:"name"`
	Cuisine string `json:"cuisine"`
}

type Employee struct {
	Name string      `json:"name"`
	Age  json.Number `json:"age"`
}

func main() {
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

	newRestaurants := []Restaurant{
		{"Rule of Thirds", "Japanese"},
		{"Xi'an Famous Foods", "Chinese"},
		{"Joe's Shanghai", "Chinese"},
		{"Shanghai Asian Manor", "Chinese"},
	}

	newEmployees := []interface{}{
		Employee{"John", "25"},
		Employee{"Jane", "30"},
		Employee{"Doe", "35"},
	}

	// Insert the restaurants using InsertOne
	for _, restaurant := range newRestaurants {
		id, err := db.InsertOne("restaurants", restaurant)
		if err != nil {
			log.Fatalf("error inserting restaurant: %v", err)
			return
		} else {
			log.Printf("Inserted restaurant: %s", id)
		}

		// Find by ID (success)
		doc, err := db.FindOneById("restaurants", id)
		if err != nil {
			log.Fatalf("error finding restaurant by ID: %v", err)
			return
		}

		restaurant := Restaurant{}
		err = objectdb.Unmarshal(doc, &restaurant)
		if err != nil {
			log.Fatalf("error unmarshalling restaurant: %v", err)
			return
		}

		fmt.Printf("Found restaurant by ID: %+v\n", restaurant)
	}

	// Find by ID (failure)
	_, err = db.FindOneById("restaurants", "nonexistent")
	if err != nil {
		log.Printf("error finding restaurant by ID: %v", err)
	} else {
		log.Fatalf("expected error finding restaurant by ID")
		return
	}

	// Insert the employees using InsertMany
	ids, err := db.InsertMany("employees", newEmployees)
	if err != nil {
		log.Fatalf("error inserting employees: %v", err)
		return
	} else {
		log.Printf("Inserted employees: %v", ids)
	}

	// Find all restaurants in the collection
	restaurants, err := db.FindMany("restaurants", map[string]interface{}{}, objectdb.Options{})
	if err != nil {
		log.Fatalf("error finding restaurants: %v", err)
		return
	}

	// Find all employees in the collection
	employees, err := db.FindMany("employees", map[string]interface{}{}, objectdb.Options{})
	if err != nil {
		log.Fatalf("error finding employees: %v", err)
		return
	}

	// Print all the restaurants
	fmt.Println("All restaurants:")
	for index, doc := range restaurants {
		restaurant := Restaurant{}
		err := objectdb.Unmarshal(doc, &restaurant)
		if err != nil {
			log.Fatalf("error unmarshalling restaurant: %v", err)
			return
		}

		fmt.Printf("%d: %+v\n", index, restaurant)
	}

	// Print all the employees
	fmt.Println("All employees:")
	for index, doc := range employees {
		employee := Employee{}
		err := objectdb.Unmarshal(doc, &employee)
		if err != nil {
			log.Fatalf("error unmarshalling employee: %v", err)
			return
		}

		fmt.Printf("%d: %+v\n", index, employee)
	}

	// Find 2 chinese restaurants
	chineseRestaurants, err := db.FindMany("restaurants", map[string]interface{}{"cuisine": "Chinese"}, objectdb.Options{Limit: 2})
	if err != nil {
		log.Fatalf("error finding chinese restaurants: %v", err)
		return
	}

	// Print the chinese restaurants
	fmt.Println("Chinese restaurants:")
	for index, doc := range chineseRestaurants {
		restaurant := Restaurant{}
		err := objectdb.Unmarshal(doc, &restaurant)
		if err != nil {
			log.Fatalf("error unmarshalling restaurant: %v", err)
			return
		}

		fmt.Printf("%d: %+v\n", index, restaurant)
	}

	// Find one employee with the age of 30
	employee, err := db.FindOne("employees", map[string]interface{}{
		"age": 30,
	})
	if err != nil {
		log.Fatalf("error finding one employee: %v", err)
		return
	}

	// Delete the employee by ID
	employeeId, ok := employee["_id"]
	if !ok {
		log.Fatalf("error getting employee ID")
		return
	}
	err = db.DeleteOneById("employees", employeeId.(string))
	if err != nil {
		log.Fatalf("error deleting employee: %v", err)
		return
	} else {
		log.Printf("Deleted employee: %s", employeeId)
	}

	fmt.Printf("30 years old employee: %v\n", employee)
}
