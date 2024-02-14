package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/boonsuen/objectdb"
)

type Address struct {
	Postcode string `json:"postcode"`
}

type Restaurant struct {
	Name    string  `json:"name"`
	Cuisine string  `json:"cuisine"`
	Address Address `json:"address"`
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
		{"Rule of Thirds", "Japanese", Address{"80000"}},
		{"Xi'an Famous Foods", "Chinese", Address{"10000"}},
		{"Joe's Shanghai", "Chinese", Address{"20000"}},
		{"Shanghai Asian Manor", "Chinese", Address{"80000"}},
		{"Good Bread", "Chinese", Address{"10000"}},
		{"Shanghai Cuisine", "Chinese", Address{"10000"}},
	}

	newEmployees := []interface{}{
		Employee{"John", "25"},
		Employee{"John", "20"},
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
	restaurants, err := db.FindMany("restaurants", nil, objectdb.Options{})
	if err != nil {
		log.Fatalf("error finding restaurants: %v", err)
		return
	}

	// Find all employees in the collection
	employees, err := db.FindMany("employees", objectdb.Query{}, objectdb.Options{})
	if err != nil {
		log.Fatalf("error finding employees: %v", err)
		return
	}

	// Print all the restaurants
	fmt.Println("\nAll restaurants:")
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
	fmt.Println("\nAll employees:")
	for index, doc := range employees {
		employee := Employee{}
		err := objectdb.Unmarshal(doc, &employee)
		if err != nil {
			log.Fatalf("error unmarshalling employee: %v", err)
			return
		}

		fmt.Printf("%d: %+v\n", index, employee)
	}

	// Find 2 chinese restaurants in the postcode of 10000
	resQuery := objectdb.Query{
		{"AND", []objectdb.Condition{
			{Path: "cuisine", Operator: "=", Value: "Chinese"},
			{Path: "address.postcode", Operator: "=", Value: "10000"},
		}},
	}

	chineseRestaurants, err := db.FindMany("restaurants", resQuery, objectdb.Options{Limit: 2})

	if err != nil {
		log.Fatalf("error finding chinese restaurants: %v", err)
		return
	}

	// Print the chinese restaurants
	fmt.Println("\n2 Chinese restaurants in 10000:")
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
	employee, err := db.FindOne("employees", objectdb.Query{
		{"AND", []objectdb.Condition{
			{Path: "age", Operator: "=", Value: 30},
		}},
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
		fmt.Println("\nDeleted employee ID:", employeeId)
	}

	fmt.Printf("\n30 years old employee: %v\n", employee)

	// Find employees named John or Jane, whose age is between 20 and 30
	myQuery := objectdb.Query{
		{"OR", []objectdb.Condition{
			{Path: "name", Operator: "=", Value: "Jane"},
			{Path: "name", Operator: "=", Value: "John"},
		}},
		{"AND", []objectdb.Condition{
			{Path: "age", Operator: ">", Value: 20},
			{Path: "age", Operator: "<", Value: 40},
		}},
	}

	employees, err = db.FindMany("employees", myQuery, objectdb.Options{})
	if err != nil {
		log.Fatalf("error finding one employee: %v", err)
		return
	}

	fmt.Printf("\nEmployees named John or Jane, whose age is between 20 and 30: %v\n", employees)
}
