# ObjectDB

ObjectDB is a document-oriented NoSQL database for Go.

## Features

- **Embedded**: No database server required
- **Document-oriented**: Store and query structs as JSON documents
- **Tiny**: Simple and lightweight

## Implementation

Internally, ObjectDB uses the [Pebble](https://github.com/cockroachdb/pebble) LSM key-value store as its storage engine.

## Installation

```shell
go get github.com/boonsuen/objectdb
```

## Database and Collection

ObjectDB stores documents in collections. A collection is a set of documents. A database can have multiple collections.

### Open a Database

```go
db, err := objectdb.Open("db")
if err != nil {
  log.Fatal(err)
  return
}
defer db.Close()
```

### Insert Documents

Collections are created implicitly when a document is inserted into a collection. Each document is identified by a unique UUID, which is added to the document as the `_id` field.

Insert a document into a collection:

```go
type Employee struct {
  Name string      `json:"name"`
  Age  json.Number `json:"age"`
}

employee := Employee{
  Name: "John",
  Age:  "30",
}

id, err := db.InsertOne("employees", employee)
if err != nil {
  log.Fatal(err)
}
```

Insert multiple documents into a collection:

```go
type Address struct {
  Postcode string `json:"postcode"`
}

type Restaurant struct {
  Name    string  `json:"name"`
  Cuisine string  `json:"cuisine"`
  Address Address `json:"address"`
}

newRestaurants := []Restaurant{
  {"Restaurant A", "Italian", Address{"80000"}},
  {"Restaurant B", "Fast Food", Address{"10000"}},
  {"Restaurant C", "Fast Food", Address{"10000"}},
  {"Restaurant D", "Fast Food", Address{"10000"}},
}

ids, err := db.InsertMany("restaurants", newRestaurants)
if err != nil {
  log.Fatalf("error inserting restaurants: %v", err)
  return
} else {
  log.Printf("Inserted restaurants' IDs: %v", ids)
}

```

## Queries

### Find a Document

A single document in a collection can be retrieved by using the `FindOne` or `FindOneByID` method. Use the `Unmarshal` method to convert the document to a struct.

```go
doc, err := db.FindOneById("employees", id)
if err != nil {
  log.Fatal(err)
}

employee := Restaurant{}
err = objectdb.Unmarshal(doc, &employee)
if err != nil {
  log.Fatalf("error unmarshalling restaurant: %v", err)
  return
}
```

`FindOne` returns the first matching document. It's similar to using `FindMany` with a limit of 1.

```go
// Find one employee with the age of 30
employee, err := db.FindOne("employees", objectdb.Query{
  {"AND", []objectdb.Condition{
    {Path: "age", Operator: "=", Value: 30},
  }},
})
```

### Find Multiple Documents

To find multiple matching documents in a collection, use the `FindMany` method.

With empty query and options, it returns all documents in the collection.

```go
employees, err := db.FindMany("employees", objectdb.Query{}, objectdb.Options{})
```

### Limiting

The `Options` struct specifies the limit of the number of matching documents to return.

```go
objectdb.Options{Limit: 2}
```

### Filtering

The `Query` struct specifies the conditions to filter the documents.

The following example finds 2 Fast Food restaurants with the postcode of 10000.

```go
resQuery := objectdb.Query{
  {"AND", []objectdb.Condition{
    {Path: "cuisine", Operator: "=", Value: "Fast Food"},
    {Path: "address.postcode", Operator: "=", Value: "10000"},
  }},
}

ffRestaurants, err := db.FindMany("restaurants", resQuery, objectdb.Options{Limit: 2})
```

The query accepts multiple conditions. The `AND` and `OR` operators can be used to combine the conditions. Top-level conditions (each element in the `Query` slice) are **implicitly** combined with the `AND` operator. Only two levels of nesting are supported.

```go
query := objectdb.Query{
  {"AND", []objectdb.Condition{
    {Path: "name", Operator: "=", Value: "John"},
    {Path: "age", Operator: ">=", Value: "27"},
  }},
  {"OR", []objectdb.Condition{
    {Path: "address.city", Operator: "=", Value: "NY"},
    {Path: "address.postcode", Operator: "=", Value: "10000"},
  }},
}
```

Query above is equivalent to the following SQL where clause:

```sql
WHERE (name = 'John' AND age >= 27) AND (address.city = 'NY' OR address.postcode = '10000')
```

## Delete Documents

### Delete a Document

To delete a document, use the `DeleteOneById` method.

```go
err = db.DeleteOneById("collectionName", id)
```

## Indexing

ObjectDB keep tracks of the path-value pairs of the documents in a index. This allows for efficient querying of documents for certain queries. A search will fall back to a full collection scan when it is not possible to solely rely on the index to satisfy the query.
