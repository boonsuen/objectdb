package objectdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/boonsuen/objectdb/fts"
	"github.com/cockroachdb/pebble"
	"github.com/google/uuid"
)

var (
	ErrDuplicateKey      = errors.New("duplicate key")           // A document with the same key already exists
	ErrNoDocuments       = errors.New("no documents found")      // No documents are found for a filter/query
	ErrDocumentNotExists = errors.New("document does not exist") // A document does not exist given an ID
)

type DB struct {
	store *pebble.DB
	index *pebble.DB
	fts   *fts.FTS
}

type Document map[string]interface{}

type Options struct {
	Limit int
}

// Example of a query:
// Top-level implicitly ANDs all the conditions
// query = [
// 	{
// 		"Operator": "AND",
// 		"Operands": [
// 			{
// 				"Field": "name",
// 				"Operator": "=",
// 				"Value": "Shaun Persad"
// 			},
// 			{
// 				"Field": "age",
// 				"Operator": ">=",
// 				"Value": 27
// 			}
// 		]
// 	},
// 	{
// 		"Operator": "OR",
// 		"Operands": [
// 			{
// 				"Field": "address.city",
// 				"Operator": "=",
// 				"Value": "New York"
// 			},
// 			{
// 				"Field": "address.postcode",
// 				"Operator": "=",
// 				"Value": "10000"
// 			}
// 		]
// 	}
// ]

// The above query is equivalent to the following SQL where clause:
// (top-level implicitly ANDs all the conditions)
// (name = "Shaun Persad" AND age >= 27) AND (address.city = "New York" OR address.postcode = "10000")
// - nested conditions are currently not supported
// - nested field is denoted by a dot (.) (e.g. address.city) in the path

// The query of a single condition is equivalent to the following query:
// {
// 	"Operator": "AND",
// 	"Operands": [
// 		{
// 			"Field": "name",
// 			"Operator": "=",
// 			"Value": "Shaun Persad"
// 		}
// 	]
// }

// The query of a single condition is equivalent to the following SQL where clause:
// name = "Shaun Persad"

type Condition struct {
	Path     string
	Operator string
	Value    interface{}
}

type Query []struct {
	Operator string // AND or OR
	Operands []Condition
}

// Comparison operators
const (
	EQ  = "="
	NE  = "!="
	GT  = ">"
	GTE = ">="
	LT  = "<"
	LTE = "<="
)

// Open opens the underlying storage engine
func Open(path string) (*DB, error) {
	db := DB{store: nil, index: nil, fts: nil}
	var err error

	db.store, err = pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}

	db.index, err = pebble.Open(path+".index", &pebble.Options{})
	if err != nil {
		return nil, err
	}

	db.fts, err = fts.NewFTS(path + ".text_index")

	return &db, err
}

// Close closes the underlying storage engine
func (db *DB) Close() error {
	err := db.store.Close()
	if err != nil {
		return err
	}
	err = db.index.Close()
	if err != nil {
		return err
	}
	err = db.fts.Close()
	if err != nil {
		return err
	}

	return nil
}

/****************
 * Insert
****************/

func (db *DB) InsertOne(collectionName string, document interface{}) (string, error) {
	id := uuid.New().String()

	// Convert the document to a map
	documentMap := map[string]interface{}{}
	b, err := json.Marshal(document)
	if err != nil {
		return "", err
	}

	if err := json.Unmarshal(b, &documentMap); err != nil {
		return "", err
	}

	// Add _id to document
	documentMap["_id"] = id

	// Marshal the document into a byte slice
	bs, err := json.Marshal(documentMap)
	if err != nil {
		return "", err
	}

	// Build the key
	key := getDocumentKey(collectionName, id)

	// Check if the key already exists
	value, closer, err := db.store.Get(key)
	if err != nil && err != pebble.ErrNotFound {
		return "", err
	}
	if value != nil {
		return "", fmt.Errorf("%w: %s", ErrDuplicateKey, id)
	}
	if closer != nil {
		defer closer.Close()
	}

	// Write the document to the store
	if err := db.store.Set(key, bs, pebble.Sync); err != nil {
		return "", err
	}

	// Add the document to the index
	if err := db.indexDocument(collectionName, id, documentMap); err != nil {
		return "", err
	}

	// Add the document to the full-text search index
	if err := db.fts.AddToIndex(collectionName, id, document); err != nil {
		return "", err
	}

	return id, nil
}

func (db *DB) InsertMany(collectionName string, documents []interface{}) ([]string, error) {
	var ids []string

	for _, document := range documents {
		id, err := db.InsertOne(collectionName, document)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}

	return ids, nil
}

/****************
 * Find
****************/

func (db *DB) FindOneById(collectionName, id string) (Document, error) {
	// Build the key
	key := getDocumentKey(collectionName, id)

	// Get the document from the store
	value, closer, err := db.store.Get(key)
	if err != nil {
		// If the document does not exist, return an error
		if err == pebble.ErrNotFound {
			return nil, ErrDocumentNotExists
		}

		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	defer closer.Close()

	// Unmarshal the document
	var document Document
	if err := json.Unmarshal(value, &document); err != nil {
		return nil, err
	}

	return document, nil
}

func (db *DB) FindOne(collectionName string, query Query) (Document, error) {
	documents, err := db.FindMany(collectionName, query, Options{Limit: 1})

	if len(documents) == 0 {
		return nil, ErrNoDocuments
	}

	return documents[0], err
}

func (db *DB) FindMany(collectionName string, query Query, options Options) ([]Document, error) {
	var documents []Document

	// For AND condition, if it contains at least one EQ condition, we can use the index
	// to check. If it contains only non-EQ conditions, fallback to scanning the entire collection.

	// For OR condition, if it contains all EQ conditions, we can use the index to check.
	// If it contains at least one non-EQ condition, fallback to scanning the entire collection.

	// Note that the query is not nested, and the top-level implicitly ANDs all the conditions.

	fallbackToFullScan := false

	// Check if query is not empty and not nil
	// Empty or nil query means full scan
	if len(query) > 0 && query != nil {
		// Top-level implicitly ANDs all the conditions
		for _, topOperand := range query {
			// If the top-level condition is OR, fallback to full scan if it contains at least one non-EQ condition
			if topOperand.Operator == "OR" {
				for _, operand := range topOperand.Operands {
					if operand.Operator != EQ {
						fallbackToFullScan = true
						break
					}
				}
			}

			// If the top-level condition is AND, check if it contains only non-EQ conditions
			foundEQ := false
			for _, operand := range topOperand.Operands {
				if operand.Operator == EQ {
					foundEQ = true
					break
				}
			}

			if !foundEQ {
				fallbackToFullScan = true
				break
			}
		}
	} else {
		fallbackToFullScan = true
	}

	// (... AND ...) AND (... OR ...)
	// Since top-level are ANDed, we can use the technique of counting how many
	// conditions are EQ. For example, there are 3 AND conditions above.
	// ((... OR ...) is one AND condition) and there are 2 out of 3 EQ conditions.
	// If the id appears in the index for all 3 AND conditions, then it is a match.

	if !fallbackToFullScan {
		// Use the index to check

		allMatchedIdsFromIndex := []string{}

		idsConditionCount := map[string]int{}
		nonRangeConditionCount := 0

		for _, topOperand := range query {
			if topOperand.Operator == "OR" {
				// Here, all the OR-ed conditions are EQ conditions, and because
				// it is considered as "one of the AND conditions" in the top-level perspective,
				// we add 1 to the nonRangeConditionCount regardless of the number of conditions in the OR.

				nonRangeConditionCount++

				matchedIdsInOr := map[string]bool{}

				for _, operand := range topOperand.Operands {
					// Build the index key
					indexKey := getIndexKey(collectionName, buildPathValue(operand.Path, fmt.Sprintf("%v", operand.Value)))

					idsString, closer, err := db.index.Get([]byte(indexKey))
					if err != nil && err != pebble.ErrNotFound {
						return nil, err
					}

					if closer != nil {
						defer closer.Close()
					}

					ids := strings.Split(string(idsString), ",")

					for _, id := range ids {
						matchedIdsInOr[id] = true
					}
				}

				// Put the matched IDs in the OR condition into the idsConditionCount
				for id := range matchedIdsInOr {
					_, ok := idsConditionCount[id]
					if !ok {
						idsConditionCount[id] = 0
					}
					idsConditionCount[id]++
				}
			} else {
				// Here, at least one of the ANDs is an EQ condition
				for _, operand := range topOperand.Operands {
					if operand.Operator == EQ {
						nonRangeConditionCount++

						// Build the index key
						indexKey := getIndexKey(collectionName, buildPathValue(operand.Path, fmt.Sprintf("%v", operand.Value)))

						idsString, closer, err := db.index.Get([]byte(indexKey))

						if err != nil && err != pebble.ErrNotFound {
							return nil, err
						}

						if closer != nil {
							defer closer.Close()
						}

						ids := strings.Split(string(idsString), ",")

						for _, id := range ids {
							_, ok := idsConditionCount[id]
							if !ok {
								idsConditionCount[id] = 0
							}
							idsConditionCount[id]++
						}
					}
				}
			}
		}

		for id, count := range idsConditionCount {
			if count == nonRangeConditionCount {
				allMatchedIdsFromIndex = append(allMatchedIdsFromIndex, id)
			}
		}

		if len(allMatchedIdsFromIndex) > 0 {
			for _, id := range allMatchedIdsFromIndex {
				document, err := db.FindOneById(collectionName, id)
				if err != nil && err != ErrDocumentNotExists {
					return nil, err
				}

				// Since the allMatchedIdsFromIndex are those that match the EQ conditions only,
				// we need to check if the document matches the other conditions as well.
				if matchQuery(document, query) {
					documents = append(documents, document)

					// Limit = 0 means no limit
					if options.Limit > 0 && len(documents) >= options.Limit {
						break
					}
				}
			}
		}
	} else {
		// Fallback to scanning the entire collection
		iter := db.store.NewIter(nil)
		defer iter.Close()

		for iter.First(); iter.Valid(); iter.Next() {
			var document Document
			if err := json.Unmarshal(iter.Value(), &document); err != nil {
				return nil, err
			}

			// Check the collection name
			if strings.Split(string(iter.Key()), ":")[0] != collectionName {
				continue
			}

			if matchQuery(document, query) {
				documents = append(documents, document)

				// Limit = 0 means no limit
				if options.Limit > 0 && len(documents) >= options.Limit {
					break
				}
			}
		}
	}

	return documents, nil
}

func getDocumentKey(collectionName, id string) []byte {
	return []byte(collectionName + ":" + id)
}

func getIndexKey(collectionName, pathValue string) []byte {
	return []byte(collectionName + ":" + pathValue)
}

// matchQuery checks if a document matches a query.
func matchQuery(document Document, query Query) bool {
	// Top-level implicitly ANDs all the conditions
	for _, topOperand := range query {
		// OR condition
		if topOperand.Operator == "OR" {
			foundMatch := false
			for _, operand := range topOperand.Operands {
				if matchCondition(document, operand) {
					foundMatch = true
					break
				}
			}

			if !foundMatch {
				return false
			}
		} else {
			// AND condition
			for _, operand := range topOperand.Operands {
				if !matchCondition(document, operand) {
					return false
				}
			}
		}
	}

	return true
}

// matchCondition checks if a document matches a condition.
func matchCondition(document Document, condition Condition) bool {
	value, ok := getValueFromPath(document, condition.Path)

	if !ok {
		return false
	}

	if condition.Operator == EQ {
		return fmt.Sprintf("%v", value) == fmt.Sprintf("%v", condition.Value)
	} else if condition.Operator == NE {
		return fmt.Sprintf("%v", value) != fmt.Sprintf("%v", condition.Value)
	}

	// Handle >, >=, <, <=
	right, err := strconv.ParseFloat(fmt.Sprintf("%v", condition.Value), 64)
	if err != nil {
		return false
	}

	var left float64
	switch v := value.(type) {
	case float64:
		left = v
	case float32:
		left = float64(v)
	case uint:
		left = float64(v)
	case uint8:
		left = float64(v)
	case uint16:
		left = float64(v)
	case uint32:
		left = float64(v)
	case uint64:
		left = float64(v)
	case int:
		left = float64(v)
	case int8:
		left = float64(v)
	case int16:
		left = float64(v)
	case int32:
		left = float64(v)
	case int64:
		left = float64(v)
	case string:
		left, err = strconv.ParseFloat(v, 64)
		if err != nil {
			return false
		}
	default:
		return false
	}

	switch condition.Operator {
	case GT:
		return left > right
	case GTE:
		return left >= right
	case LT:
		return left < right
	case LTE:
		return left <= right
	}

	return false
}

func getValueFromPath(document map[string]interface{}, path string) (interface{}, bool) {
	var docSegment any = document
	for _, part := range strings.Split(path, ".") {
		switch v := docSegment.(type) {
		case map[string]interface{}:
			docSegment = v[part]
		default:
			return nil, false
		}
	}

	return docSegment, true
}

// Unmarshal a document into a struct
func Unmarshal(doc Document, v interface{}) error {
	b, err := json.Marshal(doc)
	if err != nil {
		return err
	}

	return json.Unmarshal(b, v)
}

/****************
 * Delete
****************/

func (db *DB) DeleteOneById(collectionName, id string) error {
	// Build the key
	key := getDocumentKey(collectionName, id)

	// Get document by ID
	document, err := db.FindOneById(collectionName, id)
	if err != nil {
		return err
	}

	// Delete the document from the index
	err = db.deleteDocumentFromIndex(collectionName, id, document)
	if err != nil {
		return err
	}

	// Delete the document from the full-text search text index
	err = db.fts.DeleteFromIndex(collectionName, id, document)
	if err != nil {
		return err
	}

	// Delete the document from the store
	err = db.store.Delete(key, pebble.Sync)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) deleteDocumentFromIndex(collectionName, id string, document Document) error {
	pv := getPathValues(document, "")

	for _, pathValue := range pv {
		// Build the index key
		indexKey := getIndexKey(collectionName, pathValue)

		// Get the current value of the index
		idsString, closer, err := db.index.Get([]byte(indexKey))
		if err != nil && err != pebble.ErrNotFound {
			return err
		}

		if len(idsString) == 0 {
			// The document does not exist in the index
			if closer != nil {
				err = closer.Close()
				if err != nil {
					return err
				}
			}

			return nil
		}

		ids := strings.Split(string(idsString), ",")

		// Remove the ID from the index
		newIds := []string{}
		for _, existingId := range ids {
			if id != existingId {
				newIds = append(newIds, existingId)
			}
		}

		// If there are no more IDs, delete the index key
		if len(newIds) == 0 {
			err = db.index.Delete([]byte(indexKey), pebble.Sync)
			if err != nil {
				return err
			}
		} else {
			idsString = []byte(strings.Join(newIds, ","))
			err = db.index.Set([]byte(indexKey), idsString, pebble.Sync)
			if err != nil {
				return err
			}
		}

		if closer != nil {
			err = closer.Close()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

/****************
 * Index
****************/

// Index a document
func (db *DB) indexDocument(collectionName, id string, document Document) error {
	pv := getPathValues(document, "")

	for _, pathValue := range pv {
		// Build the index key
		indexKey := getIndexKey(collectionName, pathValue)

		// Get the current value of the index
		idsString, closer, err := db.index.Get([]byte(indexKey))
		if err != nil && err != pebble.ErrNotFound {
			return err
		}

		if len(idsString) == 0 {
			idsString = []byte(id)
		} else {
			ids := strings.Split(string(idsString), ",")

			found := false
			for _, existingId := range ids {
				if id == existingId {
					found = true
				}
			}

			if !found {
				idsString = append(idsString, []byte(","+id)...)
			}
		}

		if closer != nil {
			err = closer.Close()
			if err != nil {
				return err
			}
		}

		err = db.index.Set([]byte(indexKey), idsString, pebble.Sync)
		if err != nil {
			return err
		}
	}

	return nil
}

func getPathValues(document Document, prefix string) []string {
	var pvs []string

	// Exclude _id from the index
	delete(document, "_id")

	for key, value := range document {
		switch v := value.(type) {
		case map[string]interface{}:
			pvs = append(pvs, getPathValues(v, key)...)
			continue
		case []interface{}:
			continue
		}

		if prefix != "" {
			key = prefix + "." + key
		}

		pvs = append(pvs, buildPathValue(key, value))
	}

	return pvs
}

func buildPathValue(path string, value interface{}) string {
	return fmt.Sprintf("%s=%v", path, value)
}

/****************
 * Full-text search
****************/

func (db *DB) Search(collectionName, text string) ([]Document, error) {
	documentIds, err := db.fts.Search(collectionName, text)
	if err != nil {
		return nil, err
	}

	var documents []Document
	for _, id := range documentIds {
		document, err := db.FindOneById(collectionName, id)
		if err != nil {
			return nil, err
		}

		documents = append(documents, document)
	}

	return documents, nil
}

// Clear all data in the store and index
func (db *DB) Clear() error {
	// Clear the store
	iter := db.store.NewIter(nil)
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		if err := db.store.Delete(iter.Key(), pebble.Sync); err != nil {
			return err
		}
	}

	// Clear the index
	iter = db.index.NewIter(nil)
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		if err := db.index.Delete(iter.Key(), pebble.Sync); err != nil {
			return err
		}
	}

	// Clear the full-text search index
	if err := db.fts.Clear(); err != nil {
		return err
	}

	return nil
}

// Pretty print all the key value pairs in the index
func (db *DB) PrintIndex() error {
	iter := db.index.NewIter(nil)
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		fmt.Printf("%s: %s\n", iter.Key(), iter.Value())
	}

	return nil
}
