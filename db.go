package objectdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

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
}

type Document map[string]interface{}

type Options struct {
	Limit int
}

func Open(path string) (*DB, error) {
	db := DB{store: nil, index: nil}
	var err error
	db.store, err = pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}

	db.index, err = pebble.Open(path+".index", &pebble.Options{})
	return &db, err
}

// Close closes the underlying storage engine
func (db *DB) Close() error {
	err := db.store.Close()
	if err != nil {
		return err
	}
	return db.index.Close()
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

	// Check if the key already exists (use main store for first implementation)
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

	// Write the index to the index store
	// TODO: To be implemented

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

func (db *DB) FindOne(collectionName string, filter Document) (Document, error) {
	documents, err := db.FindMany(collectionName, filter, Options{Limit: 1})

	if len(documents) == 0 {
		return nil, ErrNoDocuments
	}

	return documents[0], err
}

func (db *DB) FindMany(collectionName string, filter Document, options Options) ([]Document, error) {
	var documents []Document

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

		if matchQuery(document, filter) {
			documents = append(documents, document)

			// Limit = 0 means no limit
			if options.Limit > 0 && len(documents) >= options.Limit {
				break
			}
		}
	}

	return documents, nil
}

func getDocumentKey(collectionName, id string) []byte {
	return []byte(collectionName + ":" + id)
}

// matchQuery checks if a document matches a query.
func matchQuery(doc, query Document) bool {
	for key, value := range query {
		if docValue, ok := doc[key]; !ok || fmt.Sprintf("%v", docValue) != fmt.Sprintf("%v", value) {
			return false
		}
	}
	return true
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

	// Delete the document from the store
	err := db.store.Delete(key, pebble.Sync)
	if err != nil {
		return err
	}

	// Delete the index from the index store
	// TODO: To be implemented

	return nil
}

// Clear all documents in the store
// (temporary function for testing)
func (db *DB) Clear() error {
	iter := db.store.NewIter(nil)
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		if err := db.store.Delete(iter.Key(), pebble.Sync); err != nil {
			return err
		}
	}

	return nil
}
