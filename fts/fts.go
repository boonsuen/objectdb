package fts

import (
	"reflect"
	"strings"
	"unicode"

	"github.com/cockroachdb/pebble"
	snowballeng "github.com/kljensen/snowball/english"
)

type FTS struct {
	textIndex *pebble.DB // Inverted index store
}

func NewFTS(path string) (*FTS, error) {
	textIndex, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &FTS{textIndex: textIndex}, nil
}

func (fts *FTS) Close() error {
	return fts.textIndex.Close()
}

// Text Analysis

// -- Tokenization
func tokenize(text string) []string {
	return strings.FieldsFunc(text, func(r rune) bool {
		// Split on any character that is not a letter or a number.
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})
}

// -- Normalization
// -- -- Lowercase
func lowercaseFilter(tokens []string) []string {
	r := make([]string, len(tokens))
	for i, token := range tokens {
		r[i] = strings.ToLower(token)
	}
	return r
}

// -- -- Stop Words
func stopwordFilter(tokens []string) []string {
	var stopwords = map[string]struct{}{
		"a": {}, "and": {}, "be": {}, "have": {}, "i": {},
		"in": {}, "of": {}, "that": {}, "the": {}, "to": {},
	}
	r := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if _, ok := stopwords[token]; !ok {
			r = append(r, token)
		}
	}
	return r
}

// -- -- Stemming
func stemmerFilter(tokens []string) []string {
	r := make([]string, len(tokens))
	for i, token := range tokens {
		r[i] = snowballeng.Stem(token, false)
	}
	return r
}

// -- Analysis Pipeline
func analyze(text string) []string {
	tokens := tokenize(text)
	tokens = lowercaseFilter(tokens)
	tokens = stopwordFilter(tokens)
	tokens = stemmerFilter(tokens)
	return tokens
}

// Building the Inverted Index
func (fts *FTS) AddToIndex(collectionName string, id string, document interface{}) error {
	// Get the text fields
	t := reflect.TypeOf(document)
	v := reflect.ValueOf(document)
	typeOfDoc := v.Type()

	// Iterate through the fields
	for i := 0; i < v.NumField(); i++ {
		fieldName := typeOfDoc.Field(i).Name
		field, found := t.FieldByName(fieldName)
		if !found {
			continue
		}

		// Get the tag value
		tagValue := field.Tag.Get("objectdb")

		// Split the tag value by ;
		tagValues := strings.Split(tagValue, ";")

		// Check if the tag value contains "textIndex"
		for _, tag := range tagValues {
			if tag == "textIndex" {
				// This field will be indexed for full-text search
				fieldValue := v.Field(i).Interface()

				tokens := analyze(fieldValue.(string))

				for _, token := range tokens {
					// Add the token to the inverted index
					// -- Build the key
					indexKey := getIndexKey(collectionName, token)
					// -- Get the existing value
					idsString, closer, err := fts.textIndex.Get(indexKey)
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

					err = fts.textIndex.Set([]byte(indexKey), idsString, pebble.Sync)
					if err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

// Deleting from the Inverted Index
func (fts *FTS) DeleteFromIndex(collectionName string, id string, document map[string]interface{}) error {
	// Iterate through the fields
	for _, fieldValue := range document {
		// Check if the field is string type
		if reflect.TypeOf(fieldValue).Kind() != reflect.String {
			continue
		}

		tokens := analyze(fieldValue.(string))

		for _, token := range tokens {
			// -- Build the key
			indexKey := getIndexKey(collectionName, token)
			// -- Get the existing value
			idsString, closer, err := fts.textIndex.Get(indexKey)
			if err != nil && err != pebble.ErrNotFound {
				return err
			}

			if len(idsString) == 0 {
				// No match
				continue
			} else {
				ids := strings.Split(string(idsString), ",")

				// Remove the id from the list
				var newIds []string
				for _, existingId := range ids {
					if id != existingId {
						newIds = append(newIds, existingId)
					}
				}

				// Update the inverted index
				if len(newIds) == 0 {
					err = fts.textIndex.Delete(indexKey, pebble.Sync)
					if err != nil {
						return err
					}
				} else {
					idsString = []byte(strings.Join(newIds, ","))
					err = fts.textIndex.Set([]byte(indexKey), idsString, pebble.Sync)
					if err != nil {
						return err
					}
				}
			}

			if closer != nil {
				err = closer.Close()
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// Querying
func (fts *FTS) Search(collectionName, text string) ([]string, error) {
	var matchedIds []string

	tokens := analyze(text)
	for _, token := range tokens {
		// Get the existing value
		indexKey := getIndexKey(collectionName, token)
		idsString, closer, err := fts.textIndex.Get(indexKey)
		if err != nil && err != pebble.ErrNotFound {
			return nil, err
		}

		if len(idsString) == 0 {
			// No match
			continue
		} else {
			ids := strings.Split(string(idsString), ",")

			if len(matchedIds) == 0 {
				matchedIds = ids
			} else {
				// Find the intersection
				matchedIds = intersection(matchedIds, ids)
			}
		}

		if closer != nil {
			err = closer.Close()
			if err != nil {
				return nil, err
			}
		}
	}

	return matchedIds, nil
}

func intersection(a, b []string) []string {
	m := make(map[string]bool)
	var result []string
	for _, item := range a {
		m[item] = true
	}
	for _, item := range b {
		if _, ok := m[item]; ok {
			result = append(result, item)
		}
	}
	return result
}

// Utils
func getIndexKey(collectionName, token string) []byte {
	return []byte(collectionName + ":" + token)
}

func (fts *FTS) Clear() error {
	iter := fts.textIndex.NewIter(nil)
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		if err := fts.textIndex.Delete(iter.Key(), pebble.Sync); err != nil {
			return err
		}
	}

	return nil
}

// Print Index
func (fts *FTS) PrintIndex() error {
	iter := fts.textIndex.NewIter(nil)
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()
		println(string(key), string(value))
	}

	return nil
}
