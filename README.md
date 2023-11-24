# index

Bulk indexing and index switching

## How to use

```go
package main

import (
	"context"
	"log"

	"github.com/elastic/go-elasticsearch/v6"
	"github.com/ugent-library/index"
)

func main() {
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{"address"},
	})

	if err != nil {
		log.Println("error")
	}

	// Create a new switcher, auto-create a new index
	switcher, err := index.NewSwitcher(client, "my-index", "{...}")

	if err != nil {
		log.Println("error")
	}

	// Create a new indexer for the newly created index
	name := switcher.Name()
	indexer, err := index.NewIndexer(client, name, index.IndexerConfig{})

	if err != nil {
		log.Println("error")
	}

	// Add a doc to the new index
	ctx := context.Background()
	id := "id"
	doc := []byte("{...}")
	indexer.Index(ctx, id, doc)
	indexer.Close(ctx)

	// Switch the alias over to the new index
	switcher.Switch(ctx, 5)
}
```