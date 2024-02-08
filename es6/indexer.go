package index

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/elastic/go-elasticsearch/v6"
	"github.com/elastic/go-elasticsearch/v6/esutil"
)

var DefaultFlushInterval = 1 * time.Second

type IndexerConfig struct {
	OnError        func(error)
	OnIndexFailure func(string, error)
	OnIndexSuccess func(string)
	// FlushInterval defaults to 1 second.
	FlushInterval time.Duration
}

type Indexer struct {
	bi             esutil.BulkIndexer
	onIndexFailure func(string, error)
	onIndexSuccess func(string)
}

func NewIndexer(client *elasticsearch.Client, index string, config IndexerConfig) (*Indexer, error) {
	if config.FlushInterval == 0 {
		config.FlushInterval = DefaultFlushInterval
	}

	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:        client,
		Index:         index,
		FlushInterval: config.FlushInterval,
		Refresh:       "true",
		OnError: func(ctx context.Context, err error) {
			config.OnError(fmt.Errorf("index error: %w", err))
		},
	})
	if err != nil {
		return nil, err
	}

	return &Indexer{
		bi:             bi,
		onIndexFailure: config.OnIndexFailure,
		onIndexSuccess: config.OnIndexSuccess,
	}, nil
}

func (b *Indexer) Index(ctx context.Context, id string, doc []byte) error {
	err := b.bi.Add(
		ctx,
		esutil.BulkIndexerItem{
			Action:       "index",
			DocumentID:   id,
			DocumentType: "_doc",
			Body:         bytes.NewReader(doc),
			OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
				b.onIndexSuccess(item.DocumentID)
			},
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
				if err != nil {
					err = fmt.Errorf("index error: %v", err)
				} else {
					err = fmt.Errorf("index error: %s: %s", res.Error.Type, res.Error.Reason)
				}

				b.onIndexFailure(item.DocumentID, err)
			},
		},
	)

	return err
}

func (b *Indexer) Close(ctx context.Context) error {
	return b.bi.Close(ctx)
}
