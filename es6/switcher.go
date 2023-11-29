package index

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v6"
	"github.com/elastic/go-elasticsearch/v6/esapi"
)

type switcher struct {
	client *elasticsearch.Client
	alias  string
	index  string
}

func NewSwitcher(client *elasticsearch.Client, alias, settings string) (*switcher, error) {
	index := fmt.Sprintf("%s_%s", alias, time.Now().UTC().Format("20060102150405"))

	body := strings.NewReader(settings)
	res, err := client.Indices.Create(index, client.Indices.Create.WithBody(body))

	if err != nil {
		return nil, err
	}

	if res.IsError() {
		return nil, fmt.Errorf("%+v", res)
	}

	return &switcher{
		client: client,
		alias:  alias,
		index:  index,
	}, nil
}

func (is *switcher) Name() string {
	return is.index
}

func (is *switcher) Switch(ctx context.Context, retention int) error {
	actions := []map[string]any{
		{
			"add": map[string]string{
				"alias": is.alias,
				"index": is.index,
			},
		},
	}

	oldIndices, err := is.oldIndices(ctx)
	if err != nil {
		return err
	}

	for i, idx := range oldIndices {
		if retention < 0 || i >= len(oldIndices)-retention {
			actions = append(actions, map[string]any{
				"remove": map[string]string{
					"alias": is.alias,
					"index": idx,
				},
			})
		} else {
			actions = append(actions, map[string]any{
				"remove_index": map[string]string{
					"index": idx,
				},
			})
		}
	}

	body, err := json.Marshal(map[string]any{"actions": actions})
	if err != nil {
		return err
	}

	req := esapi.IndicesUpdateAliasesRequest{Body: bytes.NewReader(body)}
	res, err := req.Do(ctx, is.client)

	if err != nil {
		return err
	}

	if res.IsError() {
		return fmt.Errorf("%+v", res)
	}

	return nil
}

func (is *switcher) oldIndices(ctx context.Context) ([]string, error) {
	req := esapi.CatIndicesRequest{
		Format: "json",
	}
	res, err := req.Do(ctx, is.client)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("%+v", res)
	}

	indices := []struct{ Index string }{}
	if err := json.NewDecoder(res.Body).Decode(&indices); err != nil {
		return nil, err
	}

	r := regexp.MustCompile(`^` + is.alias + `_[0-9]+$`)

	var oldIndices []string
	for _, idx := range indices {
		if r.MatchString(idx.Index) && idx.Index != is.index {
			oldIndices = append(oldIndices, idx.Index)
		}
	}

	sort.Strings(oldIndices)

	return oldIndices, nil
}
