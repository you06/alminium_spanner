package main

import (
	"cloud.google.com/go/spanner"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestDefaultTweetHashKeyStore_Insert(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	id := uuid.New().String()
	author := "sinmetal"
	now := time.Now()
	s := NewTweetHashKeyStore(spannerClient)

	key := s.NewKey(id, author)
	key = fmt.Sprintf("%s-_-%s", t.Name(), key)
	if err := s.Insert(ctx, &TweetHashKey{
		ID:        key,
		Author:    author,
		Content:   "Hellow Spanner",
		Favos:     []string{"sinsilver"},
		Sort:      1,
		CreatedAt: now,
		UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	k := spanner.Key{key}
	_, err := s.Get(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
}
