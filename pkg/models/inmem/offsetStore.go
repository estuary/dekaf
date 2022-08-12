package inmem

import (
	"fmt"
	"sync"
)

type OffsetStore struct {
	mu sync.Mutex
	// Keys are a concatenation of topic-group-partition, returning an int offset.
	data map[string]int
}

func offsetKey(topic string, group string, partition int) string {
	return fmt.Sprintf("%s-%s-%d", topic, group, partition)
}

func (s *OffsetStore) PutOffset(topic string, group string, partition int, offset int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Make sure the storage is initialized.
	if s.data == nil {
		s.data = make(map[string]int)
	}

	s.data[offsetKey(topic, group, partition)] = offset

	return nil
}

func (s *OffsetStore) GetOffset(topic string, group string, partition int) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if got, ok := s.data[offsetKey(topic, group, partition)]; ok {
		return got
	}

	// No records for this partition yet, so start at the beginning.
	return -1
}
