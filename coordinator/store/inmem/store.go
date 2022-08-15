package inmem

import (
	"sync"
	"time"

	"github.com/estuary/dekaf/coordinator"
)

// inmem Store has a map of in-memory groups
// has a simple pub/sub mechanism for allowing delayed responses

// probably need to move consumergroup to its own package

type store struct {
	mu     sync.RWMutex
	groups map[string]*coordinator.ConsumerGroup
}

func New() *store {
	return &store{
		groups: make(map[string]*coordinator.ConsumerGroup),
	}
}

func (s *store) AddMemberToGroup(member string, group string, protocols []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.groups[group]; !ok {
		s.groups[group] = coordinator.NewConsumerGroup(group)
	}

	g := s.groups[group]

	g.AddMember(member, protocols)
}

func (s *store) RemoveMemberFromGroup(member string, group string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if g, ok := s.groups[group]; ok {
		g.RemoveMember(member)
	}
}

func (s *store) SyncMemberForGroup(member string, group string, assignments map[string][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Do nothing if we don't know of this group.
	if g, ok := s.groups[group]; ok {
		g.SyncMember(member, assignments)
	}
}

func (s *store) HeartbeatForMemberInGroup(member string, group string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// TODO: This
}

func (s *store) AssignedLeaderForGroup(group string) (string, []string) {
	// TODO: Timeout handling. For now this will loop forever.

	for {
		l, ms := func() (string, []string) {
			s.mu.RLock()
			defer s.mu.RUnlock()

			if _, ok := s.groups[group]; !ok {
				panic("group did not exist")
			}
			g := s.groups[group]

			// Will be "", nil if the group is not in the "stable" state.
			return g.LeaderAndMembers()
		}()

		if l != "" {
			return l, ms
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (s *store) GroupSyncCompleted(member string, group string) []byte {
	// TODO: Timeout handling. For now this will loop forever.

	for {
		assignments := func() map[string][]byte {
			s.mu.RLock()
			defer s.mu.RUnlock()

			// Return the assignments if the group is in a stable state.
			if _, ok := s.groups[group]; !ok {
				panic("group did not exist")
			}
			g := s.groups[group]

			// Will be nil if the group is not in the "stable" state.
			return g.GroupAssignments()
		}()

		if assignments != nil {
			return assignments[member]
		}
		time.Sleep(100 * time.Millisecond)
	}
}
