package coordinator

import "log"

type state interface {
	addMember(member string, protocols []string)
	removeMember(member string)
	syncMember(member string, assignments map[string][]byte)
	string() string
}

type ConsumerGroup struct {
	id                 string
	currentState       state
	generation         int
	membersJoined      map[string]bool
	membersSyncd       map[string]bool
	leader             string
	supportedProtocols []string
	topics             []string
	groupAssignments   map[string][]byte

	preparingRebalanceState  state
	completingRebalanceState state
	stableState              state
}

// TODO: heartbeats, generation ids, what happens if all the members leave?

// States in the actual kafka code: https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/coordinator/group/GroupMetadata.scala

/**
 * Group is preparing to rebalance
 *
 * action: respond to heartbeats with REBALANCE_IN_PROGRESS
 *         respond to sync group with REBALANCE_IN_PROGRESS
 *         remove member on leave group request
 *         park join group requests from new or existing members until all expected members have joined
 *         allow offset commits from previous generation
 *         allow offset fetch requests
 * transition: some members have joined by the timeout => CompletingRebalance
 *             all members have left the group => Empty
 *             group is removed by partition emigration => Dead
 */
type preparingRebalance struct {
	g *ConsumerGroup
}

func (s *preparingRebalance) string() string {
	return "preparingRebalance"
}

func (s *preparingRebalance) addMember(member string, protocols []string) {
	log.Printf("%s - %s - %s", "preparingRebalance", "addMember", member)

	s.g.membersJoined[member] = true

	// TODO: Handle protocols.

	// If we have all the members joined...
	for _, v := range s.g.membersJoined {
		if !v {
			return
		}
	}

	// ...get ready to complete the rebalance.
	s.g.setState(s.g.completingRebalanceState)
}

func (s *preparingRebalance) removeMember(member string) {
	log.Printf("%s - %s - %s", "preparingRebalance", "removeMember", member)

	delete(s.g.membersJoined, member)

	// if we have all the members joined...
	for _, v := range s.g.membersJoined {
		if !v {
			return
		}
	}

	// ...get ready to complete the rebalance.
	s.g.setState(s.g.completingRebalanceState)
}

func (s *preparingRebalance) syncMember(member string, assignments map[string][]byte) {
	log.Printf("%s - %s - %s", "preparingRebalance", "syncMember", member)

}

/**
 * Group is awaiting state assignment from the leader
 *
 * action: respond to heartbeats with REBALANCE_IN_PROGRESS
 *         respond to offset commits with REBALANCE_IN_PROGRESS
 *         park sync group requests from followers until transition to Stable
 *         allow offset fetch requests
 * transition: sync group with state assignment received from leader => Stable
 *             join group from new member or existing member with updated metadata => PreparingRebalance
 *             leave group from existing member => PreparingRebalance
 *             member failure detected => PreparingRebalance
 *             group is removed by partition emigration => Dead
 */
type completingRebalance struct {
	g *ConsumerGroup
}

func (s *completingRebalance) string() string {
	return "completingRebalance"
}

func (s *completingRebalance) addMember(member string, protocols []string) {
	log.Printf("%s - %s - %s", "completingRebalance", "addMember", member)

	// TODO: Optimization for a member who is already a member joining again -> do nothing.

	// TODO: Handle protocols.

	s.g.setState(s.g.preparingRebalanceState)

	// make all members unjoined
	for m := range s.g.membersJoined {
		s.g.membersJoined[m] = false
	}

	// this member is joined
	s.g.membersJoined[member] = true

	// if we have all the members joined...
	for _, v := range s.g.membersJoined {
		if !v {
			return
		}
	}

	// ...get ready to complete the rebalance.
	s.g.setState(s.g.completingRebalanceState)
}

func (s *completingRebalance) removeMember(member string) {
	log.Printf("%s - %s - %s", "completingRebalance", "removeMember", member)

	// Don't do anything if member is not a member.
	if _, ok := s.g.membersJoined[member]; !ok {
		return
	}

	s.g.setState(s.g.preparingRebalanceState)

	delete(s.g.membersJoined, member)

	// make all members unjoined
	for m := range s.g.membersJoined {
		s.g.membersJoined[m] = false
	}
}

func (s *completingRebalance) syncMember(member string, assignments map[string][]byte) {
	log.Printf("%s - %s - %s", "completingRebalance", "syncMember", member)

	// If member is not in the group, don't do anything
	var isMember bool
	for m := range s.g.membersJoined {
		if m == member {
			isMember = true
			break
		}
	}
	if !isMember {
		return
	}

	// Mark the member as syncd.
	s.g.membersSyncd[member] = true

	// If this is the leader, store the assignments.
	if member == s.g.leader {
		s.g.groupAssignments = assignments
	}

	// If all members have syncd, go to stable.
	for m := range s.g.membersJoined {
		if !s.g.membersSyncd[m] {
			return
		}
	}
	s.g.setState(s.g.stableState)
}

/**
 * Group is stable
 *
 * action: respond to member heartbeats normally
 *         respond to sync group from any member with current assignment
 *         respond to join group from followers with matching metadata with current group metadata
 *         allow offset commits from member of current generation
 *         allow offset fetch requests
 * transition: member failure detected via heartbeat => PreparingRebalance
 *             leave group from existing member => PreparingRebalance
 *             leader join-group received => PreparingRebalance
 *             follower join-group with new metadata => PreparingRebalance
 *             group is removed by partition emigration => Dead
 */
type stable struct {
	g *ConsumerGroup
}

func (s *stable) string() string {
	return "stable"
}

func (s *stable) addMember(member string, protocols []string) {
	log.Printf("%s - %s - %s", "stable", "addMember", member)

	// TODO: Optimization for a member who is already a member joining again -> do nothing.

	// TODO: Handle protocols.

	// Make all members unjoined
	for m := range s.g.membersJoined {
		s.g.membersJoined[m] = false
	}

	// this member is joined
	s.g.membersJoined[member] = true

	// If we have all the members joined...
	for _, v := range s.g.membersJoined {
		if !v {
			s.g.setState(s.g.preparingRebalanceState)
			return
		}
	}

	// ...get ready to complete the rebalance.
	s.g.setState(s.g.completingRebalanceState)
}

func (s *stable) removeMember(member string) {
	log.Printf("%s - %s - %s", "stable", "removeMember", member)
	// Don't do anything if member is not a member.
	if _, ok := s.g.membersJoined[member]; !ok {
		return
	}

	delete(s.g.membersJoined, member)

	// Make all members unjoined
	for m := range s.g.membersJoined {
		s.g.membersJoined[m] = false
	}
	s.g.setState(s.g.preparingRebalanceState)
}

func (s *stable) syncMember(member string, assignments map[string][]byte) {}

func NewConsumerGroup(id string) *ConsumerGroup {
	g := &ConsumerGroup{
		id:            id,
		membersJoined: make(map[string]bool),
		membersSyncd:  make(map[string]bool),
	}

	g.preparingRebalanceState = &preparingRebalance{g}
	g.completingRebalanceState = &completingRebalance{g}
	g.stableState = &stable{g}

	g.setState(g.preparingRebalanceState)

	return g
}

// handle state transitions
func (g *ConsumerGroup) setState(newState state) {
	// TODO: Increment generation ID.

	// TODO: Handle invalid state transitions.

	switch newState.(type) {
	case *preparingRebalance:
		g.leader = ""
		for k := range g.membersSyncd {
			delete(g.membersSyncd, k)
		}
	case *completingRebalance:
		// We now know all the members, do pick a leader randomly
		for m := range g.membersJoined {
			g.leader = m
			break
		}
	}

	g.currentState = newState
}

// Only valid to be called in "completingRebalance" state.
func (g *ConsumerGroup) LeaderAndMembers() (string, []string) {
	if g.currentState.string() != "completingRebalance" {
		return "", nil
	}

	members := []string{}
	for m := range g.membersJoined {
		members = append(members, m)
	}

	return g.leader, members
}

// Only valid to be called in "stable" state.
func (g *ConsumerGroup) GroupAssignments() map[string][]byte {
	if g.currentState.string() != "stable" {
		return nil
	}

	return g.groupAssignments
}

func (g *ConsumerGroup) AddMember(member string, protocols []string) {
	g.currentState.addMember(member, protocols)
}

func (g *ConsumerGroup) RemoveMember(member string) {
	g.currentState.removeMember(member)
}

func (g *ConsumerGroup) SyncMember(member string, assignments map[string][]byte) {
	g.currentState.syncMember(member, assignments)
}
