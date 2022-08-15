package coordinator

import (
	"fmt"

	"github.com/estuary/dekaf/protocol"
)

// mostly proxies through to the store, but will take care of blocking until a group leader is
// assigned.

// must watch for heartbeat expirations in the store

// better to not use protocol.* things once we get there

type groupStore interface {
	AddMemberToGroup(member string, group string, protocols []string)
	RemoveMemberFromGroup(member string, group string)
	SyncMemberForGroup(member string, group string, assignments map[string][]byte)
	HeartbeatForMemberInGroup(member string, group string)
	AssignedLeaderForGroup(group string) (string, []string)
	GroupSyncCompleted(member string, group string) []byte // Assignment bytes for this group member.
}

type GroupCoordinator struct {
	store groupStore
}

func NewGroupCoordinator(store groupStore) *GroupCoordinator {
	// TODO: watch for store heartbeat expirations

	return &GroupCoordinator{
		store: store,
	}
}

// heartbeats, sync group, leave group, join group, offset commit, offset fetch

// TODO: Placeholder
var lastID int = 1

func (c *GroupCoordinator) AddMemberToGroup(req *protocol.JoinGroupRequest) (string, string, []string) {
	member := req.MemberID
	group := req.GroupID
	protocols := req.GroupProtocols

	// TODO: Need to do something with the protocol metadata.
	protoNames := []string{}
	for _, p := range protocols {
		protoNames = append(protoNames, p.ProtocolName)
	}

	// If member is empty, generate a member ID
	if member == "" {
		member = fmt.Sprintf("member_%d", lastID)
		lastID++
	}

	c.store.AddMemberToGroup(member, group, protoNames)

	// This will block until a leader is assigned.
	leader, members := c.store.AssignedLeaderForGroup(group)

	return member, leader, members
}

func (c *GroupCoordinator) RemoveMemberFromGroup(req *protocol.SyncGroupRequest) {
	member := req.MemberID
	group := req.GroupID

	c.store.RemoveMemberFromGroup(member, group)
}

func (c *GroupCoordinator) SyncMemberForGroup(req *protocol.SyncGroupRequest) []byte {
	member := req.MemberID
	group := req.GroupID
	assignments := req.GroupAssignments

	as := make(map[string][]byte)
	for _, a := range assignments {
		as[a.MemberID] = a.MemberAssignment
	}

	c.store.SyncMemberForGroup(member, group, as)

	// Gets the assignment for this specific member. Blocks until the sync is completed.
	assignment := c.store.GroupSyncCompleted(member, group)

	return assignment
}

func (c *GroupCoordinator) HeartbeatForMemberInGroup(req *protocol.SyncGroupRequest) []byte {
	member := req.MemberID
	group := req.GroupID
	assignments := req.GroupAssignments

	as := make(map[string][]byte)
	for _, a := range assignments {
		as[a.MemberID] = a.MemberAssignment
	}

	c.store.SyncMemberForGroup(member, group, as)

	// Gets the assignment for this specific member. Blocks until the sync is completed.
	assignment := c.store.GroupSyncCompleted(member, group)

	return assignment
}
