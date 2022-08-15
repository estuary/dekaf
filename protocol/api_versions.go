package protocol

// This is basically the same response returned by Kafka 0.10
var APIVersions = []APIVersion{
	{APIKey: ProduceKey, MinVersion: 0, MaxVersion: 2},
	{APIKey: FetchKey, MinVersion: 0, MaxVersion: 3},
	{APIKey: OffsetsKey, MinVersion: 0, MaxVersion: 1},
	{APIKey: MetadataKey, MinVersion: 0, MaxVersion: 2},
	{APIKey: LeaderAndISRKey, MinVersion: 0, MaxVersion: 0},
	{APIKey: StopReplicaKey, MinVersion: 0, MaxVersion: 0},
	{APIKey: UpdateMetadataKey, MinVersion: 0, MaxVersion: 3},
	{APIKey: ControlledShutdownKey, MinVersion: 0, MaxVersion: 1},
	{APIKey: OffsetCommitKey, MinVersion: 0, MaxVersion: 2},
	{APIKey: OffsetFetchKey, MinVersion: 0, MaxVersion: 2},
	{APIKey: FindCoordinatorKey, MinVersion: 0, MaxVersion: 0},
	{APIKey: JoinGroupKey, MinVersion: 0, MaxVersion: 1},
	{APIKey: HeartbeatKey, MinVersion: 0, MaxVersion: 1},
	{APIKey: LeaveGroupKey, MinVersion: 0, MaxVersion: 0},
	{APIKey: SyncGroupKey, MinVersion: 0, MaxVersion: 1},
	{APIKey: DescribeGroupsKey, MinVersion: 0, MaxVersion: 0},
	{APIKey: ListGroupsKey, MinVersion: 0, MaxVersion: 0},
	{APIKey: APIVersionsKey, MinVersion: 0, MaxVersion: 0},
	{APIKey: CreateTopicsKey, MinVersion: 0, MaxVersion: 1},
	{APIKey: DeleteTopicsKey, MinVersion: 0, MaxVersion: 0},
}

var APIVersionsLimited = []APIVersion{
	{APIKey: ProduceKey, MinVersion: 0, MaxVersion: 2}, // Python beam connector requires this, although it does not use it.
	{APIKey: FetchKey, MinVersion: 0, MaxVersion: 3},
	{APIKey: OffsetsKey, MinVersion: 0, MaxVersion: 1},
	{APIKey: MetadataKey, MinVersion: 0, MaxVersion: 2},
	{APIKey: APIVersionsKey, MinVersion: 0, MaxVersion: 0},
}
