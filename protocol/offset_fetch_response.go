package protocol

type OffsetFetchTopicResponse struct {
	Topic      string
	Partitions []OffsetFetchPartition
	ErrorCode  int16
}

type OffsetFetchPartition struct {
	Partition int32
	Offset    int64
	Metadata  *string
	ErrorCode int16
}

type OffsetFetchResponse struct {
	APIVersion int16

	Responses []OffsetFetchTopicResponse
}

func (r *OffsetFetchResponse) Encode(e PacketEncoder) (err error) {
	if err := e.PutArrayLength(len(r.Responses)); err != nil {
		return err
	}
	for _, resp := range r.Responses {
		if err := e.PutString(resp.Topic); err != nil {
			return err
		}
		if err := e.PutArrayLength(len(resp.Partitions)); err != nil {
			return err
		}
		for _, p := range resp.Partitions {
			e.PutInt32(p.Partition)
			e.PutInt64(p.Offset)
			if err := e.PutNullableString(p.Metadata); err != nil {
				return err
			}
			e.PutInt16(p.ErrorCode)
		}
		if r.APIVersion >= 2 {
			e.PutInt16(resp.ErrorCode)
		}
	}

	return nil
}

func (r *OffsetFetchResponse) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version

	responses, err := d.ArrayLength()
	if err != nil {
		return err
	}
	r.Responses = make([]OffsetFetchTopicResponse, responses)
	for _, resp := range r.Responses {
		if resp.Topic, err = d.String(); err != nil {
			return err
		}
		partitions, err := d.ArrayLength()
		if err != nil {
			return err
		}
		resp.Partitions = make([]OffsetFetchPartition, partitions)
		for i, p := range resp.Partitions {
			if p.Partition, err = d.Int32(); err != nil {
				return err
			}
			if p.Offset, err = d.Int64(); err != nil {
				return err
			}
			if p.Metadata, err = d.NullableString(); err != nil {
				return err
			}
			if p.ErrorCode, err = d.Int16(); err != nil {
				return err
			}
			resp.Partitions[i] = p
		}
	}
	return nil
}

func (r *OffsetFetchResponse) Version() int16 {
	return r.APIVersion
}
