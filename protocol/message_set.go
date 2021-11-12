package protocol

// https://kafka.apache.org/documentation/#messageset
type MessageSet struct {
	Offset                  int64
	Size                    int32
	Message                 *Message
	PartialTrailingMessages bool
}

func (ms *MessageSet) Encode(e PacketEncoder) error {
	e.PutInt64(ms.Offset)
	e.Push(&SizeField{})
	if err := ms.Message.Encode(e); err != nil {
		return err
	}
	e.Pop()
	return nil
}

func (ms *MessageSet) Decode(d PacketDecoder) error {
	var err error
	if ms.Offset, err = d.Int64(); err != nil {
		return err
	}
	if ms.Size, err = d.Int32(); err != nil {
		return err
	}
	if err = ms.Message.Decode(d); err != nil {
		return err
	}
	return nil
}
