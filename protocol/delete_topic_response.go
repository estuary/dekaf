package protocol

import "time"

type DeleteTopicsResponse struct {
	APIVersion int16

	ThrottleTime    time.Duration
	TopicErrorCodes []*TopicErrorCode
}

func (c *DeleteTopicsResponse) Encode(e PacketEncoder) error {
	if c.APIVersion >= 1 {
		e.PutInt32(int32(c.ThrottleTime / time.Millisecond))
	}
	if err := e.PutArrayLength(len(c.TopicErrorCodes)); err != nil {
		return err
	}
	for _, t := range c.TopicErrorCodes {
		if err := e.PutString(t.Topic); err != nil {
			return err
		}
		e.PutInt16(t.ErrorCode)
	}
	return nil
}

func (c *DeleteTopicsResponse) Decode(d PacketDecoder, version int16) error {
	c.APIVersion = version
	if version >= 1 {
		throttle, err := d.Int32()
		if err != nil {
			return err
		}
		c.ThrottleTime = time.Duration(throttle) * time.Millisecond
	}
	l, err := d.ArrayLength()
	if err != nil {
		return err
	}
	c.TopicErrorCodes = make([]*TopicErrorCode, l)
	for i := range c.TopicErrorCodes {
		topic, err := d.String()
		if err != nil {
			return err
		}
		errorCode, err := d.Int16()
		if err != nil {
			return err
		}
		c.TopicErrorCodes[i] = &TopicErrorCode{
			Topic:     topic,
			ErrorCode: errorCode,
		}
	}
	return nil
}

func (r *DeleteTopicsResponse) Version() int16 {
	return r.APIVersion
}
