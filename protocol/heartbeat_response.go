package protocol

import "time"

type HeartbeatResponse struct {
	APIVersion int16

	ThrottleTime time.Duration
	ErrorCode    int16
}

func (r *HeartbeatResponse) Encode(e PacketEncoder) error {
	if r.APIVersion >= 1 {
		e.PutInt32(int32(r.ThrottleTime / time.Millisecond))
	}
	e.PutInt16(r.ErrorCode)
	return nil
}

func (r *HeartbeatResponse) Decode(d PacketDecoder, version int16) (err error) {
	r.APIVersion = version

	if version >= 1 {
		throttle, err := d.Int32()
		if err != nil {
			return err
		}
		r.ThrottleTime = time.Duration(throttle) / time.Millisecond
	}
	r.ErrorCode, err = d.Int16()
	return err
}

func (r *HeartbeatResponse) Key() int16 {
	return HeartbeatKey
}

func (r *HeartbeatResponse) Version() int16 {
	return r.APIVersion
}
