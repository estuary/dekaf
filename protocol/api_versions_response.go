package protocol

import "time"

type APIVersionsResponse struct {
	APIVersion int16

	ErrorCode    int16
	APIVersions  []APIVersion
	ThrottleTime time.Duration
}

type APIVersion struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}

// Version 4 format
func (c *APIVersionsResponse) Encode(e PacketEncoder) error {
	e.PutInt16(c.ErrorCode)

	if err := e.PutArrayVarLength(len(c.APIVersions)); err != nil {
		return err
	}
	for _, av := range c.APIVersions {
		e.PutInt16(av.APIKey)
		e.PutInt16(av.MinVersion)
		e.PutInt16(av.MaxVersion)
		e.PutInt8(0) // Tagged Fields?
	}
	if c.APIVersion >= 1 {
		e.PutInt32(int32(c.ThrottleTime / time.Millisecond))
	}
	return nil
}

func (c *APIVersionsResponse) Decode(d PacketDecoder, version int16) error {
	c.APIVersion = version
	l, err := d.ArrayLength()
	if err != nil {
		return err
	}
	c.APIVersions = make([]APIVersion, l)
	for i := range c.APIVersions {
		key, err := d.Int16()
		if err != nil {
			return err
		}

		minVersion, err := d.Int16()
		if err != nil {
			return err
		}

		maxVersion, err := d.Int16()
		if err != nil {
			return err
		}

		c.APIVersions[i] = APIVersion{
			APIKey:     key,
			MinVersion: minVersion,
			MaxVersion: maxVersion,
		}
	}
	if version >= 1 {
		throttle, err := d.Int32()
		if err != nil {
			return err
		}
		c.ThrottleTime = time.Duration(throttle) * time.Millisecond
	}
	return nil
}

func (r *APIVersionsResponse) Version() int16 {
	return r.APIVersion
}
