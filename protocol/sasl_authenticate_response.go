package protocol

type SaslAuthenticateResponse struct {
	ErrorCode         int16
	ErrMsg            *string
	SaslAuthBytes     []byte
	SessionLifetimeMs int64
}

func (r *SaslAuthenticateResponse) Encode(e PacketEncoder) (err error) {
	e.PutInt16(int16(r.ErrorCode))

	if err := e.PutNullableString(r.ErrMsg); err != nil {
		return err
	}

	if err := e.PutBytes(r.SaslAuthBytes); err != nil {
		return err
	}
	e.PutInt64(r.SessionLifetimeMs)

	return nil
}

func (r *SaslAuthenticateResponse) Decode(d PacketDecoder, version int16) (err error) {
	return nil
}
