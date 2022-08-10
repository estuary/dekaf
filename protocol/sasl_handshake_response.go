package protocol

type SaslHandshakeResponse struct {
	ErrorCode         int16
	EnabledMechanisms []string
}

func (r *SaslHandshakeResponse) Encode(e PacketEncoder) (err error) {
	e.PutInt16(int16(r.ErrorCode))
	return e.PutStringArray(r.EnabledMechanisms)
}

func (r *SaslHandshakeResponse) Decode(d PacketDecoder, version int16) (err error) {
	return nil
}
