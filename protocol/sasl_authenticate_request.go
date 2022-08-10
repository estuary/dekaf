package protocol

type SaslAuthenticateRequest struct {
	SaslAuthBytes []byte
}

func (r *SaslAuthenticateRequest) Decode(d PacketDecoder, version int16) (err error) {
	if r.SaslAuthBytes, err = d.Bytes(); err != nil {
		return err
	}

	return nil
}
