package protocol

type SaslHandshakeRequest struct {
	APIVersion int16 // TBD: Not used.
	Mechanism  string
}

func (r *SaslHandshakeRequest) Decode(d PacketDecoder, version int16) (err error) {
	if r.Mechanism, err = d.String(); err != nil {
		return err
	}

	return nil
}
