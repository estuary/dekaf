package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFindCoordinatorRequest(t *testing.T) {
	req := require.New(t)
	exp := &FindCoordinatorRequest{
		APIVersion:      1,
		CoordinatorKey:  "coord-key",
		CoordinatorType: 1,
	}
	b, err := Encode(exp)
	req.NoError(err)
	var act FindCoordinatorRequest
	err = Decode(b, &act, exp.Version())
	req.NoError(err)
	req.Equal(exp, &act)
}
