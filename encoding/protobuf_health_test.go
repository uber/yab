package encoding

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yarpc/yab/transport"
)

func TestProtobufHealth(t *testing.T) {
	s := &protoHealthSerializer{}
	assert.Equal(t, Protobuf, s.Encoding())
}

func TestProtobufHealthRequest(t *testing.T) {
	tests := []struct {
		desc        string
		serviceName string
		reqBody     []byte
		bsOut       []byte
		errMsg      string
	}{
		{
			desc:  "empty",
			bsOut: []byte{},
		},
		{
			desc:        "no input",
			serviceName: "x",
			reqBody:     []byte("x"),
			errMsg:      "cannot specify --health and a request body",
		},
		{
			desc:        "pass",
			serviceName: "x",
			bsOut:       []byte{0xa, 0x1, 0x78},
		},
		{
			desc:        "fail",
			serviceName: string([]byte{0xc3, 0x28}),
			errMsg:      "invalid UTF-8",
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			serializer := &protoHealthSerializer{serviceName: tt.serviceName}
			got, err := serializer.Request(tt.reqBody)
			if tt.errMsg == "" {
				assert.NoError(t, err, "%v", tt.desc)
				require.NotNil(t, got, "%v: Invalid request")
				assert.Equal(t, tt.bsOut, got.Body)
			} else {
				assert.Nil(t, got, "%v: Error cases should not return any bytes", tt.desc)
				require.Error(t, err, "%v", tt.desc)
				assert.Contains(t, err.Error(), tt.errMsg, "%v: invalid error", tt.desc)
			}
		})
	}
}

func TestProtobufHealthResponse(t *testing.T) {
	tests := []struct {
		desc      string
		reqBody   []byte
		outAsJSON string
		errMsg    string
	}{
		{
			desc:      "pass",
			reqBody:   nil,
			outAsJSON: "{}",
		},
		{
			desc:    "fail",
			reqBody: []byte{0x1, 0x2},
			errMsg:  "cannot parse invalid wire-format data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			serializer := &protoHealthSerializer{}
			response := &transport.Response{
				Body: tt.reqBody,
			}
			got, err := serializer.Response(response)
			if tt.errMsg == "" {
				assert.NoError(t, err, "%v", tt.desc)
				assert.NotNil(t, got, "%v: Invalid request")
				r, err := json.Marshal(got)
				assert.NoError(t, err)
				assert.JSONEq(t, tt.outAsJSON, string(r))

				err = serializer.CheckSuccess(response)
				assert.NoError(t, err)
			} else {
				assert.Nil(t, got, "%v: Error cases should not return any bytes", tt.desc)
				require.Error(t, err, "%v", tt.desc)
				assert.Contains(t, err.Error(), tt.errMsg, "%v: invalid error", tt.desc)
			}
		})
	}
}
