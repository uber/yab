package transportmiddleware

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/yarpc/yab/transport"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type headerTransportMiddleware struct {
	wantErr bool
}

func (tm headerTransportMiddleware) Apply(ctx context.Context, req *transport.Request) (*transport.Request, error) {
	req.Headers["foo"] = "bar"
	if tm.wantErr {
		return nil, errors.New("bad apply")
	}
	return req, nil
}

func TestTransportMiddleware(t *testing.T) {
	tests := []struct {
		dontRegister bool
		wantErr      bool
	}{
		{ /* run without options */ },
		{wantErr: true},
		{dontRegister: true},
	}
	for idx, tt := range tests {
		restore := func() {}

		// create the test middleware
		tm := &headerTransportMiddleware{
			wantErr: tt.wantErr,
		}
		if !tt.dontRegister {
			restore = Register(tm)
			registerLock.RLock()
			require.Equal(t, tm, registeredMiddleware)
			registerLock.RUnlock()
		}

		// create test request
		headers := map[string]string{"zim": "zam"}
		rawReq := &transport.Request{
			Headers: headers,
			Method:  "get",
		}

		// modify the test request
		req, err := Apply(context.TODO(), rawReq)
		restore()
		if tt.dontRegister {
			assert.NoError(t, err, "[%d] apply should not error", idx)
			_, ok := req.Headers["foo"]
			fmt.Println(req.Headers)
			assert.False(t, ok, "[%d] test middleware should not have applied", idx)
			continue
		}
		if tt.wantErr {
			assert.Error(t, err)
			continue
		}

		// verify previous values
		for k, v := range headers {
			assert.Equal(t, v, req.Headers[k], "[%d] previous header was not preserved", idx)
		}
		assert.Equal(t, "get", req.Method, "[%d] previous method was not preserved", idx)

		// verify modified values
		assert.Equal(t, "bar", req.Headers["foo"], "[%d] test middleware should have applied", idx)
	}
}

func TestRegisterRace(t *testing.T) {
	registerCh := make(chan struct{})
	restoreCh := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			<-registerCh // wait until register signal is received
			tm := &headerTransportMiddleware{}
			restore := Register(tm)

			<-restoreCh
			restore()
			wg.Done()
		}()
	}

	close(registerCh) // synchronize all calls to Register()
	close(restoreCh)
	wg.Wait()

	// check that middleware is nil now
	registerLock.RLock()
	require.Equal(t, nil, registeredMiddleware)
	registerLock.RUnlock()
}
