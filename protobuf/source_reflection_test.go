package protobuf

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ygrpc "go.uber.org/yarpc/transport/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	rpb "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
)

func TestReflection(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	s := grpc.NewServer()
	reflection.Register(s)
	go s.Serve(ln)

	// Ensure that all streams are closed by the end of the test.
	defer s.GracefulStop()

	source, err := NewDescriptorProviderReflection(ReflectionArgs{
		Timeout: time.Second,
		Peers:   []string{ln.Addr().String()},
	})
	require.NoError(t, err)
	require.NotNil(t, source)

	// Close the streaming reflect call to ensure GracefulStop doesn't block.
	defer source.Close()

	t.Run("valid service", func(t *testing.T) {
		result, err := source.FindService("grpc.reflection.v1alpha.ServerReflection")
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("non-service symbol", func(t *testing.T) {
		result, err := source.FindService("grpc.reflection.v1alpha.ServerReflectionRequest")
		require.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), `could not find gRPC service`)
	})

	t.Run("no such symbol", func(t *testing.T) {
		result, err := source.FindService("wat")
		require.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), `could not find gRPC service "wat"`)
	})

	t.Run("valid message", func(t *testing.T) {
		msg, err := source.FindMessage("grpc.reflection.v1alpha.ServerReflectionRequest")
		assert.NoError(t, err)
		assert.NotNil(t, msg)
		assert.IsType(t, &desc.MessageDescriptor{}, msg)
	})

	t.Run("return nil if the message type is not found", func(t *testing.T) {
		msg, err := source.FindMessage("not-to-be-found")
		assert.NoError(t, err)
		assert.Nil(t, msg)
	})
}

func TestReflectionWithProtocolInPeer(t *testing.T) {
	source, err := NewDescriptorProviderReflection(ReflectionArgs{
		Timeout: time.Second,
		Peers:   []string{"grpc://127.0.0.1:12345"},
	})
	assert.Nil(t, source)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "peer contains scheme")
}

func TestReflectionClosedPort(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "failed to listen on a port")
	ln.Close()

	got, err := NewDescriptorProviderReflection(ReflectionArgs{
		Timeout: time.Second,
		Peers:   []string{ln.Addr().String()},
	})

	assert.Contains(t, err.Error(), "could not reach reflection server")
	assert.Nil(t, got)
}

func TestReflectionNotRegistered(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	s := grpc.NewServer()
	go s.Serve(ln)

	// Ensure that all streams are closed by the end of the test.
	defer s.GracefulStop()

	got, err := NewDescriptorProviderReflection(ReflectionArgs{
		Timeout: time.Second,
		Peers:   []string{ln.Addr().String()},
	})
	require.NoError(t, err, "failed to create reflection provider")

	_, err = got.FindService("foo")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown service grpc.reflection.v1alpha.ServerReflection", "unexpected error")
}

func TestReflectionRoutingHeaders(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	s := grpc.NewServer()
	drs := &dummyReflectionServer{}
	rpb.RegisterServerReflectionServer(s, drs)
	go s.Serve(ln)

	// Ensure that all streams are closed by the end of the test.
	defer s.GracefulStop()

	baseMD := metadata.Pairs(
		ygrpc.CallerHeader, "test-caller",
		ygrpc.ServiceHeader, "test-service",
		ygrpc.EncodingHeader, "proto",
	)
	tests := []struct {
		msg  string
		rd   string
		rk   string
		want map[string]string
	}{
		{
			msg:  "no routing delegate or key",
			want: nil,
		},
		{
			msg:  "only routing delegate",
			rd:   "test-rd",
			want: map[string]string{ygrpc.RoutingDelegateHeader: "test-rd"},
		},
		{
			msg:  "only routing key",
			rk:   "test-rk",
			want: map[string]string{ygrpc.RoutingKeyHeader: "test-rk"},
		},
		{
			msg: "routing delegate & key",
			rd:  "test-rd",
			rk:  "test-rk",
			want: map[string]string{
				ygrpc.RoutingDelegateHeader: "test-rd",
				ygrpc.RoutingKeyHeader:      "test-rk",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.msg, func(t *testing.T) {
			drs.Reset()

			source, err := NewDescriptorProviderReflection(ReflectionArgs{
				Timeout:         time.Second,
				Peers:           []string{ln.Addr().String()},
				Caller:          "test-caller",
				Service:         "test-service",
				RoutingDelegate: tt.rd,
				RoutingKey:      tt.rk,
			})
			require.NoError(t, err)
			defer source.Close()

			_, err = source.FindService("foo")
			require.Error(t, err, "expected error from stub server")
			assert.Contains(t, err.Error(), assert.AnError.Error(), "unexpected error")

			for k := range baseMD {
				assert.Equal(t, baseMD.Get(k), drs.md.Get(k), "unexpected values for header %v", k)
			}
			for k := range tt.want {
				assert.Equal(t, []string{tt.want[k]}, drs.md.Get(k), "unexpected values for header %v", k)
			}
		})
	}
}

func TestResolverAlreadyExists(t *testing.T) {
	ln, err := net.Listen("tcp", "localhost:0")
	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	require.NoError(t, err)

	defer ln.Close()
	defer ln2.Close()

	s := grpc.NewServer()
	reflection.Register(s)

	go s.Serve(ln)
	go s.Serve(ln2)

	// Ensure that all streams are closed by the end of the test.
	defer s.GracefulStop()

	provider, err := NewDescriptorProviderReflection(ReflectionArgs{
		Timeout: time.Second,
		Peers:   []string{ln.Addr().String()},
		Service: "test",
	})
	require.NoError(t, err, "failed to create reflection provider")
	_, err = provider.FindService("grpc.reflection.v1alpha.ServerReflection")
	assert.NoError(t, err, "unexpected error")

	provider, err = NewDescriptorProviderReflection(ReflectionArgs{
		Timeout: time.Second,
		Peers:   []string{ln2.Addr().String()},
		Service: "test",
	})
	require.NoError(t, err, "failed to create reflection provider")
	_, err = provider.FindService("grpc.reflection.v1alpha.ServerReflection")
	assert.NoError(t, err, "unexpected error")

}

func TestE2eErrors(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	s := grpc.NewServer()
	drs := &dummyReflectionServer{returnErr: errors.New("test error")}
	rpb.RegisterServerReflectionServer(s, drs)
	go s.Serve(ln)

	// Ensure that all streams are closed by the end of the test.
	defer s.GracefulStop()

	source, err := NewDescriptorProviderReflection(ReflectionArgs{
		Timeout: time.Second,
		Peers:   []string{ln.Addr().String()},
		Service: "TestE2eErrors",
	})
	require.NoError(t, err)
	defer source.Close()

	t.Run("test error handling in find method", func(t *testing.T) {
		desc, err := source.FindMessage("test")
		require.Nil(t, desc, "unexpected message descriptor")
		assert.EqualError(t, err, "error in protobuf reflection: rpc error: code = Unknown desc = test error")
	})

	t.Run("test error handling in find service", func(t *testing.T) {
		desc, err := source.FindService("test")
		require.Nil(t, desc, "unexpected message descriptor")
		assert.EqualError(t, err, "error in protobuf reflection: rpc error: code = Unknown desc = test error")
	})
}

type dummyReflectionServer struct {
	md        metadata.MD
	returnErr error
}

func (s *dummyReflectionServer) Reset() {
	s.md = nil
}

func (s *dummyReflectionServer) ServerReflectionInfo(r rpb.ServerReflection_ServerReflectionInfoServer) error {
	if s.returnErr != nil {
		return s.returnErr
	}

	if md, ok := metadata.FromIncomingContext(r.Context()); ok {
		s.md = md
	}
	return assert.AnError
}
