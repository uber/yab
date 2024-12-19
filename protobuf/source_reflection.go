package protobuf

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/grpcreflect"
	"github.com/yarpc/yab/encoding/encodingerror"
	yproto "go.uber.org/yarpc/encoding/protobuf"
	ygrpc "go.uber.org/yarpc/transport/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	rpb "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
)

// ReflectionArgs are args for constructing a DescriptorProvider that reaches out to a reflection server.
type ReflectionArgs struct {
	Caller          string
	Service         string
	RoutingDelegate string
	RoutingKey      string
	Peers           []string
	Timeout         time.Duration
}

// NewDescriptorProviderReflection returns a DescriptorProvider that reaches
// out to a reflection server to access file descriptors.
func NewDescriptorProviderReflection(args ReflectionArgs) (DescriptorProvider, error) {
	r, deregisterScheme := GenerateAndRegisterManualResolver()
	defer deregisterScheme()
	peers := make([]resolver.Address, len(args.Peers))
	for i, p := range args.Peers {
		if strings.Contains(p, "://") {
			return nil, fmt.Errorf("peer contains scheme %q", p)
		}
		peers[i] = resolver.Address{Addr: p, Type: resolver.Backend}
	}
	r.InitialState(resolver.State{Addresses: peers})

	ctx, cancel := context.WithTimeout(context.Background(), args.Timeout)
	conn, err := grpc.DialContext(
		ctx,
		r.Scheme()+":///", // minimal target to dial registered host:port pairs
		grpc.WithAuthority(strings.Join(args.Peers, ",")),
		grpc.WithBlock(),
		grpc.WithInsecure())
	if err != nil {
		cancel()
		return nil, fmt.Errorf("could not reach reflection server: %s", err)
	}
	pbClient := rpb.NewServerReflectionClient(conn)

	routingHeaders := metadata.Pairs(
		ygrpc.CallerHeader, args.Caller,
		ygrpc.ServiceHeader, args.Service,
		ygrpc.EncodingHeader, string(yproto.Encoding),
	)
	if args.RoutingDelegate != "" {
		routingHeaders.Append(ygrpc.RoutingDelegateHeader, args.RoutingDelegate)
	}
	if args.RoutingKey != "" {
		routingHeaders.Append(ygrpc.RoutingKeyHeader, args.RoutingKey)
	}

	metadataContext := metadata.NewOutgoingContext(ctx, routingHeaders)
	return &grpcreflectSource{
		client:     grpcreflect.NewClient(metadataContext, pbClient),
		cancelFunc: cancel,
	}, nil
}

type grpcreflectSource struct {
	client     *grpcreflect.Client
	cancelFunc context.CancelFunc
}

func (s *grpcreflectSource) FindMessage(messageType string) (*desc.MessageDescriptor, error) {
	msg, err := s.client.ResolveMessage(messageType)

	if grpcreflect.IsElementNotFoundError(err) {
		// If we couldn't find the message through the client,
		// return nil instead to follow the contract
		return nil, nil
	}

	if err != nil {
		return nil, wrapReflectionError(err)
	}

	return msg, err
}

func (s *grpcreflectSource) FindService(fullyQualifiedName string) (*desc.ServiceDescriptor, error) {
	service, err := s.client.ResolveService(fullyQualifiedName)
	if err != nil {
		if !grpcreflect.IsElementNotFoundError(err) {
			return nil, wrapReflectionError(err)
		}

		available, availableErr := s.client.ListServices()
		if availableErr != nil && !grpcreflect.IsElementNotFoundError(availableErr) {
			return nil, wrapReflectionError(availableErr)
		}

		return nil, encodingerror.NotFound{
			Encoding:   "gRPC",
			SearchType: "service",
			Search:     fullyQualifiedName,
			Example:    "--method Service/Method",
			Available:  available,
		}
	}

	return service, nil
}

func (s *grpcreflectSource) Close() {
	s.cancelFunc()
	s.client.Reset()
}

func wrapReflectionError(err error) error {
	return fmt.Errorf("error in protobuf reflection: %v", err)
}

func GenerateAndRegisterManualResolver() (*manual.Resolver, func()) {
	scheme := strconv.FormatInt(time.Now().UnixNano(), 36)
	r := manual.NewBuilderWithScheme(scheme)
	resolver.Register(r)
	return r, func() { resolver.UnregisterForTesting(scheme) }
}
