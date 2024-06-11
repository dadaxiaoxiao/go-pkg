package trace

import (
	"context"
	"github.com/dadaxiaoxiao/go-pkg/accesslog"
	"github.com/dadaxiaoxiao/go-pkg/grpcx"
	"github.com/dadaxiaoxiao/go-pkg/internal/api/user"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	etcdv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/resolver"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/zipkin"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"math/rand"
	"testing"
	"time"
)

type InterceptorTestSuite struct {
	suite.Suite
	etcdClient *etcdv3.Client
	log        accesslog.Logger
}

func (s *InterceptorTestSuite) SetupSuite() {
	initZipkin()

	client, err := etcdv3.New(etcdv3.Config{
		Endpoints: []string{"localhost:12379"},
	})
	require.NoError(s.T(), err)
	s.etcdClient = client

	log, err := zap.NewDevelopment()
	require.NoError(s.T(), err)
	s.log = accesslog.NewZapLogger(log)
}

func TestInterceptor(t *testing.T) {
	suite.Run(t, new(InterceptorTestSuite))
}

func (s *InterceptorTestSuite) TestGrpcServer() {
	svc := &Server{}
	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			NewInterceptorBuilder(nil, nil).BuildServer(),
		))
	svc.Register(server)

	grpc_server := &grpcx.Server{
		Server:     server,
		Port:       8082,
		Name:       "demo_user",
		Log:        s.log,
		EtcdTTL:    60,
		EtcdClient: s.etcdClient,
	}
	err := grpc_server.Serve()
	if err != nil {
		// 启动失败，或者退出了服务器
		s.T().Log("退出 gRPC 服务", err)
	}
}

func (s *InterceptorTestSuite) TestGrpcClient() {
	t := s.T()
	bd, err := resolver.NewBuilder(s.etcdClient)
	require.NoError(s.T(), err)
	conn, err := grpc.Dial("etcd:///service/demo_user",
		grpc.WithResolvers(bd),
		grpc.WithChainUnaryInterceptor(
			NewInterceptorBuilder(nil, nil).BuildClient(),
		),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.NoError(t, err)
	client := user.NewUserServiceClient(conn)
	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		header := metadata.Pairs(
			"app", "qinye_demo",
			"client-ip", "localhost")
		ctx = metadata.NewOutgoingContext(ctx, header)

		spanCtx, span := otel.Tracer("interceptor_test").Start(ctx, "client_getbyid")
		resp, err := client.GetById(spanCtx, &user.GetByIdRequest{
			Id: 1,
		})
		cancel()
		// 模拟复杂的业务
		time.Sleep(time.Millisecond * 10)
		span.End()
		assert.NoError(t, err)
		t.Log(resp.User)
	}
	time.Sleep(time.Second * 3)
}

type Server struct {
	user.UnimplementedUserServiceServer
}

func (s *Server) Register(server *grpc.Server) {
	user.RegisterUserServiceServer(server, s)
}

func (s *Server) GetById(ctx context.Context, req *user.GetByIdRequest) (*user.GetByIdResponse, error) {

	ctx, span := otel.Tracer("user_server").Start(ctx, "get_by_id_biz")
	defer span.End()
	time.Sleep(time.Millisecond * time.Duration(rand.Int31n(100)))

	return &user.GetByIdResponse{
		User: &user.User{
			Id:   1,
			Name: "qinye",
		},
	}, nil
}

func initZipkin() {
	res, err := newResource("qinye_demo", "v0.0.1")
	if err != nil {
		panic(err)
	}
	prop := newPropagator()
	// 在客户端和服务端之间传递 tracing 的相关信息
	otel.SetTextMapPropagator(prop)
	tp, err := newTraceProvider(res)
	otel.SetTracerProvider(tp)
}

func newResource(serviceName, serviceVersion string) (*resource.Resource, error) {
	return resource.Merge(resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL,
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(serviceVersion)))
}

func newTraceProvider(res *resource.Resource) (*trace.TracerProvider, error) {
	exporter, err := zipkin.New(
		"http://localhost:9411/api/v2/spans")
	if err != nil {
		return nil, err
	}

	traceProvider := trace.NewTracerProvider(
		trace.WithBatcher(exporter,
			// Default is 5s. Set to 1s for demonstrative purposes.
			trace.WithBatchTimeout(time.Second)),
		trace.WithResource(res),
	)
	return traceProvider, nil
}

func newPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{})
}
