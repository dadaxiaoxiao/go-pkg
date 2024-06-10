package logging

import (
	"context"
	"github.com/dadaxiaoxiao/go-pkg/accesslog"
	"github.com/dadaxiaoxiao/go-pkg/grpcx"
	"github.com/dadaxiaoxiao/go-pkg/internal/api/user"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	etcdv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/resolver"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"testing"
	"time"
)

type InterceptorTestSuite struct {
	suite.Suite
	etcdClient *etcdv3.Client
	log        accesslog.Logger
}

func (s *InterceptorTestSuite) SetupSuite() {
	client, err := etcdv3.New(etcdv3.Config{
		Endpoints: []string{"localhost:12379"},
	})
	require.NoError(s.T(), err)
	s.etcdClient = client

	log, err := zap.NewDevelopment()
	require.NoError(s.T(), err)
	s.log = accesslog.NewZapLogger(log)
}

func (s *InterceptorTestSuite) TestGrpcServer() {
	svc := &Server{}

	server := grpc.NewServer(grpc.ChainUnaryInterceptor(
		// 日志拦截器
		NewInterceptorBuilder(s.log).BuildServer(),
	))

	svc.Register(server)

	grpc_server := &grpcx.Server{
		Server:     server,
		Port:       8081,
		Name:       "demo_user",
		Log:        s.log,
		EtcdTTL:    60,
		EtcdClient: s.etcdClient,
	}
	grpc_server.Serve()
}

func (s *InterceptorTestSuite) TestGrpcClient() {
	bd, err := resolver.NewBuilder(s.etcdClient)
	require.NoError(s.T(), err)
	cc, err := grpc.Dial("etcd:///service/demo_user",
		// 使用 etcd 的服务发现
		grpc.WithResolvers(bd),
		grpc.WithTransportCredentials(insecure.NewCredentials()))

	client := user.NewUserServiceClient(cc)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)

	resp, err := client.GetById(ctx, &user.GetByIdRequest{
		Id: 1,
	})
	cancel()
	require.NoError(s.T(), err)
	s.T().Log(resp.User)
}

func TestInterceptor(t *testing.T) {
	suite.Run(t, new(InterceptorTestSuite))
}

type Server struct {
	user.UnimplementedUserServiceServer
}

func (s *Server) Register(server *grpc.Server) {
	user.RegisterUserServiceServer(server, s)
}

func (s *Server) GetById(context.Context, *user.GetByIdRequest) (*user.GetByIdResponse, error) {
	return &user.GetByIdResponse{
		User: &user.User{
			Id:   1,
			Name: "qinye",
		},
	}, nil
}
