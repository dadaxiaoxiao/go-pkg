package prometheus

import (
	"context"
	"github.com/dadaxiaoxiao/go-pkg/accesslog"
	"github.com/dadaxiaoxiao/go-pkg/grpcx"
	"github.com/dadaxiaoxiao/go-pkg/internal/api/user"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	etcdv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/resolver"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"net/http"
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

func TestInterceptor(t *testing.T) {
	suite.Run(t, new(InterceptorTestSuite))
}

func initPromethues() {
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		// 暴露监听端口
		// 这里需要prometheus 监听 localhost:8081/metrics
		http.ListenAndServe(":8081", nil)
	}()
}

func (s *InterceptorTestSuite) TestGrpcServer() {
	initPromethues()

	svc := &Server{}
	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			NewInterceptorBuilder("dadaxiaoxiao", "demo").BuildServer(),
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
	grpc_server.Serve()
}

func (s *InterceptorTestSuite) TestGrpcClient() {
	bd, err := resolver.NewBuilder(s.etcdClient)
	require.NoError(s.T(), err)
	cc, err := grpc.Dial("etcd:///service/demo_user",
		// 使用 etcd 的服务发现
		grpc.WithResolvers(bd),
		grpc.WithTransportCredentials(insecure.NewCredentials()))

	assert.NoError(s.T(), err)

	client := user.NewUserServiceClient(cc)
	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		resp, err := client.GetById(ctx, &user.GetByIdRequest{
			Id: 1,
		})
		cancel()
		// 模拟复杂的业务
		time.Sleep(time.Millisecond * 10)
		assert.NoError(s.T(), err)
		s.T().Log(resp.User)
	}
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
