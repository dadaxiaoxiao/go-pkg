package ratelimit

import (
	"context"
	"github.com/dadaxiaoxiao/go-pkg/accesslog"
	"github.com/dadaxiaoxiao/go-pkg/grpcx"
	"github.com/dadaxiaoxiao/go-pkg/internal/api/user"
	"github.com/dadaxiaoxiao/go-pkg/ratelimit"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	etcdv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/resolver"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"math/rand"
	"testing"
	"time"
)

type InterceptorTestSuite struct {
	suite.Suite
	etcdClient *etcdv3.Client
	log        accesslog.Logger
	limiter    ratelimit.Limiter
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

	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6389",
	})

	s.limiter = ratelimit.NewRedisSlideWindowLimiter(redisClient,
		ratelimit.WithInterval(time.Second),
		ratelimit.WithRate(5))
}

func TestInterceptor(t *testing.T) {
	suite.Run(t, new(InterceptorTestSuite))
}

func (s *InterceptorTestSuite) TestGrpcServer() {
	svc := &Server{}
	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			NewInterceptorBuilder(s.limiter, "qinye_demo:request:count", s.log).BuildUnaryServerInterceptor(),
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
	t := s.T()
	bd, err := resolver.NewBuilder(s.etcdClient)
	require.NoError(s.T(), err)
	conn, err := grpc.Dial("etcd:///service/demo_user",
		grpc.WithResolvers(bd),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.NoError(t, err)
	client := user.NewUserServiceClient(conn)
	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		resp, err := client.GetById(ctx, &user.GetByIdRequest{
			Id: 1,
		})
		cancel()
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

func (s *Server) GetById(context.Context, *user.GetByIdRequest) (*user.GetByIdResponse, error) {
	time.Sleep(time.Millisecond * time.Duration(rand.Int31n(50)))
	return &user.GetByIdResponse{
		User: &user.User{
			Id:   1,
			Name: "qinye",
		},
	}, nil
}
