package grpcx

import (
	"context"
	"github.com/dadaxiaoxiao/go-pkg/accesslog"
	"github.com/dadaxiaoxiao/go-pkg/netx"
	etcdv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/endpoints"
	"google.golang.org/grpc"
	"net"
	"strconv"
	"time"
)

type Server struct {
	*grpc.Server
	Port int
	Name string
	Log  accesslog.Logger

	// ETCD 服务注册租约 TTL
	EtcdTTL     int64
	EtcdClient  *etcdv3.Client
	etcdManager endpoints.Manager
	etcdKey     string
	cancel      func()
}

func NewServer(server *grpc.Server, port int,
	serverName string, log accesslog.Logger,
	etcdTTL int64, etcdClient *etcdv3.Client) *Server {
	return &Server{
		Server:     server,
		Port:       port,
		Name:       serverName,
		Log:        log,
		EtcdTTL:    etcdTTL,
		EtcdClient: etcdClient,
	}
}

func (s *Server) Serve() error {
	// 这里统一链路控制
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	port := strconv.Itoa(s.Port)
	// 监听 tcp ，端口
	l, err := net.Listen("tcp", ":"+port)

	if err != nil {
		return err
	}

	// 服务注册
	err = s.register(ctx, port)
	if err != nil {
		return err
	}

	// 这边会阻塞，类似与 gin.Run
	return s.Server.Serve(l)
}

// register 服务注册
func (s *Server) register(ctx context.Context, port string) error {
	cli := s.EtcdClient
	serviceName := "service/" + s.Name
	// endpoint 以服务为维度。一个服务一个 Manager
	em, err := endpoints.NewManager(cli, serviceName)
	if err != nil {
		return err
	}
	s.etcdManager = em
	ip := netx.GetOutboundIP()
	add := ip + ":" + port
	s.etcdKey = "service/" + s.Name + "/" + add

	// 开启续约,心跳机制
	leaseResp, err := cli.Grant(ctx, s.EtcdTTL)
	if err != nil {
		return err
	}

	ch, err := cli.KeepAlive(ctx, leaseResp.ID)
	if err != nil {
		return err
	}

	go func() {
		// 循环channel
		for chResp := range ch {
			// 正常就是打印一下 DEBUG 日志啥的
			// 这里每次续约都会打一次
			s.Log.Debug("续约心跳", accesslog.String("message", chResp.String()))
		}
	}()

	// 注册入注册中心
	return em.AddEndpoint(ctx, s.etcdKey, endpoints.Endpoint{
		Addr: add,
	}, etcdv3.WithLease(leaseResp.ID))
}

// Close 优雅退出
func (s *Server) Close() error {
	if s.cancel != nil {
		// 停跳心跳机制
		s.cancel()
	}

	if s.etcdManager != nil {
		// 通知注册中心下线,取消注册
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := s.etcdManager.DeleteEndpoint(ctx, s.etcdKey)
		if err != nil {
			return err
		}
	}

	if s.EtcdClient != nil {
		// 关闭sdk
		err := s.EtcdClient.Close()
		if err != nil {
			return err
		}
	}
	s.Server.GracefulStop()
	return nil
}
