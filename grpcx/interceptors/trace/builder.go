package trace

import (
	"context"
	"github.com/dadaxiaoxiao/go-pkg/grpcx/interceptors"
	"github.com/go-kratos/kratos/v2/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type InterceptorBuilder struct {
	interceptors.Builder
	tracer trace.Tracer
	// 跨进程传播上下文
	propagator propagation.TextMapPropagator
}

func NewInterceptorBuilder(tracer trace.Tracer, propagator propagation.TextMapPropagator) *InterceptorBuilder {
	return &InterceptorBuilder{
		tracer:     tracer,
		propagator: propagator,
	}
}

func (i *InterceptorBuilder) BuildClient() grpc.UnaryClientInterceptor {
	propagator := i.propagator
	if propagator == nil {
		// 获取全局 propagator
		propagator = otel.GetTextMapPropagator()
	}
	tracer := i.tracer
	if tracer == nil {
		tracer = otel.Tracer("github.com/dadaxiaoxiao/go-pkg/grpcx/interceptors/trace")
	}
	// 自定义属性字段
	attrs := []attribute.KeyValue{
		semconv.RPCSystemKey.String("grpc"),
		attribute.Key("rpc.grpc.kind").String("unary"),
		attribute.Key("rpc.component").String("client"),
	}
	return func(ctx context.Context, method string, req,
		reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) (err error) {
		ctx, span := tracer.Start(ctx, method,
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(attrs...))
		defer span.End()
		span.SetAttributes(
			semconv.RPCMethodKey.String(method),
			semconv.NetPeerNameKey.String(i.PeerName(ctx)),
			attribute.Key("net.peer.ip").String(i.PeerIp(ctx)),
		)
		defer func() {
			if err != nil {
				span.RecordError(err)
				if e := errors.FromError(err); e != nil {
					span.SetAttributes(semconv.RPCGRPCStatusCodeKey.Int64(int64(e.Code)))
				}
				span.SetStatus(codes.Error, err.Error())
			} else {
				span.SetStatus(codes.Ok, "OK")
			}
		}()
		ctx = inject(ctx, propagator)
		err = invoker(ctx, method, req, reply, cc, opts...)
		return err
	}
}

func (i *InterceptorBuilder) BuildServer() grpc.UnaryServerInterceptor {
	propagator := i.propagator
	if propagator == nil {
		// 获取全局 propagator
		propagator = otel.GetTextMapPropagator()
	}
	tracer := i.tracer
	if tracer == nil {
		tracer = otel.Tracer("github.com/dadaxiaoxiao/go-pkg/grpcx/interceptors/trace")
	}
	// 自定义属性字段
	attrs := []attribute.KeyValue{
		semconv.RPCSystemKey.String("grpc"),
		attribute.Key("rpc.grpc.kind").String("unary"),
		attribute.Key("rpc.component").String("server"),
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		// 接收端从请求中解析出 SpanContext
		ctx = extract(ctx, propagator)
		ctx, span := tracer.Start(ctx, info.FullMethod,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(attrs...))
		defer span.End()
		span.SetAttributes(
			semconv.RPCMethodKey.String(info.FullMethod),
			semconv.NetPeerNameKey.String(i.PeerName(ctx)),
			attribute.Key("net.peer.ip").String(i.PeerIp(ctx)),
		)
		defer func() {
			// handler 执行结束时
			if err != nil {
				// 追踪错误
				span.RecordError(err)
			} else {
				span.SetStatus(codes.Ok, "OK")
			}
		}()
		resp, err = handler(ctx, req)
		return
	}
}

// inject 发送
// 发送端将 SpanContext 注入到请求中
func inject(ctx context.Context, p propagation.TextMapPropagator) context.Context {
	// carrier
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(map[string]string{})
	}
	p.Inject(ctx, GrpcHeaderCarrier(md))
	return metadata.NewOutgoingContext(ctx, md)
}

// extract 接收
// 接收端从请求中解析出 SpanContext
func extract(ctx context.Context, p propagation.TextMapPropagator) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		md = metadata.New(map[string]string{})
	}
	return p.Extract(ctx, GrpcHeaderCarrier(md))
}

// GrpcHeaderCarrier grpc head 搬运工
// 这里使用适配器
type GrpcHeaderCarrier metadata.MD

func (mc GrpcHeaderCarrier) Get(key string) string {
	vals := metadata.MD(mc).Get(key)
	if len(vals) > 0 {
		return vals[0]
	}
	return ""
}

func (mc GrpcHeaderCarrier) Set(key string, value string) {
	metadata.MD(mc).Set(key, value)
}

func (mc GrpcHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(mc))
	for k := range metadata.MD(mc) {
		keys = append(keys, k)
	}
	return keys
}
