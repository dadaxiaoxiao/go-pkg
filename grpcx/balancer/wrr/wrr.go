package wrr

import (
	"context"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"sync"
	"sync/atomic"
)

const name = "custom_weight_round_robin"

func init() {
	balancer.Register(newWrrPickerBuilder())
}

// newBuilder
func newWrrPickerBuilder() balancer.Builder {
	return base.NewBalancerBuilder(name, &WrrPickerBuilder{}, base.Config{HealthCheck: true})
}

// WrrPickerBuilder 平滑加权轮询picker builder
type WrrPickerBuilder struct {
}

func (w WrrPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	conns := make([]*weightConn, 0, len(info.ReadySCs))
	for sc, sci := range info.ReadySCs {
		cc := &weightConn{
			cc: sc,
		}
		// 读取 Metadata 信息
		v, ok := sci.Address.Metadata.(map[string]any)
		if ok {
			// 如果没有 weight 这个key ，weightVal 为nil
			weightVal := v["weight"]
			// 经过注册中心的转发后，变成了float64
			// weightVal 为nil，断言后返回 0
			weight, _ := weightVal.(float64)
			cc.weight = int(weight)
		}
		if cc.weight == 0 {
			// 这里避免权重0值的问题
			cc.weight = 10
		}
		cc.currentWeight = cc.weight
		conns = append(conns, cc)
	}
	return &WrrPicker{conns: conns}
}

// WrrPicker 平滑加权轮询picker
type WrrPicker struct {
	conns []*weightConn
	mutex sync.Mutex
}

func (w *WrrPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	// 这里防止并发操作
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if len(w.conns) == 0 {
		// 兜底降级
		// 没有候选节点
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	var total int
	var maxConn *weightConn

	for _, cc := range w.conns {
		if !cc.available.Load() {
			// 当前节点不可用
			continue
		}

		// 计算总权重
		total += cc.weight
		// 计算当前权重
		cc.currentWeight = cc.weight + cc.currentWeight

		if maxConn == nil || maxConn.currentWeight < cc.currentWeight {
			// 选中服务节点
			maxConn = cc
		}
	}

	// 更新选中节点的当前权重
	maxConn.currentWeight = maxConn.currentWeight - total
	return balancer.PickResult{
		SubConn: maxConn.cc,
		Done: func(info balancer.DoneInfo) {
			// 这里执行failover 相关 ，动态调整权重
			err := info.Err
			if err == nil {
				// 这里可以考虑增加权重 weight/currentWeight ，但是要注意最大值
				return
			}
			switch err {
			case context.Canceled: // 主动取消
				return
			case context.DeadlineExceeded: // 超时
			case io.EOF, io.ErrUnexpectedEOF:
				// 认为该节点已经崩了
				// 这里可以考虑降低权重 weight/currentWeight ，但是要注意最小值 和 0
				maxConn.available.Store(true)
			default:
				st, ok := status.FromError(err)
				if ok {
					code := st.Code()
					switch code {
					case codes.Unavailable:
						// 不可用，可能表达的是熔断
						// 移走该节点，该节点已经不可用
						maxConn.available.Store(false)
						// 开启额外的goroutine 去探活
						go func() {
							// for 循环 + healthCheck
							// 或者不使用for，只使用 healthCheck 这里睡眠一分钟，假设节点便回复
							for {
								if w.healthCheck(maxConn) {
									// 节点挪回可用节点
									maxConn.available.Store(true)
									return
								}
							}
						}()
					case codes.ResourceExhausted:
						// 资源耗尽，可能表达的是限流
						// 这可以挪走，可以留着，但是要降低 currentWeight 和 weight，达到减少它被选中的概率
					}
				}
			}
		},
	}, nil
}

func (p *WrrPicker) healthCheck(cc *weightConn) bool {
	// 调用 grpc 内置的那个 health check 接口
	// 待实现
	return true
}

// weightConn 权重节点连接
type weightConn struct {
	// 权重
	weight int
	// 当前权重
	currentWeight int
	// gRPC 节点连接
	cc balancer.SubConn

	available *atomic.Bool
}
