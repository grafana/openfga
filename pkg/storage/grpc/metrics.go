package grpc

import (
	"context"
	"io"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"github.com/openfga/openfga/internal/build"
)

var (
	grpcClientHandledTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "grpc_client_handled_total",
		Help:      "Total number of RPCs completed on the client, regardless of success or failure.",
	}, []string{"grpc_service", "grpc_method", "grpc_code"})

	grpcClientHandlingSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: build.ProjectName,
		Name:      "grpc_client_handling_seconds",
		Help:      "Histogram of response latency (seconds) of the gRPC until it is finished by the application.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"grpc_service", "grpc_method"})
)

func UnaryClientInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	start := time.Now()
	err := invoker(ctx, method, req, reply, cc, opts...)
	st, _ := status.FromError(err)
	service, methodName := splitMethodName(method)
	grpcClientHandledTotal.WithLabelValues(service, methodName, st.Code().String()).Inc()
	grpcClientHandlingSeconds.WithLabelValues(service, methodName).Observe(time.Since(start).Seconds())
	return err
}

func StreamClientInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	start := time.Now()
	clientStream, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		st, _ := status.FromError(err)
		service, methodName := splitMethodName(method)
		grpcClientHandledTotal.WithLabelValues(service, methodName, st.Code().String()).Inc()
		grpcClientHandlingSeconds.WithLabelValues(service, methodName).Observe(time.Since(start).Seconds())
		return nil, err
	}
	return &monitoredClientStream{ClientStream: clientStream, method: method, start: start}, nil
}

type monitoredClientStream struct {
	grpc.ClientStream
	method string
	start  time.Time
}

func (s *monitoredClientStream) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)
	if err == io.EOF {
		s.report(nil)
		return err
	}
	if err != nil {
		s.report(err)
		return err
	}
	return nil
}

func (s *monitoredClientStream) SendMsg(m any) error {
	err := s.ClientStream.SendMsg(m)
	if err != nil {
		s.report(err)
		return err
	}
	return nil
}

func (s *monitoredClientStream) report(err error) {
	st, _ := status.FromError(err)
	service, methodName := splitMethodName(s.method)
	grpcClientHandledTotal.WithLabelValues(service, methodName, st.Code().String()).Inc()
	grpcClientHandlingSeconds.WithLabelValues(service, methodName).Observe(time.Since(s.start).Seconds())
}

// `splitMethodName` splits a full method name into service and method.
// It handles the case where the method name is prefixed with a slash.
func splitMethodName(fullMethodName string) (string, string) {
	if len(fullMethodName) == 0 {
		return "unknown", "unknown"
	}
	if fullMethodName[0] == '/' {
		fullMethodName = fullMethodName[1:]
	}

	for i := len(fullMethodName) - 1; i >= 0; i-- {
		if fullMethodName[i] == '/' {
			return fullMethodName[:i], fullMethodName[i+1:]
		}
	}
	return "unknown", fullMethodName
}
