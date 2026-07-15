package rpc

import (
	"context"
	"io"
	"reflect"
	"unsafe"

	"github.com/lxt1045/rpc/codec"
)

type status uint8

const (
	statusInit    status = 0
	statusRunning status = 0
)

type Service struct {
	*codec.Codec

	svcMethods    []SvcMethod
	svcInterfaces map[string]uint32
	_type         reflect.Type
	middleware    []MiddlewareRespFunc
}

func (s Service) Methods() []codec.Method {
	callers := make([]codec.Method, len(s.svcMethods))
	for i, m := range s.svcMethods {
		if len(s.middleware) > 0 {
			callers[i] = WrapSvcMethod{
				SvcMethod:  m,
				middleware: s.middleware,
			}
		} else {
			callers[i] = m
		}
	}
	return callers
}
func (s Service) CloneMethods(svc interface{}) []codec.Method {
	svcPointer := reflect.ValueOf(svc).UnsafePointer()
	callers := make([]codec.Method, len(s.svcMethods))
	for i, m := range s.svcMethods {
		m.SvcPointer = svcPointer
		if len(s.middleware) > 0 {
			callers[i] = WrapSvcMethod{
				SvcMethod:  m,
				middleware: s.middleware,
			}
		} else {
			callers[i] = m
		}
	}
	return callers
}

// StartService fRegister: pb.RegisterHelloService, svc: implementation
func StartService(ctx context.Context, rwc io.ReadWriteCloser, svc interface{}, fRegisters ...interface{}) (s Service, err error) {
	rpc, err := StartPeer(ctx, rwc, svc, fRegisters...)
	if err != nil {
		return
	}
	s = rpc.Service
	return
}

func (c Service) Close(ctx context.Context) (err error) {
	return c.Codec.Close()
}

// func (s Service) Clone(ctx context.Context, cancel context.CancelFunc, rwc io.ReadWriteCloser, svc interface{}) (sNew Service, err error) {
// 	s.Codec, err = codec.NewCodec(ctx, cancel, rwc, s.CloneMethods(svc), nil, false)
// 	if err != nil {
// 		return
// 	}
// 	return s, nil
// }

func (s Service) MethodIdx(method string) (idx uint32, exist bool) {
	idx, exist = s.svcInterfaces[method]
	return
}

func (s Service) Call(ctx context.Context, methodIdx uint32, req unsafe.Pointer) (resp unsafe.Pointer, err error) {
	// if int(methodIdx) >= len(methods) {
	// 	err = errors.Errorf("methodIdx is out of range")
	// 	return
	// }

	m := s.svcMethods[methodIdx]
	resp, err = m.Func(m.SvcPointer, ctx, req)
	return
}

func (s Service) AllInterfaces() (is []SvcMethod) {
	return s.svcMethods
}

func (rpc *Service) Use(middleware ...MiddlewareRespFunc) {
	rpc.middleware = append(rpc.middleware, middleware...)
	return
}
