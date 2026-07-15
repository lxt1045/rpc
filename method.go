package rpc

import (
	"context"
	"reflect"
	"strings"
	"unsafe"

	"github.com/lxt1045/errors"
	"github.com/lxt1045/rpc/codec"
	"google.golang.org/grpc"
)

var _ codec.Method = SvcMethod{} // 检查是由实现接口

type SvcMethod struct {
	CliMethod
	Func       func(unsafe.Pointer, context.Context, unsafe.Pointer) (unsafe.Pointer, error)
	SvcPointer unsafe.Pointer
	// bStream    bool // 是否双向流
}

func (m SvcMethod) SvcInvoke(ctx context.Context, req codec.Msg) (resp codec.Msg, err error) {
	p := (*[2]unsafe.Pointer)(unsafe.Pointer(&req))[1]
	pr, err := m.Func(m.SvcPointer, ctx, p)
	if err != nil || pr == nil {
		return
	}
	resp = reflect.NewAt(m.respType, pr).Interface().(codec.Msg)
	return
}

type CliMethod struct {
	reqType  reflect.Type
	respType reflect.Type
	Name     string
	CallID   uint16
}

func (m CliMethod) ReqType() reflect.Type {
	return m.reqType
}
func (m CliMethod) RespType() reflect.Type {
	return m.respType
}

func (m CliMethod) NewReq() codec.Msg {
	msg := reflect.New(m.reqType).Interface().(codec.Msg)
	return msg
}
func (m CliMethod) NewResp() codec.Msg {
	if m.respType == nil {
		return nil
	}
	msg := reflect.New(m.respType).Interface().(codec.Msg)
	return msg
}
func (m CliMethod) FuncName() string {
	return m.Name
}

func (m CliMethod) SvcInvoke(ctx context.Context, req codec.Msg) (resp codec.Msg, err error) {
	return
}

// RegisterServiceServer(s *grpc.Server, srv ServiceServer)
func getSvcMethods(fRegister interface{}, svc interface{}) (methods []SvcMethod, err error) {
	regMethodType := reflect.TypeOf(fRegister)
	if regMethodType.Kind() != reflect.Func {
		err = errors.Errorf("arg fun must be func")
		return
	}
	if regMethodType.NumIn() != 2 || regMethodType.NumOut() != 0 {
		err = errors.Errorf("arg fun args error")
		return
	}
	arg0 := regMethodType.In(0)
	argGrpcService := reflect.TypeOf(&grpc.Server{})
	if arg0.String() != argGrpcService.String() {
		err = errors.Errorf("arg fun args error, arg0:%s, argGrpcService:%s", arg0, argGrpcService)
		return
	}

	ifaceType := regMethodType.In(1)
	if ifaceType.Kind() != reflect.Interface {
		err = errors.Errorf("arg fun args error")
		return
	}
	if ifaceType.NumMethod() == 0 {
		err = errors.Errorf("service %s has no funcs", ifaceType.String())
		return
	}

	// 判断是否实现了接口
	if svc != nil {
		if svcImpType := reflect.TypeOf(svc); !svcImpType.Implements(ifaceType) {
			err = errors.Errorf("the handler of type %v that does not satisfy %v", svcImpType, ifaceType)
			return
		}
	}

	for i := 0; i < ifaceType.NumMethod(); i++ {
		method := ifaceType.Method(i)
		mType := method.Type

		// stream 模式，抛弃
		if mType.NumOut() == 1 || mType.NumIn() < 2 {
			continue
		}

		reqType := mType.In(1).Elem()   // 形参是指针，所以要换成原始数据结构形式
		respType := mType.Out(0).Elem() // 同上

		// 判断该 Call 是否需要返回，如果不需要返回则会直接不管返回值，提高返回性能
		if respType.NumField() <= 3 && strings.HasSuffix(respType.String(), ".Empty") {
			bFound := false
			for i := 0; i < respType.NumField(); i++ {
				field := respType.Field(i)
				if reflect.StructTag(field.Tag).Get("protobuf") != "" {
					bFound = true
				}
			}
			if !bFound {
				respType = nil // 返回值成员为空的时候
			}
		}

		m := SvcMethod{
			CliMethod: CliMethod{
				reqType:  reqType,
				respType: respType,
				Name:     ifaceType.String() + "." + method.Name, // ifaceType.PkgPath() + "." +
			},
		}

		if svc != nil {
			methodTarget := reflect.ValueOf(
				func(unsafe.Pointer, context.Context, unsafe.Pointer) (unsafe.Pointer, error) {
					return nil, nil
				},
			).Interface()

			svcImpType := reflect.TypeOf(svc)
			svcImpMethod, ok := svcImpType.MethodByName(method.Name)
			if !ok {
				err = errors.Errorf("svcImpType.MethodByName not ok")
				return
			}
			methodSource := svcImpMethod.Func.Interface()
			ifMethod := (*[2]uintptr)(unsafe.Pointer(&methodSource))
			ifNew := (*[2]uintptr)(unsafe.Pointer(&methodTarget))
			ifNew[1] = ifMethod[1] // 改变 type 保留原始指针 ptr
			f, ok := methodTarget.(func(unsafe.Pointer, context.Context, unsafe.Pointer) (unsafe.Pointer, error))
			if !ok {
				err = errors.Errorf("svcImpType.MethodByName not ok")
				return
			}
			m.Func = f
			m.SvcPointer = reflect.ValueOf(svc).UnsafePointer()
		}

		methods = append(methods, m)
	}

	return
}

// NewClientClient(cc *grpc.ClientConn) ClientClient
func getCliMethods(fRegister interface{}) (methods []CliMethod, err error) {
	regMethodType := reflect.TypeOf(fRegister)
	if regMethodType.Kind() != reflect.Func {
		err = errors.Errorf("arg fun must be func")
		return
	}
	if regMethodType.NumIn() != 1 || regMethodType.NumOut() != 1 {
		err = errors.Errorf("arg fun args error")
		return
	}
	arg0 := regMethodType.In(0)
	argGrpcClientConn := reflect.TypeOf(&grpc.ClientConn{})
	if arg0.String() != argGrpcClientConn.String() {
		err = errors.Errorf("arg fun args error, arg0:%s, argGrpcService:%s", arg0, argGrpcClientConn)
		return
	}

	ifaceType := regMethodType.Out(0)
	if ifaceType.Kind() != reflect.Interface {
		err = errors.Errorf("arg fun args error")
		return
	}
	if ifaceType.NumMethod() == 0 {
		err = errors.Errorf("svc %s has no funcs", ifaceType.String())
		return
	}

	ifaceName, needReplace, newSuffix := ifaceType.String(), "Client", "Server"
	if strings.HasSuffix(ifaceName, needReplace) {
		ifaceName = ifaceName[:len(ifaceName)-len(needReplace)] + newSuffix
	}
	for i := 0; i < ifaceType.NumMethod(); i++ {
		method := ifaceType.Method(i)
		mType := method.Type

		// stream 模式，抛弃
		if mType.NumOut() == 1 || mType.NumIn() < 2 {
			continue
		}

		reqType := mType.In(1).Elem()   // 形参是指针，所以要换成原始数据结构形式
		respType := mType.Out(0).Elem() // 同上

		// 判断该 Call 是否需要返回，如果不需要返回则会直接不管返回值，提高返回性能
		if respType.NumField() <= 3 && strings.HasSuffix(respType.String(), ".Empty") {
			bFound := false
			for i := 0; i < respType.NumField(); i++ {
				field := respType.Field(i)
				if reflect.StructTag(field.Tag).Get("protobuf") != "" {
					bFound = true
				}
			}
			if !bFound {
				respType = nil // 返回值成员为空的时候
			}
		}

		m := CliMethod{
			reqType:  reqType,
			respType: respType,
			Name:     ifaceName + "." + method.Name, // ifaceType.PkgPath() + "." +
		}

		methods = append(methods, m)
	}

	return
}
