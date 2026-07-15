package http

import (
	"context"
	"crypto/tls"
	"io/fs"
	"log"
	"net/http"

	"github.com/gorilla/websocket"
	"github.com/lxt1045/rpc/test/socks/filesystem"
	"github.com/lxt1045/utils/config"
	llog "github.com/lxt1045/utils/log"
	"golang.org/x/net/http2"
)

func InitHTTP(ctx context.Context, addr string, conf config.TLS, f func(conn *websocket.Conn)) (srv *http.Server, err error) {
	router, err := NewWsRouter(ctx, f)
	if err != nil {
		return
	}

	//fmt.Sprintf(":%v", conf.GetEnv("HTTPS_PORT"))
	certPEM, err := fs.ReadFile(filesystem.Static, conf.ServerCert)
	if err != nil {
		return
	}
	keyPEM, err := fs.ReadFile(filesystem.Static, conf.ServerKey)
	if err != nil {
		return
	}

	srvCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		log.Println("try to load key & crt err", err)
		return
	}

	config := &tls.Config{
		Certificates: []tls.Certificate{srvCert}, // 服务器证书
		ClientAuth:   tls.NoClientCert,           // 不要求客户端证书
		MinVersion:   tls.VersionTLS12,
		MaxVersion:   tls.VersionTLS13,

		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
			tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
			tls.TLS_RSA_WITH_AES_128_CBC_SHA,
			tls.TLS_RSA_WITH_AES_256_CBC_SHA,
		},
	}
	srv = &http.Server{
		Addr:      addr,
		Handler:   router,
		TLSConfig: config,
		ErrorLog:  log.New(llog.GetStdOutput(ctx), "", log.Lshortfile|log.Ldate|log.Ltime),
		// ConnState:   func(c net.Conn, cs http.ConnState) {},
		// ConnContext: func(ctx context.Context, c net.Conn) context.Context { return ctx },
	}

	http2.ConfigureServer(srv, &http2.Server{})

	go func() {
		err = srv.ListenAndServeTLS("", "")
		if err != nil {
			log.Println("ListenAndServeTLS err", err)
			return
		}
	}()

	return
}
