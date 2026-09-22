package socks_faux_kcp

import (
	"crypto/tls"
	"testing"

	"github.com/lxt1045/utils/cert"
	"github.com/lxt1045/utils/config"
)

const testTLSHost = "speedtest.cn"

// testCerts 现场生成的内存 CA + server/client 证书链（与 test/cert 同链，
// 纯内存、无文件依赖）。
type testCerts struct {
	caPEM                    []byte
	serverCertPEM, serverKey []byte
	clientCertPEM, clientKey []byte
}

func newTestCerts(t *testing.T) *testCerts {
	t.Helper()
	root, err := cert.NewSelfSigned()
	if err != nil {
		t.Fatal(err)
	}
	rootKey, rootCert, err := root.MakeEcdsa(cert.CertModeRoot, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := cert.New(rootKey, rootCert)
	if err != nil {
		t.Fatal(err)
	}
	serverKey, serverCert, err := ca.MakeEcdsa(cert.CertModeLeaf, nil, []string{testTLSHost})
	if err != nil {
		t.Fatal(err)
	}
	clientKey, clientCert, err := ca.MakeEcdsa(cert.CertModeLeaf, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return &testCerts{
		caPEM:         rootCert,
		serverCertPEM: serverCert, serverKey: serverKey,
		clientCertPEM: clientCert, clientKey: clientKey,
	}
}

// serverTLS 服务端配置（mTLS：要求并校验客户端证书，与生产 LoadTLSConfig 一致）。
func (c *testCerts) serverTLS(t *testing.T) *tls.Config {
	t.Helper()
	cfg, err := config.BytesToTLSConfig(c.serverCertPEM, c.serverKey, c.caPEM)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// clientTLS 客户端配置（携带客户端证书，校验服务端 ServerName）。
func (c *testCerts) clientTLS(t *testing.T) *tls.Config {
	t.Helper()
	cfg, err := config.BytesToTLSConfig(c.clientCertPEM, c.clientKey, c.caPEM)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ServerName = testTLSHost
	return cfg
}

// TestTLSCertsUsable 校验测试证书的 mTLS 握手能完成（防止后续用例失败时难以定位）。
func TestTLSCertsUsable(t *testing.T) {
	certs := newTestCerts(t)
	scfg, ccfg := certs.serverTLS(t), certs.clientTLS(t)
	if scfg == nil || ccfg == nil {
		t.Fatal("nil tls config")
	}
	if scfg.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatal("server should require client cert (mTLS)")
	}
}
