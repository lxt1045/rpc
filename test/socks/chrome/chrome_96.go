package chrome

import (
	"github.com/bogdanfinn/fhttp/http2"
	"github.com/bogdanfinn/tls-client/profiles"

	tls "github.com/bogdanfinn/utls"
)

var (
	Chrome96 = createChrome96()
	Chrome96UA = "Mozilla/5.0 (Linux; Android 10; MI 6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.104 Mobile Safari/537.36"
)

func createChrome96() *profiles.ClientProfile {
	var (
		clientHelloId     tls.ClientHelloID
		settings          map[http2.SettingID]uint32
		settingsOrder     []http2.SettingID
		pseudoHeaderOrder []string
		connectionFlow    uint32
		priorities        []http2.Priority
		headerPriority    *http2.PriorityParam
	)
	clientHelloId = tls.ClientHelloID{
		Client:               "Chrome", 
		RandomExtensionOrder: false,
		Version:              "96",
		Seed:                 nil,
		SpecFactory: func() (tls.ClientHelloSpec, error) {
			return tls.ClientHelloSpec{
				CipherSuites: []uint16{
					tls.GREASE_PLACEHOLDER,
					tls.TLS_AES_128_GCM_SHA256,
					tls.TLS_AES_256_GCM_SHA384,
					tls.TLS_CHACHA20_POLY1305_SHA256,
					tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
					tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
					tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
					tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
					tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
					tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
					tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
					tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
					tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
					tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
					tls.TLS_RSA_WITH_AES_128_CBC_SHA,
					tls.TLS_RSA_WITH_AES_256_CBC_SHA,

				},
				CompressionMethods: []byte{
					tls.CompressionNone,
				},
				Extensions: // tls.ShuffleChromeTLSExtensions( // 随机化处理
					[]tls.TLSExtension{
						&tls.UtlsGREASEExtension{},
					&tls.SNIExtension{},
					&tls.ExtendedMasterSecretExtension{},
					&tls.RenegotiationInfoExtension{ // (65281)
							Renegotiation: tls.RenegotiateOnceAsClient,
						},
					&tls.SupportedCurvesExtension{Curves: []tls.CurveID{
							tls.GREASE_PLACEHOLDER,
						tls.X25519,
						tls.CurveP256,
						tls.CurveP384,
						}},
					&tls.SupportedPointsExtension{SupportedPoints: []byte{
							tls.PointFormatUncompressed,
						}},
					&tls.SessionTicketExtension{},
					&tls.ALPNExtension{AlpnProtocols: []string{
							"h2",
							"http/1.1",
						}},
					&tls.StatusRequestExtension{},
					&tls.SignatureAlgorithmsExtension{SupportedSignatureAlgorithms: []tls.SignatureScheme{
							tls.ECDSAWithP256AndSHA256,
					tls.PSSWithSHA256,
					tls.PKCS1WithSHA256,
					tls.ECDSAWithP384AndSHA384,
					tls.PSSWithSHA384,
					tls.PKCS1WithSHA384,
					tls.PSSWithSHA512,
					tls.PKCS1WithSHA512,
						}},
					&tls.SCTExtension{},
					&tls.KeyShareExtension{KeyShares: []tls.KeyShare{
							{Group: tls.CurveID(tls.GREASE_PLACEHOLDER), Data: []byte{0}},
							{Group: tls.X25519},
						}},
					&tls.PSKKeyExchangeModesExtension{Modes: []uint8{
							tls.PskModeDHE,
						}},
					&tls.SupportedVersionsExtension{Versions: []uint16{
							tls.GREASE_PLACEHOLDER,
					tls.VersionTLS13,
					tls.VersionTLS12,
					tls.VersionTLS11,
					tls.VersionTLS10,
						}},
					&tls.UtlsCompressCertExtension{Algorithms: []tls.CertCompressionAlgo{
							tls.CertCompressionBrotli,
						}},
					&tls.ApplicationSettingsExtension{
							// CodePoint:          tls.ExtensionALPSOld,
							SupportedProtocols: []string{"h2"},
						},
					&tls.UtlsGREASEExtension{},
					&tls.UtlsPaddingExtension{
					GetPaddingLen: tls.BoringPaddingStyle,
				},
					(tls.PreSharedKeyExtension)(&tls.FakePreSharedKeyExtension{}), // 忽略 pre_shared_key扩展用于零往返时间(0-RTT)恢复之前的会话。
					},
				// ), // 随机化处理
			}, nil
		},
	}
	settings = map[http2.SettingID]uint32{
				http2.SettingHeaderTableSize:   65536,
		http2.SettingMaxConcurrentStreams:   1000,
		http2.SettingInitialWindowSize:   6291456,
		http2.SettingMaxHeaderListSize:   262144,
	}
	settingsOrder = []http2.SettingID{
				http2.SettingHeaderTableSize,
		http2.SettingMaxConcurrentStreams,
		http2.SettingInitialWindowSize,
		http2.SettingMaxHeaderListSize,
	}
	pseudoHeaderOrder = []string{
		":method",
		":authority",
		":scheme",
		":path",

	}
	connectionFlow = 15663105
	headerPriority = &http2.PriorityParam{
		StreamDep: 0,    // depends_on
		Exclusive: true, // exclusive
		Weight:    255,  // weight: 1 ~ 256
	}
	// priorities = []http2.Priority{
	// 	{StreamID: 3, PriorityParam: http2.PriorityParam{
	// 		StreamDep: 0,
	// 		Exclusive: false,
	// 		Weight:    200,
	// 	}},
	// }

	p := profiles.NewClientProfile(clientHelloId, settings, settingsOrder,
		pseudoHeaderOrder, connectionFlow, priorities, headerPriority)
	return &p
}