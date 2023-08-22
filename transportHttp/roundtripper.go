package transportHttp

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"time"
)

func NewPipeLineRoundTripper(tlsInfo TLSInfo, dialTimeout time.Duration) (http.RoundTripper, error) {
	return NewTimeoutTransport(tlsInfo, dialTimeout, 0, 0)
}

func newStreamRoundTripper(tlsInfo TLSInfo, dialTimeout time.Duration) (http.RoundTripper, error) {
	return NewTimeoutTransport(tlsInfo, dialTimeout, ConnReadTimeout, ConnWriteTimeout)
}

func NewTimeoutTransport(info TLSInfo, dialtimeoutd, rdtimeoutd, wtimeoutd time.Duration) (*http.Transport, error) {
	cfg, err := info.ClientConfig()
	if err != nil {
		return nil, err
	}
	t := &http.Transport{
		//这行代码是在创建一个HTTP传输对象时设置代理。http.ProxyFromEnvironment是一个函数，它返回当前环境变量中设置的代理地址。
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&rwTimeoutDialer{
			Dialer: net.Dialer{
				Timeout:   dialtimeoutd,
				KeepAlive: 30 * time.Second,
			},
			rdtimeoutd: rdtimeoutd,
			wtimeoutd:  wtimeoutd,
		}).DialContext,
		TLSHandshakeTimeout: 10 * time.Second,
		TLSClientConfig:     cfg,
	}

	//通过控制读写超时时间，来控制连接是否可以一直保持打开状态
	if rdtimeoutd != 0 || wtimeoutd != 0 {
		t.MaxIdleConnsPerHost = -1
	} else {
		t.MaxIdleConnsPerHost = 1024
	}

	return t, nil
}

type TLSInfo struct {
	// CertFile is the _server_ cert, it will also be used as a _client_ certificate if ClientCertFile is empty
	CertFile string
	// KeyFile is the key for the CertFile
	KeyFile string
	// ClientCertFile is a _client_ cert for initiating connections when ClientCertAuth is defined. If ClientCertAuth
	// is true but this value is empty, the CertFile will be used instead.
	ClientCertFile string
	// ClientKeyFile is the key for the ClientCertFile
	ClientKeyFile string

	TrustedCAFile       string
	ClientCertAuth      bool
	CRLFile             string
	InsecureSkipVerify  bool
	SkipClientSANVerify bool

	// ServerName ensures the cert matches the given host in case of discovery / virtual hosting
	ServerName string

	// HandshakeFailure is optionally called when a connection fails to handshake. The
	// connection will be closed immediately afterwards.
	HandshakeFailure func(*tls.Conn, error)

	// CipherSuites is a list of supported cipher suites.
	// If empty, Go auto-populates it by default.
	// Note that cipher suites are prioritized in the given order.
	CipherSuites []uint16

	// MinVersion is the minimum TLS version that is acceptable.
	// If not set, the minimum version is TLS 1.2.
	MinVersion uint16

	// MaxVersion is the maximum TLS version that is acceptable.
	// If not set, the default used by Go is selected (see tls.Config.MaxVersion).
	MaxVersion uint16

	selfCert bool

	// parseFunc exists to simplify testing. Typically, parseFunc
	// should be left nil. In that case, tls.X509KeyPair will be used.
	parseFunc func([]byte, []byte) (tls.Certificate, error)

	// AllowedCN is a CN which must be provided by a client.
	AllowedCN string

	// AllowedHostname is an IP address or hostname that must match the TLS
	// certificate provided by a client.
	AllowedHostname string

	// EmptyCN indicates that the cert must have empty CN.
	// If true, ClientConfig() will return an error for a cert with non empty CN.
	EmptyCN bool
}

func (info TLSInfo) String() string {
	return fmt.Sprintf("cert = %s, key = %s, client-cert=%s, client-key=%s, trusted-ca = %s, client-cert-auth = %v, crl-file = %s", info.CertFile, info.KeyFile, info.ClientCertFile, info.ClientKeyFile, info.TrustedCAFile, info.ClientCertAuth, info.CRLFile)
}

func (info TLSInfo) Empty() bool {
	return info.CertFile == "" && info.KeyFile == ""
}

// ClientConfig generates a tls.Config object for use by an HTTP client.
func (info TLSInfo) ClientConfig() (*tls.Config, error) {
	var cfg *tls.Config
	return cfg, nil
}
