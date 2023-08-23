package transportHttp

import (
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
