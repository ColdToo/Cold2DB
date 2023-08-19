package transport

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
	tr, err := NewTransport(info, dialtimeoutd)
	if err != nil {
		return nil, err
	}

	if rdtimeoutd != 0 || wtimeoutd != 0 {
		// the timed out connection will timeout soon after it is idle.
		// it should not be put back to http transport as an idle connection for future usage.
		tr.MaxIdleConnsPerHost = -1
	} else {
		// allow more idle connections between peers to avoid unnecessary port allocation.
		tr.MaxIdleConnsPerHost = 1024
	}

	tr.DialContext = (&rwTimeoutDialer{
		Dialer: net.Dialer{
			Timeout:   dialtimeoutd,
			KeepAlive: 30 * time.Second,
		},
		rdtimeoutd: rdtimeoutd,
		wtimeoutd:  wtimeoutd,
	}).DialContext
	return tr, nil
}

func NewTransport(info TLSInfo, dialtimeoutd time.Duration) (*http.Transport, error) {
	cfg, err := info.ClientConfig()
	if err != nil {
		return nil, err
	}

	t := &http.Transport{
		//这行代码是在创建一个HTTP传输对象时设置代理。http.ProxyFromEnvironment是一个函数，它返回当前环境变量中设置的代理地址。
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   dialtimeoutd,     //拨号的超时时间超时
			KeepAlive: 30 * time.Second, //如果连接在30s内都没有活动那么关闭连接
		}).DialContext,
		TLSHandshakeTimeout: 10 * time.Second,
		TLSClientConfig:     cfg,
	}

	return t, nil
}
