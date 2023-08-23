package transportHttp

import (
	"fmt"
	types "github.com/ColdToo/Cold2DB/transportHttp/types"
	"io"
	"net/http"
	"net/url"
	"strings"
)

var (
	errMemberRemoved  = fmt.Errorf("the member has been permanently removed from the cluster")
	errMemberNotFound = fmt.Errorf("member not found")
)

func createPostRequest(u url.URL, path string, body io.Reader, contentType string, urls types.URLs, from, clusterId types.ID) *http.Request {
	uu := u
	uu.Path = path
	req, err := http.NewRequest("POST", uu.String(), body)
	if err != nil {

	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("X-Server-From", from.Str())
	req.Header.Set("X-Etcd-Cluster-ID", clusterId.Str())

	setPeerURLsHeader(req, urls)

	return req
}

func checkPostResponse(resp *http.Response, body []byte, req *http.Request, to types.ID) error {
	switch resp.StatusCode {
	case http.StatusPreconditionFailed:
		switch strings.TrimSuffix(string(body), "\n") {
		case errClusterIDMismatch.Error():
			//plog.Errorf("request sent was ignored (cluster ID mismatch: remote[%s]=%s, local=%s)",
			//to, resp.Header.Get("X-Etcd-Cluster-ID"), req.Header.Get("X-Etcd-Cluster-ID"))
			return errClusterIDMismatch
		default:
			return fmt.Errorf("unhandled error %q when precondition failed", string(body))
		}
	case http.StatusForbidden:
		return errMemberRemoved
	case http.StatusNoContent:
		return nil
	default:
		return fmt.Errorf("unexpected http status %s while posting to %q", http.StatusText(resp.StatusCode), req.URL.String())
	}
}

func reportCriticalError(err error, errc chan<- error) {
	select {
	case errc <- err:
	default:
	}
}

// setPeerURLsHeader reports local urls for peer discovery
func setPeerURLsHeader(req *http.Request, urls types.URLs) {
	peerURLs := make([]string, urls.Len())
	for i := range urls {
		peerURLs[i] = urls[i].String()
	}
	req.Header.Set("X-PeerURLs", strings.Join(peerURLs, ","))
}

// IsClosedConnError returns true if the error is from closing listener, cmux.
// copied from golang.org/x/net/http2/http2.go
func IsClosedConnError(err error) bool {
	// 'use of closed network connection' (Go <=1.8)
	// 'use of closed file or network connection' (Go >1.8, internal/poll.ErrClosing)
	// 'mux: listener closed' (cmux.ErrListenerClosed)
	return err != nil && strings.Contains(err.Error(), "closed")
}
