package transport

import (
	"fmt"
	types "github.com/ColdToo/Cold2DB/transport/types"
	"go.etcd.io/etcd/version"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/coreos/go-semver/semver"
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

func compareMajorMinorVersion(a, b *semver.Version) int {
	na := &semver.Version{Major: a.Major, Minor: a.Minor}
	nb := &semver.Version{Major: b.Major, Minor: b.Minor}
	switch {
	case na.LessThan(*nb):
		return -1
	case nb.LessThan(*na):
		return 1
	default:
		return 0
	}
}

// serverVersion returns the server version from the given header.
func serverVersion(h http.Header) *semver.Version {
	verStr := h.Get("X-Server-Version")
	// backward compatibility with etcd 2.0
	if verStr == "" {
		verStr = "2.0.0"
	}
	return semver.Must(semver.NewVersion(verStr))
}

// serverVersion returns the min cluster version from the given header.
func minClusterVersion(h http.Header) *semver.Version {
	verStr := h.Get("X-Min-Cluster-Version")
	// backward compatibility with etcd 2.0
	if verStr == "" {
		verStr = "2.0.0"
	}
	return semver.Must(semver.NewVersion(verStr))
}

// checkVersionCompatibility checks whether the given version is compatible
// with the local version.
func checkVersionCompatibility(name string, server, minCluster *semver.Version) (
	localServer *semver.Version,
	localMinCluster *semver.Version,
	err error) {
	localServer = semver.Must(semver.NewVersion(version.Version))
	localMinCluster = semver.Must(semver.NewVersion(version.MinClusterVersion))
	if compareMajorMinorVersion(server, localMinCluster) == -1 {
		return localServer, localMinCluster, fmt.Errorf("remote version is too low: remote[%s]=%s, local=%s", name, server, localServer)
	}
	if compareMajorMinorVersion(minCluster, localServer) == 1 {
		return localServer, localMinCluster, fmt.Errorf("local version is too low: remote[%s]=%s, local=%s", name, server, localServer)
	}
	return localServer, localMinCluster, nil
}

// setPeerURLsHeader reports local urls for peer discovery
func setPeerURLsHeader(req *http.Request, urls types.URLs) {
	peerURLs := make([]string, urls.Len())
	for i := range urls {
		peerURLs[i] = urls[i].String()
	}
	req.Header.Set("X-PeerURLs", strings.Join(peerURLs, ","))
}

// addRemoteFromRequest adds a remote peer according to an http request header
func addRemoteFromRequest(tr Transporter, r *http.Request) {
	if from, err := types.IDFromString(r.Header.Get("X-Server-From")); err == nil {
		if urls := r.Header.Get("X-PeerURLs"); urls != "" {
			tr.AddRemote(from, strings.Split(urls, ","))
		}
	}
}
