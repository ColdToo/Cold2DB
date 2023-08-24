package transport

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strings"
)

type URLs []url.URL

func NewURLs(strs []string) (URLs, error) {
	all := make([]url.URL, len(strs))
	if len(all) == 0 {
		return nil, errors.New("no valid URLs given")
	}

	for i, v := range strs {
		v = strings.TrimSpace(v)
		u, err := url.Parse(v)
		if err != nil {
			return nil, err
		}

		err = checkUrl(u)
		if err != nil {
			return nil, err
		}

		all[i] = *u
	}
	us := URLs(all)
	us.Sort()

	return us, nil
}

func checkUrl(u *url.URL) (err error) {
	// check url
	if u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "unix" && u.Scheme != "unixs" {
		return fmt.Errorf("URL scheme must be http, https, unix, or unixs: %s", u)
	}

	if _, _, err := net.SplitHostPort(u.Host); err != nil {
		return fmt.Errorf(`URL address does not have the form "host:port": %s`, u)
	}
	if u.Path != "" {
		return fmt.Errorf("URL must not contain a path: %s", u)
	}
	return
}

func MustNewURLs(strs []string) URLs {
	urls, err := NewURLs(strs)
	if err != nil {
		panic(err)
	}
	return urls
}

func (us URLs) String() string {
	return strings.Join(us.StringSlice(), ",")
}

func (us *URLs) Sort() {
	sort.Sort(us)
}
func (us URLs) Len() int           { return len(us) }
func (us URLs) Less(i, j int) bool { return us[i].String() < us[j].String() }
func (us URLs) Swap(i, j int)      { us[i], us[j] = us[j], us[i] }

func (us URLs) StringSlice() []string {
	out := make([]string, len(us))
	for i := range us {
		out[i] = us[i].String()
	}

	return out
}
