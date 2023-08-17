package transport

import (
	types "github.com/ColdToo/Cold2DB/transport/types"
	"net/url"
	"sync"
)

/*
	每个节点可能提供了多个URL供其他节点正常访问，当其中一个访问失败时，我们应该可以尝试访问另一个。
	urlPicker提供的主要功能就是在这些URL之间进行切换
*/
type urlPicker struct {
	mu     sync.Mutex // guards urls and picked
	urls   types.URLs
	picked int
}

func newURLPicker(urls types.URLs) *urlPicker {
	return &urlPicker{
		urls: urls,
	}
}

func (p *urlPicker) update(urls types.URLs) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.urls = urls
	p.picked = 0
}

func (p *urlPicker) pick() url.URL {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.urls[p.picked]
}

// unreachable notices the picker that the given url is unreachable,
// and it should use other possible urls.
func (p *urlPicker) unreachable(u url.URL) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if u == p.urls[p.picked] {
		p.picked = (p.picked + 1) % len(p.urls)
	}
}
