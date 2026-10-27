package redis

import (
	"sort"
	"sync"
)

type pubsubHub struct {
	mu       sync.RWMutex
	channels map[string]map[*client]struct{}
	patterns map[string]map[*client]struct{}
}

func newPubsubHub() *pubsubHub {
	return &pubsubHub{
		channels: make(map[string]map[*client]struct{}),
		patterns: make(map[string]map[*client]struct{}),
	}
}

func (h *pubsubHub) subscribe(c *client, channel string, pattern bool) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	target := h.channels
	if pattern {
		target = h.patterns
	}
	subs := target[channel]
	if subs == nil {
		subs = make(map[*client]struct{})
		target[channel] = subs
	}
	subs[c] = struct{}{}

	c.subMu.Lock()
	if pattern {
		c.psubs[channel] = struct{}{}
	} else {
		c.subs[channel] = struct{}{}
	}
	count := len(c.subs) + len(c.psubs)
	c.subMu.Unlock()
	return count
}

// unsubscribe 返回被移除的频道列表和每个频道对应的剩余订阅数。
func (h *pubsubHub) unsubscribe(c *client, channel string, pattern bool, all bool) ([]string, []int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	target := h.channels
	if pattern {
		target = h.patterns
	}
	c.subMu.Lock()
	var names []string
	if all {
		for name := range c.subs {
			if pattern {
				break
			}
			names = append(names, name)
		}
		for name := range c.psubs {
			if !pattern {
				break
			}
			names = append(names, name)
		}
	} else {
		names = []string{channel}
	}
	c.subMu.Unlock()

	counts := make([]int, 0, len(names))
	for _, name := range names {
		if subs := target[name]; subs != nil {
			delete(subs, c)
			if len(subs) == 0 {
				delete(target, name)
			}
		}
		c.subMu.Lock()
		if pattern {
			delete(c.psubs, name)
		} else {
			delete(c.subs, name)
		}
		count := len(c.subs) + len(c.psubs)
		c.subMu.Unlock()
		counts = append(counts, count)
	}
	return names, counts
}

func (h *pubsubHub) removeClient(c *client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for name, subs := range h.channels {
		delete(subs, c)
		if len(subs) == 0 {
			delete(h.channels, name)
		}
	}
	for name, subs := range h.patterns {
		delete(subs, c)
		if len(subs) == 0 {
			delete(h.patterns, name)
		}
	}
	c.subMu.Lock()
	c.subs = make(map[string]struct{})
	c.psubs = make(map[string]struct{})
	c.subMu.Unlock()
}

func (h *pubsubHub) publish(channel string, payload []byte) int64 {
	h.mu.RLock()
	channelClients := make([]*client, 0, len(h.channels[channel]))
	for c := range h.channels[channel] {
		channelClients = append(channelClients, c)
	}
	type patternClient struct {
		pattern string
		client  *client
	}
	var patternClients []patternClient
	for pattern, clients := range h.patterns {
		if !matchGlob([]byte(pattern), []byte(channel)) {
			continue
		}
		for c := range clients {
			patternClients = append(patternClients, patternClient{pattern: pattern, client: c})
		}
	}
	h.mu.RUnlock()

	var receivers int64
	for _, c := range channelClients {
		if err := c.send([]any{[]byte("message"), []byte(channel), payload}); err == nil {
			receivers++
		}
	}
	for _, item := range patternClients {
		if err := item.client.send([]any{[]byte("pmessage"), []byte(item.pattern), []byte(channel), payload}); err == nil {
			receivers++
		}
	}
	return receivers
}

func (h *pubsubHub) channelList(pattern string) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]string, 0, len(h.channels))
	for name, subs := range h.channels {
		if len(subs) == 0 {
			continue
		}
		if pattern != "" && !matchGlob([]byte(pattern), []byte(name)) {
			continue
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func (h *pubsubHub) numSub(channels []string) []any {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]any, 0, len(channels)*2)
	for _, channel := range channels {
		out = append(out, []byte(channel), int64(len(h.channels[channel])))
	}
	return out
}

func (h *pubsubHub) numPat() int64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return int64(len(h.patterns))
}
