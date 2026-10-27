package redis

import (
	"math"
	"strconv"
	"strings"
)

func formatScore(score float64) []byte {
	if math.IsInf(score, 1) {
		return []byte("inf")
	}
	if math.IsInf(score, -1) {
		return []byte("-inf")
	}
	return []byte(strconv.FormatFloat(score, 'g', -1, 64))
}

func parseScore(raw []byte) (float64, error) {
	s := strings.ToLower(string(raw))
	switch s {
	case "+inf", "inf":
		return math.Inf(1), nil
	case "-inf":
		return math.Inf(-1), nil
	}
	score, err := strconv.ParseFloat(string(raw), 64)
	if err != nil || math.IsNaN(score) {
		return 0, errf("ERR value is not a valid float")
	}
	return score, nil
}

func parseBound(raw []byte) (scoreBound, error) {
	exclusive := false
	if len(raw) > 0 && raw[0] == '(' {
		exclusive = true
		raw = raw[1:]
	}
	score, err := parseScore(raw)
	if err != nil {
		return scoreBound{}, err
	}
	return scoreBound{value: score, exclusive: exclusive}, nil
}

func scoreArray(entries []zEntry, withScores bool) []any {
	out := make([]any, 0, len(entries)*2)
	for _, entry := range entries {
		out = append(out, entry.member)
		if withScores {
			out = append(out, formatScore(entry.score))
		}
	}
	return out
}

func (c *client) zadd(args [][]byte) (any, bool, error) {
	if len(args) < 4 || len(args)%2 != 0 {
		return nil, false, errf("ERR wrong number of arguments for 'zadd' command")
	}
	entries := make([]zEntry, 0, (len(args)-2)/2)
	for i := 2; i < len(args); i += 2 {
		score, err := parseScore(args[i])
		if err != nil {
			return nil, false, err
		}
		entries = append(entries, zEntry{score: score, member: args[i+1]})
	}
	n, err := c.server.store.ZAdd(args[1], entries)
	return n, false, err
}

func (c *client) zscore(args [][]byte) (any, bool, error) {
	if len(args) != 3 {
		return nil, false, errf("ERR wrong number of arguments for 'zscore' command")
	}
	score, ok, err := c.server.store.ZScore(args[1], args[2])
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	return formatScore(score), false, nil
}

func (c *client) zrem(args [][]byte) (any, bool, error) {
	if len(args) < 3 {
		return nil, false, errf("ERR wrong number of arguments for 'zrem' command")
	}
	n, err := c.server.store.ZRem(args[1], args[2:]...)
	return n, false, err
}

func (c *client) zcard(args [][]byte) (any, bool, error) {
	if len(args) != 2 {
		return nil, false, errf("ERR wrong number of arguments for 'zcard' command")
	}
	n, err := c.server.store.ZCard(args[1])
	return n, false, err
}

func (c *client) zrange(cmd string, args [][]byte) (any, bool, error) {
	if len(args) < 4 {
		return nil, false, errf("ERR wrong number of arguments for '%s' command", cmd)
	}
	start, err := parseInt(args[2])
	if err != nil {
		return nil, false, errf("ERR value is not an integer or out of range")
	}
	stop, err := parseInt(args[3])
	if err != nil {
		return nil, false, errf("ERR value is not an integer or out of range")
	}
	withScores := false
	if len(args) > 4 {
		if len(args) != 5 || !strings.EqualFold(string(args[4]), "withscores") {
			return nil, false, errf("ERR syntax error")
		}
		withScores = true
	}
	entries, err := c.server.store.ZRange(args[1], start, stop, cmd == "zrevrange")
	if err != nil {
		return nil, false, err
	}
	return scoreArray(entries, withScores), false, nil
}

func (c *client) zrank(cmd string, args [][]byte) (any, bool, error) {
	if len(args) != 3 {
		return nil, false, errf("ERR wrong number of arguments for '%s' command", cmd)
	}
	rank, ok, err := c.server.store.ZRank(args[1], args[2], cmd == "zrevrank")
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	return rank, false, nil
}

func (c *client) zincrby(args [][]byte) (any, bool, error) {
	if len(args) != 4 {
		return nil, false, errf("ERR wrong number of arguments for 'zincrby' command")
	}
	delta, err := parseScore(args[2])
	if err != nil {
		return nil, false, err
	}
	score, err := c.server.store.ZIncrBy(args[1], args[3], delta)
	if err != nil {
		return nil, false, err
	}
	return formatScore(score), false, nil
}

func (c *client) zcount(args [][]byte) (any, bool, error) {
	if len(args) != 4 {
		return nil, false, errf("ERR wrong number of arguments for 'zcount' command")
	}
	min, err := parseBound(args[2])
	if err != nil {
		return nil, false, err
	}
	max, err := parseBound(args[3])
	if err != nil {
		return nil, false, err
	}
	n, err := c.server.store.ZCount(args[1], min, max)
	return n, false, err
}

func (c *client) zrangebyscore(args [][]byte) (any, bool, error) {
	if len(args) < 4 {
		return nil, false, errf("ERR wrong number of arguments for 'zrangebyscore' command")
	}
	min, err := parseBound(args[2])
	if err != nil {
		return nil, false, err
	}
	max, err := parseBound(args[3])
	if err != nil {
		return nil, false, err
	}
	withScores := false
	offset, count := int64(0), int64(-1)
	for i := 4; i < len(args); i++ {
		switch strings.ToLower(string(args[i])) {
		case "withscores":
			withScores = true
		case "limit":
			if i+2 >= len(args) {
				return nil, false, errf("ERR syntax error")
			}
			offset, err = parseInt(args[i+1])
			if err != nil {
				return nil, false, errf("ERR value is not an integer or out of range")
			}
			count, err = parseInt(args[i+2])
			if err != nil {
				return nil, false, errf("ERR value is not an integer or out of range")
			}
			i += 2
		default:
			return nil, false, errf("ERR syntax error")
		}
	}
	entries, err := c.server.store.ZRange(args[1], 0, -1, false)
	if err != nil {
		return nil, false, err
	}
	filtered := make([]zEntry, 0, len(entries))
	for _, entry := range entries {
		if min.minOK(entry.score) && max.maxOK(entry.score) {
			filtered = append(filtered, entry)
		}
	}
	if offset > 0 {
		if offset >= int64(len(filtered)) {
			filtered = nil
		} else {
			filtered = filtered[offset:]
		}
	}
	if count >= 0 && int64(len(filtered)) > count {
		filtered = filtered[:count]
	}
	return scoreArray(filtered, withScores), false, nil
}

func (c *client) keys(args [][]byte) (any, bool, error) {
	if len(args) != 2 {
		return nil, false, errf("ERR wrong number of arguments for 'keys' command")
	}
	keys, err := c.server.store.Keys(string(args[1]))
	if err != nil {
		return nil, false, err
	}
	return toAnyArray(keys), false, nil
}

func (c *client) scan(args [][]byte) (any, bool, error) {
	if len(args) < 2 {
		return nil, false, errf("ERR wrong number of arguments for 'scan' command")
	}
	cursor, err := strconv.ParseUint(string(args[1]), 10, 64)
	if err != nil {
		return nil, false, errf("ERR invalid cursor")
	}
	pattern := "*"
	count := 10
	typeFilter := ""
	for i := 2; i < len(args); i++ {
		switch strings.ToLower(string(args[i])) {
		case "match":
			if i+1 >= len(args) {
				return nil, false, errf("ERR syntax error")
			}
			pattern = string(args[i+1])
			i++
		case "count":
			if i+1 >= len(args) {
				return nil, false, errf("ERR syntax error")
			}
			count, err = strconv.Atoi(string(args[i+1]))
			if err != nil || count <= 0 {
				return nil, false, errf("ERR syntax error")
			}
			i++
		case "type":
			if i+1 >= len(args) {
				return nil, false, errf("ERR syntax error")
			}
			typeFilter = strings.ToLower(string(args[i+1]))
			i++
		default:
			return nil, false, errf("ERR syntax error")
		}
	}
	next, keys, err := c.server.store.Scan(cursor, pattern, count, typeFilter)
	if err != nil {
		return nil, false, err
	}
	return []any{[]byte(strconv.FormatUint(next, 10)), toAnyArray(keys)}, false, nil
}

func (c *client) subscribe(cmd string, args [][]byte) (any, bool, error) {
	if len(args) < 2 {
		return nil, false, errf("ERR wrong number of arguments for '%s' command", cmd)
	}
	pattern := cmd == "psubscribe"
	kind := "subscribe"
	if pattern {
		kind = "psubscribe"
	}
	replies := make(multiReply, 0, len(args)-1)
	for _, name := range args[1:] {
		count := c.server.hub.subscribe(c, string(name), pattern)
		replies = append(replies, []any{[]byte(kind), name, int64(count)})
	}
	return replies, false, nil
}

func (c *client) unsubscribe(cmd string, args [][]byte) (any, bool, error) {
	pattern := cmd == "punsubscribe"
	kind := "unsubscribe"
	if pattern {
		kind = "punsubscribe"
	}
	var names []string
	if len(args) > 1 {
		for _, name := range args[1:] {
			names = append(names, string(name))
		}
	}
	replies := multiReply{}
	if len(names) == 0 {
		removed, counts := c.server.hub.unsubscribe(c, "", pattern, true)
		for i, name := range removed {
			replies = append(replies, []any{[]byte(kind), []byte(name), int64(counts[i])})
		}
		if len(removed) == 0 {
			replies = append(replies, []any{[]byte(kind), nil, int64(0)})
		}
		return replies, false, nil
	}
	for _, name := range names {
		removed, counts := c.server.hub.unsubscribe(c, name, pattern, false)
		for i, actual := range removed {
			replies = append(replies, []any{[]byte(kind), []byte(actual), int64(counts[i])})
		}
	}
	return replies, false, nil
}

func (c *client) pubsub(args [][]byte) (any, bool, error) {
	if len(args) < 2 {
		return nil, false, errf("ERR wrong number of arguments for 'pubsub' command")
	}
	switch strings.ToLower(string(args[1])) {
	case "channels":
		pattern := ""
		if len(args) > 2 {
			pattern = string(args[2])
		}
		return toAnyArray(stringsToBytes(c.server.hub.channelList(pattern))), false, nil
	case "numsub":
		channels := make([]string, 0, len(args)-2)
		for _, channel := range args[2:] {
			channels = append(channels, string(channel))
		}
		return c.server.hub.numSub(channels), false, nil
	case "numpat":
		return c.server.hub.numPat(), false, nil
	default:
		return nil, false, errf("ERR unknown PUBSUB subcommand")
	}
}

func stringsToBytes(values []string) [][]byte {
	out := make([][]byte, len(values))
	for i := range values {
		out[i] = []byte(values[i])
	}
	return out
}
