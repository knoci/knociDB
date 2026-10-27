package redis

// matchGlob 实现 Redis KEYS/SCAN 使用的简单 glob：
// * 任意多个字符，? 单个字符，[abc]/[a-z]/[^a] 字符集，\ 转义。
func matchGlob(pattern, value []byte) bool {
	if len(pattern) == 0 {
		return len(value) == 0
	}
	switch pattern[0] {
	case '*':
		for i := 0; i <= len(value); i++ {
			if matchGlob(pattern[1:], value[i:]) {
				return true
			}
		}
		return false
	case '?':
		return len(value) > 0 && matchGlob(pattern[1:], value[1:])
	case '[':
		end := 1
		for end < len(pattern) && pattern[end] != ']' {
			if pattern[end] == '\\' && end+1 < len(pattern) {
				end++
			}
			end++
		}
		if end >= len(pattern) || len(value) == 0 {
			return false
		}
		class := pattern[1:end]
		match, ok := matchClass(class, value[0])
		if !ok {
			return false
		}
		return match && matchGlob(pattern[end+1:], value[1:])
	case '\\':
		if len(pattern) < 2 || len(value) == 0 {
			return false
		}
		return pattern[1] == value[0] && matchGlob(pattern[2:], value[1:])
	default:
		return len(value) > 0 && pattern[0] == value[0] && matchGlob(pattern[1:], value[1:])
	}
}

func matchClass(class []byte, ch byte) (bool, bool) {
	negate := false
	if len(class) > 0 && (class[0] == '^' || class[0] == '!') {
		negate = true
		class = class[1:]
	}
	matched := false
	for i := 0; i < len(class); i++ {
		if class[i] == '\\' && i+1 < len(class) {
			i++
			if class[i] == ch {
				matched = true
			}
			continue
		}
		if i+2 < len(class) && class[i+1] == '-' {
			lo, hi := class[i], class[i+2]
			if lo <= ch && ch <= hi {
				matched = true
			}
			i += 2
			continue
		}
		if class[i] == ch {
			matched = true
		}
	}
	if negate {
		matched = !matched
	}
	return matched, true
}
