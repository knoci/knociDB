package redis

import "testing"

func TestMatchGlob(t *testing.T) {
	tests := []struct {
		pattern string
		value   string
		want    bool
	}{
		{"", "", true},
		{"", "a", false},
		{"*", "", true},
		{"*", "abc", true},
		{"a*", "abc", true},
		{"a*", "xabc", false},
		{"a?c", "abc", true},
		{"a?", "a", false},
		{"[abc]", "b", true},
		{"[abc]", "d", false},
		{"[a-z]", "m", true},
		{"[a-z]", "M", false},
		{"[^a]", "b", true},
		{"[^a]", "a", false},
		{"[!a]", "a", false},
		{"[\\]]", "]", true},
		{"\\*", "*", true},
		{"\\*", "a", false},
		{"\\", "", false},
		{"[abc", "a", false},
		{"[", "", false},
		{"a[b-d]e", "ace", true},
		{"a[b-d]e", "aee", false},
	}
	for _, tt := range tests {
		if got := matchGlob([]byte(tt.pattern), []byte(tt.value)); got != tt.want {
			t.Errorf("matchGlob(%q, %q) = %v, want %v", tt.pattern, tt.value, got, tt.want)
		}
	}
}
