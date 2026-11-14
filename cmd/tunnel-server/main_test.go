package main

import "testing"

func TestParseRange(t *testing.T) {
	for in, want := range map[string][2]int{
		"20000-29999": {20000, 29999},
		"1024-65535":  {1024, 65535},
		"30000-30000": {30000, 30000},
	} {
		min, max, err := parseRange(in)
		if err != nil || min != want[0] || max != want[1] {
			t.Errorf("parseRange(%q) = %d, %d, %v", in, min, max, err)
		}
	}
	for _, in := range []string{
		"",            // nothing
		"20000",       // no upper bound
		"abc-def",     // not numbers
		"20000-",      // missing upper bound
		"-29999",      // missing lower bound
		"29999-20000", // reversed
		"80-9000",     // includes privileged ports
		"20000-70000", // beyond the valid port range
		"1-2-3",       // too many parts
	} {
		if min, max, err := parseRange(in); err == nil {
			t.Errorf("parseRange(%q) accepted as %d-%d", in, min, max)
		}
	}
}
