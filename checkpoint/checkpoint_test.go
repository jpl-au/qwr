package checkpoint

import "testing"

func TestModeConstants(t *testing.T) {
	tests := []struct {
		mode Mode
		want string
	}{
		{None, ""},
		{Passive, "PASSIVE"},
		{Full, "FULL"},
		{Restart, "RESTART"},
		{Truncate, "TRUNCATE"},
	}

	for _, tt := range tests {
		if string(tt.mode) != tt.want {
			t.Errorf("Mode %q = %q, want %q", tt.mode, string(tt.mode), tt.want)
		}
	}
}

func TestNoneIsEmpty(t *testing.T) {
	if None != "" {
		t.Errorf("None = %q, want empty string", None)
	}
}

func TestModesAreDistinct(t *testing.T) {
	modes := []Mode{Passive, Full, Restart, Truncate}
	seen := make(map[Mode]bool)

	for _, m := range modes {
		if seen[m] {
			t.Errorf("duplicate mode value: %q", m)
		}
		seen[m] = true
	}
}
