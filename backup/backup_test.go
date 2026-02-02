package backup

import "testing"

func TestMethodConstants(t *testing.T) {
	// Verify iota sequence
	if Default != 0 {
		t.Errorf("Default = %d, want 0", Default)
	}
	if API != 1 {
		t.Errorf("API = %d, want 1", API)
	}
	if Vacuum != 2 {
		t.Errorf("Vacuum = %d, want 2", Vacuum)
	}
}

func TestMethodsAreDistinct(t *testing.T) {
	methods := []Method{Default, API, Vacuum}
	seen := make(map[Method]bool)

	for _, m := range methods {
		if seen[m] {
			t.Errorf("duplicate method value: %d", m)
		}
		seen[m] = true
	}
}
