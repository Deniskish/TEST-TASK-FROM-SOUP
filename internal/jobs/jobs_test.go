package jobs

import "testing"

// TestValidation проверяет границы полей задания, нормализацию модели, число попыток по умолчанию
// и допустимые символы ключа идемпотентности.
func TestValidation(t *testing.T) {
	for _, r := range []Request{{Model: ""}, {Model: "m", DurationMS: -1}, {Model: "m", DurationMS: 60001}, {Model: "m", MaxAttempts: 21}, {Model: "m", FailUntilAttempt: -1}} {
		if r.Normalize() == nil {
			t.Fatalf("accepted invalid input: %+v", r)
		}
	}
	r := Request{Model: " m "}
	if err := r.Normalize(); err != nil || r.Model != "m" || r.MaxAttempts != 5 {
		t.Fatalf("normalization: %+v %v", r, err)
	}
	for _, s := range []string{"", "a b", "a\n", "ключ"} {
		if ValidKey(s) {
			t.Fatalf("accepted key %q", s)
		}
	}
	if !ValidKey("retry-123") {
		t.Fatal("valid key rejected")
	}
}
