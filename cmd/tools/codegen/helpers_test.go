package codegen

import "testing"

func TestSnakeCaseToPascalCase(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty", in: "", want: ""},
		{name: "single_lower", in: "a", want: "A"},
		{name: "single_upper", in: "A", want: "A"},
		{name: "simple", in: "hello", want: "Hello"},
		{name: "two_words", in: "hello_world", want: "HelloWorld"},
		{name: "all_caps", in: "HELLO_WORLD", want: "HelloWorld"},
		{name: "leading_underscore", in: "_leading", want: "Leading"},
		{name: "trailing_underscore", in: "trailing_", want: "Trailing"},
		{name: "double_underscore", in: "a__b", want: "AB"},
		{name: "only_underscores", in: "__", want: ""},
		{name: "common_id", in: "user_id", want: "UserId"},
		{name: "with_digits", in: "http_server_v2", want: "HttpServerV2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SnakeCaseToPascalCase(tt.in)
			if got != tt.want {
				t.Fatalf("SnakeCaseToPascalCase(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
