package persistencekit_test

import (
	"strings"
	"testing"

	. "github.com/dogmatiq/persistencekit"
)

func TestParseURL(t *testing.T) {
	t.Run("it returns a config for valid URLs", func(t *testing.T) {
		cases := []struct {
			Name string
			URL  string
		}{
			{"memory", "memory:///silo"},
			{"postgres", "postgres://user:pass@localhost/db"},
			{"postgresql", "postgresql://user:pass@localhost/db"},
			{"dynamodb", "dynamodb:///prefix"},
			{"s3", "s3:///bucket"},
		}
		for _, tc := range cases {
			t.Run(tc.Name, func(t *testing.T) {
				cfg, err := ParseURL(t.Context(), tc.URL)
				if err != nil {
					t.Fatal(err)
				}

				d, err := cfg.NewDriver(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				defer d.Close()
			})
		}
	})

	t.Run("when the URL is invalid", func(t *testing.T) {
		cases := []struct {
			Name    string
			URL     string
			WantErr string
		}{
			{"no scheme", "no-scheme", `persistence driver URL has no scheme: "no-scheme"`},
			{"unsupported scheme", "redis://localhost", `unsupported persistence driver scheme "redis"`},
			{"malformed", "://", "cannot parse persistence driver URL:"},
		}
		for _, tc := range cases {
			t.Run(tc.Name, func(t *testing.T) {
				_, err := ParseURL(t.Context(), tc.URL)
				if err == nil {
					t.Fatal("expected an error")
				}

				if !strings.Contains(err.Error(), tc.WantErr) {
					t.Fatalf("unexpected error: got %q, want substring %q", err.Error(), tc.WantErr)
				}
			})
		}
	})
}
