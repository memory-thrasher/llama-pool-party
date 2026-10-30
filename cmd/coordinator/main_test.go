package main

import "testing"

func TestRedactDSN(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"no secrets", "user=sid dbname=postgres", "user=sid dbname=postgres"},
		{"password", "host=127.0.0.1 password=hunter2 dbname=postgres", "host=127.0.0.1 password=***** dbname=postgres"},
		{"passfile", "user=sid passfile=/run/.pgpass dbname=postgres", "user=sid passfile=***** dbname=postgres"},
		{"sslkey", "sslkey=/tmp/key.pem dbname=postgres", "sslkey=***** dbname=postgres"},
		{"sslpassword", "sslpassword=hunter2 dbname=postgres", "sslpassword=***** dbname=postgres"},
		{"whitespace normalized", "  user=sid   dbname=postgres  ", "user=sid dbname=postgres"},
		{"not a secret key", "user=passphrase dbname=postgres", "user=passphrase dbname=postgres"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := redactDSN(tc.in); got != tc.want {
				t.Errorf("redactDSN(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
