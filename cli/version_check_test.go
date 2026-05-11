package main

import "testing"

func TestParseSemver(t *testing.T) {
	cases := []struct {
		in      string
		want    semver
		wantErr bool
	}{
		{"1.2.3", semver{1, 2, 3}, false},
		{"v1.2.3", semver{1, 2, 3}, false},
		{"  v1.2.3  ", semver{1, 2, 3}, false},
		{"0.1.0", semver{0, 1, 0}, false},
		{"10.20.30", semver{10, 20, 30}, false},
		{"1.2.3-rc.1", semver{1, 2, 3}, false},
		{"v1.2.3-rc.1", semver{1, 2, 3}, false},
		{"1.2.3+build.5", semver{1, 2, 3}, false},
		{"1.2.3-rc.1+build.5", semver{1, 2, 3}, false},

		// Errors.
		{"", semver{}, true},
		{"1.2", semver{}, true},
		{"1", semver{}, true},
		{"v", semver{}, true},
		{"a.b.c", semver{}, true},
		{"1.2.x", semver{}, true},
		{"-1.2.3", semver{}, true},
	}
	for _, c := range cases {
		got, err := parseSemver(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("parseSemver(%q): expected error, got %+v", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseSemver(%q): unexpected error: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("parseSemver(%q) = %+v, want %+v", c.in, got, c.want)
		}
	}
}

func TestSemverString(t *testing.T) {
	cases := []struct {
		in   semver
		want string
	}{
		{semver{0, 0, 0}, "0.0.0"},
		{semver{1, 2, 3}, "1.2.3"},
		{semver{10, 20, 30}, "10.20.30"},
	}
	for _, c := range cases {
		if got := c.in.String(); got != c.want {
			t.Errorf("(%+v).String() = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestInSupportWindow(t *testing.T) {
	cases := []struct {
		name      string
		cli       semver
		cloud     semver
		inWindow  bool
	}{
		// Identical minors → patch drift always OK.
		{"same minor, same patch", semver{1, 5, 0}, semver{1, 5, 0}, true},
		{"same minor, cli ahead in patch", semver{1, 5, 3}, semver{1, 5, 0}, true},
		{"same minor, cloud ahead in patch", semver{1, 5, 0}, semver{1, 5, 9}, true},

		// One minor drift either direction → in window.
		{"cli N, cloud N-1", semver{1, 5, 0}, semver{1, 4, 0}, true},
		{"cli N, cloud N+1", semver{1, 5, 0}, semver{1, 6, 0}, true},
		{"cli N, cloud N-1 with patch drift", semver{1, 5, 0}, semver{1, 4, 17}, true},

		// Two minor drift → outside window.
		{"cli N, cloud N-2", semver{1, 5, 0}, semver{1, 3, 0}, false},
		{"cli N, cloud N+2", semver{1, 5, 0}, semver{1, 7, 0}, false},
		{"cli N, cloud N-5", semver{1, 5, 0}, semver{1, 0, 0}, false},

		// Cross-major → always outside.
		{"cli 1.x, cloud 2.x", semver{1, 9, 0}, semver{2, 0, 0}, false},
		{"cli 2.x, cloud 1.x", semver{2, 0, 0}, semver{1, 9, 0}, false},
		{"cli 0.x, cloud 1.x", semver{0, 5, 0}, semver{1, 5, 0}, false},

		// v0.x: same rules — minor drift inside ±1 is still the window.
		{"cli 0.5, cloud 0.4", semver{0, 5, 0}, semver{0, 4, 0}, true},
		{"cli 0.5, cloud 0.6", semver{0, 5, 0}, semver{0, 6, 0}, true},
		{"cli 0.5, cloud 0.3", semver{0, 5, 0}, semver{0, 3, 0}, false},
	}
	for _, c := range cases {
		got := inSupportWindow(c.cli, c.cloud)
		if got != c.inWindow {
			t.Errorf("%s: inSupportWindow(%v, %v) = %v, want %v",
				c.name, c.cli, c.cloud, got, c.inWindow)
		}
	}
}
