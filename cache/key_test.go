package cache

import (
	"testing"
)

func TestDeriveKeyStableAndSensitive(t *testing.T) {
	base := BuildInput{
		Content:    map[string]string{"main.go": "package main\nfunc main(){}"},
		Toolchain:  "go:1.26.0",
		Params:     map[string]string{"-tags": "netgo", "-o": "app"},
		ManagedEnv: map[string]string{"GOOS": "linux", "GOARCH": "amd64"},
	}
	// 顺序不同的 map 字面量必须语义等价。
	reordered := BuildInput{
		Toolchain:  "go:1.26.0",
		Params:     map[string]string{"-o": "app", "-tags": "netgo"},
		Content:    map[string]string{"main.go": "package main\nfunc main(){}"},
		ManagedEnv: map[string]string{"GOARCH": "amd64", "GOOS": "linux"},
	}
	k1, k2 := DeriveKey(base), DeriveKey(reordered)
	t.Logf("base key=%s", k1)
	if k1 != k2 {
		t.Fatalf("semantically equal inputs produced different keys: %s vs %s", k1, k2)
	}
	// 稳定：再次派生一致。
	if DeriveKey(base) != k1 {
		t.Fatalf("key derivation not stable")
	}

	cases := []struct {
		name string
		mut  func(*BuildInput)
	}{
		{"content", func(in *BuildInput) { in.Content["main.go"] = "package main\nfunc main(){}\n// x" }},
		{"toolchain", func(in *BuildInput) { in.Toolchain = "go:1.25.0" }},
		{"param-value", func(in *BuildInput) { in.Params["-tags"] = "osusergo" }},
		{"param-added", func(in *BuildInput) { in.Params["-race"] = "true" }},
		{"env-value", func(in *BuildInput) { in.ManagedEnv["GOOS"] = "darwin" }},
		{"env-added", func(in *BuildInput) { in.ManagedEnv["CGO_ENABLED"] = "0" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			changed := base
			// 深拷贝避免用例间污染。
			changed.Content = cloneMap(base.Content)
			changed.Params = cloneMap(base.Params)
			changed.ManagedEnv = cloneMap(base.ManagedEnv)
			tc.mut(&changed)
			kk := DeriveKey(changed)
			t.Logf("changed(%s) key=%s", tc.name, kk)
			if kk == k1 {
				t.Fatalf("key did not change after %s mutation", tc.name)
			}
		})
	}
}

func TestDeriveKeyEmptyVsAbsent(t *testing.T) {
	a := DeriveKey(BuildInput{Toolchain: "x", Params: map[string]string{}})
	b := DeriveKey(BuildInput{Toolchain: "x"})
	c := DeriveKey(BuildInput{Toolchain: "y"})
	t.Logf("empty-map key=%s nil-map key=%s other key=%s", a, b, c)
	if a != b {
		t.Fatalf("nil map and empty map should be semantically equal")
	}
	if a == c {
		t.Fatalf("different toolchain produced same key")
	}
}

func TestEnvWhitelistFilter(t *testing.T) {
	in := BuildInput{
		Toolchain:  "t",
		ManagedEnv: map[string]string{"GOOS": "linux", "SECRET_TOKEN": "hunter2"},
	}
	f1 := in.withEnvWhitelist([]string{"GOOS"})
	f2 := in.withEnvWhitelist([]string{"GOOS"})
	if DeriveKey(f1) != DeriveKey(f2) {
		t.Fatal("filtered keys unstable")
	}
	// 白名单外变量变化不影响键。
	in.ManagedEnv["SECRET_TOKEN"] = "changed"
	if DeriveKey(in.withEnvWhitelist([]string{"GOOS"})) != DeriveKey(f1) {
		t.Fatal("non-whitelisted env must not participate in key")
	}
	// 白名单内变量变化必须影响键。
	in.ManagedEnv["GOOS"] = "windows"
	if DeriveKey(in.withEnvWhitelist([]string{"GOOS"})) == DeriveKey(f1) {
		t.Fatal("whitelisted env change must change key")
	}
}
