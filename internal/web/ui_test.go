package web

import (
	"os/exec"
	"testing"
)

func TestBrowserSafetyLogic(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("Node.js is required for JavaScript regression tests")
	}
	if output, err := exec.Command(node, "--test", "ui_regression_test.cjs").CombinedOutput(); err != nil {
		t.Fatalf("browser logic: %v\n%s", err, output)
	}
}
