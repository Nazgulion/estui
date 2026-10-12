//go:build !unix

package web

import "fmt"

func acquireProcessLock(string) (func(), error) {
	return nil, fmt.Errorf("production migration locking currently requires a Unix platform")
}
