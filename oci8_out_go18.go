// +build !go1.8

package oci8

func handleOutput(v interface{}) (outValue, bool) {
	return outValue{}, false
}
