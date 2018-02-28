// +build !go1.9

package oci8

func handleOutput(v interface{}) (outValue, bool) {
	return outValue{}, false
}
