// +build !go1.8

package oci8

import (
	"database/sql"
)

func handleOutput(v interface{}) (outValue, bool) {
	if out, ok := v.(sql.Out); ok {
		return outValue{
			Dest: out.Dest,
			In:   out.In,
		}, true
	}
	return outValue{}, false
}
