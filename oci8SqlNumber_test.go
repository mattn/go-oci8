package oci8

import (
	"database/sql"
	"math"
	"testing"
)

// TestSelectDualNumber checks select dual for number types
func TestSelectDualNumber(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}

	queryResults := testQueryResults{}

	// bool
	queryResultBoolToInt := []testQueryResult{
		testQueryResult{
			args:    []interface{}{false},
			results: [][]interface{}{[]interface{}{int64(0)}},
		},
		testQueryResult{
			args:    []interface{}{true},
			results: [][]interface{}{[]interface{}{int64(1)}},
		},
	}
	// int8: -128 to 127
	queryResultInt8ToInt := []testQueryResult{
		testQueryResult{
			args:    []interface{}{int8(-128)},
			results: [][]interface{}{[]interface{}{int64(-128)}},
		},
		testQueryResult{
			args:    []interface{}{int8(-1)},
			results: [][]interface{}{[]interface{}{int64(-1)}},
		},
		testQueryResult{
			args:    []interface{}{int8(0)},
			results: [][]interface{}{[]interface{}{int64(0)}},
		},
		testQueryResult{
			args:    []interface{}{int8(1)},
			results: [][]interface{}{[]interface{}{int64(1)}},
		},
		testQueryResult{
			args:    []interface{}{int8(127)},
			results: [][]interface{}{[]interface{}{int64(127)}},
		},
	}
	// int16: -32768 to 32767
	queryResultInt16ToInt := []testQueryResult{
		testQueryResult{
			args:    []interface{}{int16(-32768)},
			results: [][]interface{}{[]interface{}{int64(-32768)}},
		},
		testQueryResult{
			args:    []interface{}{int16(-128)},
			results: [][]interface{}{[]interface{}{int64(-128)}},
		},
		testQueryResult{
			args:    []interface{}{int16(127)},
			results: [][]interface{}{[]interface{}{int64(127)}},
		},
		testQueryResult{
			args:    []interface{}{int16(32767)},
			results: [][]interface{}{[]interface{}{int64(32767)}},
		},
	}
	// int32: -2147483648 to 2147483647
	queryResultInt32ToInt := []testQueryResult{
		testQueryResult{
			args:    []interface{}{int32(-2147483648)},
			results: [][]interface{}{[]interface{}{int64(-2147483648)}},
		},
		testQueryResult{
			args:    []interface{}{int32(-32768)},
			results: [][]interface{}{[]interface{}{int64(-32768)}},
		},
		testQueryResult{
			args:    []interface{}{int32(32767)},
			results: [][]interface{}{[]interface{}{int64(32767)}},
		},
		testQueryResult{
			args:    []interface{}{int32(2147483647)},
			results: [][]interface{}{[]interface{}{int64(2147483647)}},
		},
	}
	// int64: -9223372036854775808 to 9223372036854775807
	queryResultInt64ToInt := []testQueryResult{
		testQueryResult{
			args:    []interface{}{int64(-9223372036854775808)},
			results: [][]interface{}{[]interface{}{int64(-9223372036854775808)}},
		},
		testQueryResult{
			args:    []interface{}{int64(-2147483648)},
			results: [][]interface{}{[]interface{}{int64(-2147483648)}},
		},
		testQueryResult{
			args:    []interface{}{int64(2147483647)},
			results: [][]interface{}{[]interface{}{int64(2147483647)}},
		},
		testQueryResult{
			args:    []interface{}{int64(9223372036854775807)},
			results: [][]interface{}{[]interface{}{int64(9223372036854775807)}},
		},
	}
	// uint8: 0 to 255
	queryResultUint8ToInt := []testQueryResult{
		testQueryResult{
			args:    []interface{}{uint8(0)},
			results: [][]interface{}{[]interface{}{int64(0)}},
		},
		testQueryResult{
			args:    []interface{}{uint8(1)},
			results: [][]interface{}{[]interface{}{int64(1)}},
		},
		testQueryResult{
			args:    []interface{}{uint8(127)},
			results: [][]interface{}{[]interface{}{int64(127)}},
		},
		testQueryResult{
			args:    []interface{}{uint8(128)},
			results: [][]interface{}{[]interface{}{int64(128)}},
		},
		testQueryResult{
			args:    []interface{}{uint8(255)},
			results: [][]interface{}{[]interface{}{int64(255)}},
		},
	}
	// uint16: 0 to 65535
	queryResultUint16ToInt := []testQueryResult{
		testQueryResult{
			args:    []interface{}{uint16(255)},
			results: [][]interface{}{[]interface{}{int64(255)}},
		},
		testQueryResult{
			args:    []interface{}{uint16(65535)},
			results: [][]interface{}{[]interface{}{int64(65535)}},
		},
	}
	// uint32: 0 to 4294967295
	queryResultUint32ToInt := []testQueryResult{
		testQueryResult{
			args:    []interface{}{uint32(65535)},
			results: [][]interface{}{[]interface{}{int64(65535)}},
		},
		testQueryResult{
			args:    []interface{}{uint32(4294967295)},
			results: [][]interface{}{[]interface{}{int64(4294967295)}},
		},
	}
	// uint64: 0 to 18446744073709551615
	// for 18446744073709551615 get: get rows error: query error: sql: converting argument $1 type: uint64 values with high bit set are not supported
	queryResultUint64ToInt := []testQueryResult{
		testQueryResult{
			args:    []interface{}{uint64(4294967295)},
			results: [][]interface{}{[]interface{}{int64(4294967295)}},
		},
		testQueryResult{
			args:    []interface{}{uint64(9223372036854775807)},
			results: [][]interface{}{[]interface{}{int64(9223372036854775807)}},
		},
	}
	// bool
	queryResultBoolToFloat := []testQueryResult{
		testQueryResult{
			args:    []interface{}{false},
			results: [][]interface{}{[]interface{}{float64(0)}},
		},
		testQueryResult{
			args:    []interface{}{true},
			results: [][]interface{}{[]interface{}{float64(1)}},
		},
	}
	// int8: -128 to 127
	queryResultInt8ToFloat := []testQueryResult{
		testQueryResult{
			args:    []interface{}{int8(-128)},
			results: [][]interface{}{[]interface{}{float64(-128)}},
		},
		testQueryResult{
			args:    []interface{}{int8(-1)},
			results: [][]interface{}{[]interface{}{float64(-1)}},
		},
		testQueryResult{
			args:    []interface{}{int8(0)},
			results: [][]interface{}{[]interface{}{float64(0)}},
		},
		testQueryResult{
			args:    []interface{}{int8(1)},
			results: [][]interface{}{[]interface{}{float64(1)}},
		},
		testQueryResult{
			args:    []interface{}{int8(127)},
			results: [][]interface{}{[]interface{}{float64(127)}},
		},
	}
	// int16: -32768 to 32767
	queryResultInt16ToFloat := []testQueryResult{
		testQueryResult{
			args:    []interface{}{int16(-32768)},
			results: [][]interface{}{[]interface{}{float64(-32768)}},
		},
		testQueryResult{
			args:    []interface{}{int16(-128)},
			results: [][]interface{}{[]interface{}{float64(-128)}},
		},
		testQueryResult{
			args:    []interface{}{int16(127)},
			results: [][]interface{}{[]interface{}{float64(127)}},
		},
		testQueryResult{
			args:    []interface{}{int16(32767)},
			results: [][]interface{}{[]interface{}{float64(32767)}},
		},
	}
	// int32: -2147483648 to 2147483647
	queryResultInt32ToFloat := []testQueryResult{
		testQueryResult{
			args:    []interface{}{int32(-2147483648)},
			results: [][]interface{}{[]interface{}{float64(-2147483648)}},
		},
		testQueryResult{
			args:    []interface{}{int32(-32768)},
			results: [][]interface{}{[]interface{}{float64(-32768)}},
		},
		testQueryResult{
			args:    []interface{}{int32(32767)},
			results: [][]interface{}{[]interface{}{float64(32767)}},
		},
		testQueryResult{
			args:    []interface{}{int32(2147483647)},
			results: [][]interface{}{[]interface{}{float64(2147483647)}},
		},
	}
	// int64: -9223372036854775808 to 9223372036854775807
	queryResultInt64ToFloat := []testQueryResult{
		testQueryResult{
			args:    []interface{}{int64(-9223372036854775808)},
			results: [][]interface{}{[]interface{}{float64(-9223372036854775808)}},
		},
		testQueryResult{
			args:    []interface{}{int64(-2147483648)},
			results: [][]interface{}{[]interface{}{float64(-2147483648)}},
		},
		testQueryResult{
			args:    []interface{}{int64(2147483647)},
			results: [][]interface{}{[]interface{}{float64(2147483647)}},
		},
		testQueryResult{
			args:    []interface{}{int64(9223372036854775807)},
			results: [][]interface{}{[]interface{}{float64(9223372036854775807)}},
		},
	}
	// uint8: 0 to 255
	queryResultUint8ToFloat := []testQueryResult{
		testQueryResult{
			args:    []interface{}{uint8(0)},
			results: [][]interface{}{[]interface{}{float64(0)}},
		},
		testQueryResult{
			args:    []interface{}{uint8(1)},
			results: [][]interface{}{[]interface{}{float64(1)}},
		},
		testQueryResult{
			args:    []interface{}{uint8(127)},
			results: [][]interface{}{[]interface{}{float64(127)}},
		},
		testQueryResult{
			args:    []interface{}{uint8(128)},
			results: [][]interface{}{[]interface{}{float64(128)}},
		},
		testQueryResult{
			args:    []interface{}{uint8(255)},
			results: [][]interface{}{[]interface{}{float64(255)}},
		},
	}
	// uint16: 0 to 65535
	queryResultUint16ToFloat := []testQueryResult{
		testQueryResult{
			args:    []interface{}{uint16(255)},
			results: [][]interface{}{[]interface{}{float64(255)}},
		},
		testQueryResult{
			args:    []interface{}{uint16(65535)},
			results: [][]interface{}{[]interface{}{float64(65535)}},
		},
	}
	// uint32: 0 to 4294967295
	queryResultUint32ToFloat := []testQueryResult{
		testQueryResult{
			args:    []interface{}{uint32(65535)},
			results: [][]interface{}{[]interface{}{float64(65535)}},
		},
		testQueryResult{
			args:    []interface{}{uint32(4294967295)},
			results: [][]interface{}{[]interface{}{float64(4294967295)}},
		},
	}
	// uint64: 0 to 18446744073709551615
	// for 18446744073709551615 get: get rows error: query error: sql: converting argument $1 type: uint64 values with high bit set are not supported
	queryResultUint64ToFloat := []testQueryResult{
		testQueryResult{
			args:    []interface{}{uint64(4294967295)},
			results: [][]interface{}{[]interface{}{float64(4294967295)}},
		},
		testQueryResult{
			args:    []interface{}{uint64(9223372036854775807)},
			results: [][]interface{}{[]interface{}{float64(9223372036854775807)}},
		},
	}
	// float32 positive: sign 1 bit, exponent 8 bits, Mantissa 23 bits
	queryResultFloat32PositiveToFloat := []testQueryResult{
		testQueryResult{
			args:    []interface{}{math.Float32frombits(0x00000000)}, // 0 00000000 00000000000000000000000
			results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0x00000000))}},
		},
		testQueryResult{
			args:    []interface{}{math.Float32frombits(0x40000000)}, // 0 10000000 00000000000000000000000
			results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0x40000000))}},
		},
		testQueryResult{
			args:    []interface{}{math.Float32frombits(0x40400000)}, // 0 10000000 10000000000000000000000
			results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0x40400000))}},
		},
		testQueryResult{
			args:    []interface{}{math.Float32frombits(0x40600000)}, // 0 10000000 11000000000000000000000
			results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0x40600000))}},
		},
		testQueryResult{
			args:    []interface{}{math.Float32frombits(0x40800000)}, // 0 10000001 00000000000000000000000
			results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0x40800000))}},
		},
		testQueryResult{
			args:    []interface{}{math.Float32frombits(0x40c00000)}, // 0 10000001 10000000000000000000000
			results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0x40c00000))}},
		},
		testQueryResult{
			args:    []interface{}{math.Float32frombits(0x40e00000)}, // 0 10000001 11000000000000000000000
			results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0x40e00000))}},
		},
		testQueryResult{
			args:    []interface{}{math.Float32frombits(0x60000000)}, // 0 11000000 00000000000000000000000
			results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0x60000000))}},
		},
		testQueryResult{
			args:    []interface{}{math.Float32frombits(0x60400000)}, // 0 11000000 10000000000000000000000
			results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0x60400000))}},
		},
		testQueryResult{
			args:    []interface{}{math.Float32frombits(0x60600000)}, // 0 11000000 11000000000000000000000
			results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0x60600000))}},
		},
		testQueryResult{
			args:    []interface{}{math.Float32frombits(0x407f8000)}, // 0 10000000 11111111000000000000000
			results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0x407f8000))}},
		},
		testQueryResult{
			args:    []interface{}{math.Float32frombits(0x407fc000)}, // 0 10000000 11111111100000000000000
			results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0x407fc000))}},
		},
		testQueryResult{
			args:    []interface{}{math.Float32frombits(0x407fe000)}, // 0 10000000 11111111110000000000000
			results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0x407fe000))}},
		},
		testQueryResult{
			args:    []interface{}{math.Float32frombits(0x407ff000)}, // 0 10000000 11111111111000000000000
			results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0x407ff000))}},
		},
		/*
			TODO: get decimal errors at this point, why?
			testQueryResult{
				args:    []interface{}{math.Float32frombits(0x407ff800)}, // 0 10000000 11111111111100000000000
				results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0x407ff800))}},
			},
			testQueryResult{
				args:    []interface{}{math.Float32frombits(0x407ffc00)}, // 0 10000000 11111111111110000000000
				results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0x407ffc00))}},
			},
		*/
	}
	// float32 negative: sign 1 bit, exponent 8 bits, Mantissa 23 bits
	queryResultFloat32NegativeToFloat := []testQueryResult{
		testQueryResult{
			args:    []interface{}{math.Float32frombits(0x80000000)}, // 1 00000000 00000000000000000000000
			results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0x80000000))}},
		},
		testQueryResult{
			args:    []interface{}{math.Float32frombits(0xc0000000)}, // 1 10000000 00000000000000000000000
			results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0xc0000000))}},
		},
		testQueryResult{
			args:    []interface{}{math.Float32frombits(0xc0400000)}, // 1 10000000 10000000000000000000000
			results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0xc0400000))}},
		},
		testQueryResult{
			args:    []interface{}{math.Float32frombits(0xc0600000)}, // 1 10000000 11000000000000000000000
			results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0xc0600000))}},
		},
		testQueryResult{
			args:    []interface{}{math.Float32frombits(0xc0800000)}, // 1 10000001 00000000000000000000000
			results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0xc0800000))}},
		},
		testQueryResult{
			args:    []interface{}{math.Float32frombits(0xc0c00000)}, // 1 10000001 10000000000000000000000
			results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0xc0c00000))}},
		},
		testQueryResult{
			args:    []interface{}{math.Float32frombits(0xc0e00000)}, // 1 10000001 11000000000000000000000
			results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0xc0e00000))}},
		},
		testQueryResult{
			args:    []interface{}{math.Float32frombits(0xe0000000)}, // 1 11000000 00000000000000000000000
			results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0xe0000000))}},
		},
		testQueryResult{
			args:    []interface{}{math.Float32frombits(0xe0400000)}, // 1 11000000 10000000000000000000000
			results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0xe0400000))}},
		},
		testQueryResult{
			args:    []interface{}{math.Float32frombits(0xe0600000)}, // 1 11000000 11000000000000000000000
			results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0xe0600000))}},
		},
		testQueryResult{
			args:    []interface{}{math.Float32frombits(0xc07f8000)}, // 1 10000000 11111111000000000000000
			results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0xc07f8000))}},
		},
		testQueryResult{
			args:    []interface{}{math.Float32frombits(0xc07fc000)}, // 1 10000000 11111111100000000000000
			results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0xc07fc000))}},
		},
		testQueryResult{
			args:    []interface{}{math.Float32frombits(0xc07fe000)}, // 1 10000000 11111111110000000000000
			results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0xc07fe000))}},
		},
		testQueryResult{
			args:    []interface{}{math.Float32frombits(0xc07ff000)}, // 1 10000000 11111111111000000000000
			results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0xc07ff000))}},
		},
		/*
			TODO: get decimal errors at this point, why?
			testQueryResult{
				args:    []interface{}{math.Float32frombits(0xc07ff800)}, // 1 10000000 11111111111100000000000
				results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0xc07ff800))}},
			},
			testQueryResult{
				args:    []interface{}{math.Float32frombits(0xc07ffc00)}, // 1 10000000 11111111111110000000000
				results: [][]interface{}{[]interface{}{float64(math.Float32frombits(0xc07ffc00))}},
			},
		*/
	}

	// TODO: added float64 positive and negative

	// https://ss64.com/ora/syntax-datatypes.html

	// INTEGER
	queryResults.query = "select cast (:1 as INTEGER) from dual"
	queryResults.queryResults = queryResultBoolToInt
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt8ToInt
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt16ToInt
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt32ToInt
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt64ToInt
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint8ToInt
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint16ToInt
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint32ToInt
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint64ToInt
	testRunQueryResults(t, queryResults)

	// INT
	queryResults.query = "select cast (:1 as INT) from dual"
	queryResults.queryResults = queryResultBoolToInt
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt8ToInt
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt16ToInt
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt32ToInt
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt64ToInt
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint8ToInt
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint16ToInt
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint32ToInt
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint64ToInt
	testRunQueryResults(t, queryResults)

	// SMALLINT
	queryResults.query = "select cast (:1 as SMALLINT) from dual"
	queryResults.queryResults = queryResultBoolToInt
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt8ToInt
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt16ToInt
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt32ToInt
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt64ToInt
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint8ToInt
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint16ToInt
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint32ToInt
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint64ToInt
	testRunQueryResults(t, queryResults)

	// NUMBER(38,10)
	queryResults.query = "select cast (:1 as NUMBER(38,10)) from dual"
	queryResults.queryResults = queryResultBoolToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt8ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt16ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt32ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt64ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint8ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint16ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint32ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint64ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultFloat32PositiveToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultFloat32NegativeToFloat
	testRunQueryResults(t, queryResults)

	// DEC(38,10)
	queryResults.query = "select cast (:1 as DEC(38,10)) from dual"
	queryResults.queryResults = queryResultBoolToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt8ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt16ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt32ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt64ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint8ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint16ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint32ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint64ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultFloat32PositiveToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultFloat32NegativeToFloat
	testRunQueryResults(t, queryResults)

	// DECIMAL(38,10)
	queryResults.query = "select cast (:1 as DECIMAL(38,10)) from dual"
	queryResults.queryResults = queryResultBoolToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt8ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt16ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt32ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt64ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint8ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint16ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint32ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint64ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultFloat32PositiveToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultFloat32NegativeToFloat
	testRunQueryResults(t, queryResults)

	// NUMERIC(38,10)
	queryResults.query = "select cast (:1 as NUMERIC(38,10)) from dual"
	queryResults.queryResults = queryResultBoolToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt8ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt16ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt32ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt64ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint8ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint16ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint32ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint64ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultFloat32PositiveToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultFloat32NegativeToFloat
	testRunQueryResults(t, queryResults)

	// FLOAT
	queryResults.query = "select cast (:1 as FLOAT) from dual"
	queryResults.queryResults = queryResultBoolToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt8ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt16ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt32ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt64ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint8ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint16ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint32ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint64ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultFloat32PositiveToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultFloat32NegativeToFloat
	testRunQueryResults(t, queryResults)

	// REAL
	queryResults.query = "select cast (:1 as REAL) from dual"
	queryResults.queryResults = queryResultBoolToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt8ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt16ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt32ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt64ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint8ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint16ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint32ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint64ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultFloat32PositiveToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultFloat32NegativeToFloat
	testRunQueryResults(t, queryResults)

	// BINARY_FLOAT
	queryResults.query = "select cast (:1 as BINARY_FLOAT) from dual"
	queryResults.queryResults = queryResultBoolToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt8ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt16ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint8ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint16ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultFloat32PositiveToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultFloat32NegativeToFloat
	testRunQueryResults(t, queryResults)

	// BINARY_DOUBLE
	queryResults.query = "select cast (:1 as BINARY_DOUBLE) from dual"
	queryResults.queryResults = queryResultBoolToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt8ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt16ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt32ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt64ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint8ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint16ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint32ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint64ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultFloat32PositiveToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultFloat32NegativeToFloat
	testRunQueryResults(t, queryResults)

	// https://tour.golang.org/basics/11

	// Go
	queryResults.query = "select :1 from dual"
	queryResults.queryResults = queryResultBoolToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt8ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt16ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt32ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultInt64ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint8ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint16ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint32ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultUint64ToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultFloat32PositiveToFloat
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultFloat32NegativeToFloat
	testRunQueryResults(t, queryResults)

	// sum
	queryResults.query = "select sum(A) from (select :1 as A from dual union select :2 as A from dual)"
	queryResults.queryResults = []testQueryResult{
		testQueryResult{
			args:    []interface{}{int64(1), int64(2)},
			results: [][]interface{}{[]interface{}{float64(3)}},
		},
		testQueryResult{
			args:    []interface{}{int64(1), float64(2.25)},
			results: [][]interface{}{[]interface{}{float64(3.25)}},
		},
		testQueryResult{
			args:    []interface{}{float64(1.5), float64(2.25)},
			results: [][]interface{}{[]interface{}{float64(3.75)}},
		},
	}
	testRunQueryResults(t, queryResults)
}

// TestDestructiveNumber checks insert, select, update, and delete of number types
func TestDestructiveNumber(t *testing.T) {
	if TestDisableDatabase || TestDisableDestructive {
		t.SkipNow()
	}

	// https://ss64.com/ora/syntax-datatypes.html

	// NUMBER negative
	tableName := "NUMBER_" + TestTimeString
	err := testExec(t, "create table "+tableName+" ( A NUMBER(10,2), B NUMBER(20,4), C NUMBER(38,8) )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer testDropTable(t, tableName)

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{-9999999.99, -999999999999999.9999, -9999999999999999999999999.99999999},
			[]interface{}{-21474836, -2147483648, -2147483648},
			[]interface{}{-1234567, -123456792, -123456792},
			[]interface{}{-1.98, -1.9873, -1.98730468},
			[]interface{}{-1, -1, -1},
			[]interface{}{-0.76, -0.7617, -0.76171875},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults := testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(-21474836), float64(-2147483648), float64(-2147483648)},
					[]interface{}{float64(-9999999.99), float64(-999999999999999.9999), float64(-9999999999999999999999999.99999999)},
					[]interface{}{float64(-1234567), float64(-123456792), float64(-123456792)},
					[]interface{}{float64(-1.98), float64(-1.9873), float64(-1.98730468)},
					[]interface{}{float64(-1), float64(-1), float64(-1)},
					[]interface{}{float64(-0.76), float64(-0.7617), float64(-0.76171875)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from "+tableName+" where A = :1",
		[][]interface{}{
			[]interface{}{-21474836},
			[]interface{}{-9999999.99},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(-1234567), float64(-123456792), float64(-123456792)},
					[]interface{}{float64(-1.98), float64(-1.9873), float64(-1.98730468)},
					[]interface{}{float64(-1), float64(-1), float64(-1)},
					[]interface{}{float64(-0.76), float64(-0.7617), float64(-0.76171875)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// test truncate
	err = testExec(t, "truncate table "+tableName, nil)
	if err != nil {
		t.Error("truncate error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName,
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// NUMBER positive
	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{0, 0, 0},
			[]interface{}{0.76, 0.7617, 0.76171875},
			[]interface{}{1, 1, 1},
			[]interface{}{1.98, 1.9873, 1.98730468},
			[]interface{}{12345679, 123456792, 123456792},
			[]interface{}{21474836, 2147483647, 2147483647},
			[]interface{}{9999999.99, 999999999999999.9999, 99999999999999999999999999.99999999},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(0), float64(0), float64(0)},
					[]interface{}{float64(0.76), float64(0.7617), float64(0.76171875)},
					[]interface{}{float64(1), float64(1), float64(1)},
					[]interface{}{float64(1.98), float64(1.9873), float64(1.98730468)},
					[]interface{}{float64(9999999.99), float64(999999999999999.9999), float64(99999999999999999999999999.99999999)},
					[]interface{}{float64(12345679), float64(123456792), float64(123456792)},
					[]interface{}{float64(21474836), float64(2147483647), float64(2147483647)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from "+tableName+" where A = :1",
		[][]interface{}{
			[]interface{}{0},
			[]interface{}{1},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(0.76), float64(0.7617), float64(0.76171875)},
					[]interface{}{float64(1.98), float64(1.9873), float64(1.98730468)},
					[]interface{}{float64(9999999.99), float64(999999999999999.9999), float64(99999999999999999999999999.99999999)},
					[]interface{}{float64(12345679), float64(123456792), float64(123456792)},
					[]interface{}{float64(21474836), float64(2147483647), float64(2147483647)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// DEC negative
	tableName = "DEC_" + TestTimeString
	err = testExec(t, "create table "+tableName+
		" ( A DEC(10,2), B DEC(20,4), C DEC(38,8) )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer testDropTable(t, tableName)

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{-9999999.99, -999999999999999.9999, -9999999999999999999999999.99999999},
			[]interface{}{-21474836, -2147483648, -2147483648},
			[]interface{}{-1234567, -123456792, -123456792},
			[]interface{}{-1.98, -1.9873, -1.98730468},
			[]interface{}{-1, -1, -1},
			[]interface{}{-0.76, -0.7617, -0.76171875},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(-21474836), float64(-2147483648), float64(-2147483648)},
					[]interface{}{float64(-9999999.99), float64(-999999999999999.9999), float64(-9999999999999999999999999.99999999)},
					[]interface{}{float64(-1234567), float64(-123456792), float64(-123456792)},
					[]interface{}{float64(-1.98), float64(-1.9873), float64(-1.98730468)},
					[]interface{}{float64(-1), float64(-1), float64(-1)},
					[]interface{}{float64(-0.76), float64(-0.7617), float64(-0.76171875)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from "+tableName+" where A = :1",
		[][]interface{}{
			[]interface{}{-21474836},
			[]interface{}{-9999999.99},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(-1234567), float64(-123456792), float64(-123456792)},
					[]interface{}{float64(-1.98), float64(-1.9873), float64(-1.98730468)},
					[]interface{}{float64(-1), float64(-1), float64(-1)},
					[]interface{}{float64(-0.76), float64(-0.7617), float64(-0.76171875)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// DEC positive
	err = testExec(t, "truncate table "+tableName, nil)
	if err != nil {
		t.Error("truncate error:", err)
	}

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{0, 0, 0},
			[]interface{}{0.76, 0.7617, 0.76171875},
			[]interface{}{1, 1, 1},
			[]interface{}{1.98, 1.9873, 1.98730468},
			[]interface{}{12345679, 123456792, 123456792},
			[]interface{}{21474836, 2147483647, 2147483647},
			[]interface{}{9999999.99, 999999999999999.9999, 99999999999999999999999999.99999999},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(0), float64(0), float64(0)},
					[]interface{}{float64(0.76), float64(0.7617), float64(0.76171875)},
					[]interface{}{float64(1), float64(1), float64(1)},
					[]interface{}{float64(1.98), float64(1.9873), float64(1.98730468)},
					[]interface{}{float64(9999999.99), float64(999999999999999.9999), float64(99999999999999999999999999.99999999)},
					[]interface{}{float64(12345679), float64(123456792), float64(123456792)},
					[]interface{}{float64(21474836), float64(2147483647), float64(2147483647)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from "+tableName+" where A = :1",
		[][]interface{}{
			[]interface{}{0},
			[]interface{}{1},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(0.76), float64(0.7617), float64(0.76171875)},
					[]interface{}{float64(1.98), float64(1.9873), float64(1.98730468)},
					[]interface{}{float64(9999999.99), float64(999999999999999.9999), float64(99999999999999999999999999.99999999)},
					[]interface{}{float64(12345679), float64(123456792), float64(123456792)},
					[]interface{}{float64(21474836), float64(2147483647), float64(2147483647)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// DECIMAL negative
	tableName = "DECIMAL_" + TestTimeString
	err = testExec(t, "create table "+tableName+" ( A DECIMAL(10,2), B DECIMAL(20,4), C DECIMAL(38,8) )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer testDropTable(t, tableName)

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{-9999999.99, -999999999999999.9999, -9999999999999999999999999.99999999},
			[]interface{}{-21474836, -2147483648, -2147483648},
			[]interface{}{-1234567, -123456792, -123456792},
			[]interface{}{-1.98, -1.9873, -1.98730468},
			[]interface{}{-1, -1, -1},
			[]interface{}{-0.76, -0.7617, -0.76171875},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(-21474836), float64(-2147483648), float64(-2147483648)},
					[]interface{}{float64(-9999999.99), float64(-999999999999999.9999), float64(-9999999999999999999999999.99999999)},
					[]interface{}{float64(-1234567), float64(-123456792), float64(-123456792)},
					[]interface{}{float64(-1.98), float64(-1.9873), float64(-1.98730468)},
					[]interface{}{float64(-1), float64(-1), float64(-1)},
					[]interface{}{float64(-0.76), float64(-0.7617), float64(-0.76171875)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from "+tableName+" where A = :1",
		[][]interface{}{
			[]interface{}{-21474836},
			[]interface{}{-9999999.99},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(-1234567), float64(-123456792), float64(-123456792)},
					[]interface{}{float64(-1.98), float64(-1.9873), float64(-1.98730468)},
					[]interface{}{float64(-1), float64(-1), float64(-1)},
					[]interface{}{float64(-0.76), float64(-0.7617), float64(-0.76171875)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// DECIMAL positive
	err = testExec(t, "truncate table "+tableName, nil)
	if err != nil {
		t.Error("truncate error:", err)
	}

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{0, 0, 0},
			[]interface{}{0.76, 0.7617, 0.76171875},
			[]interface{}{1, 1, 1},
			[]interface{}{1.98, 1.9873, 1.98730468},
			[]interface{}{12345679, 123456792, 123456792},
			[]interface{}{21474836, 2147483647, 2147483647},
			[]interface{}{9999999.99, 999999999999999.9999, 99999999999999999999999999.99999999},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(0), float64(0), float64(0)},
					[]interface{}{float64(0.76), float64(0.7617), float64(0.76171875)},
					[]interface{}{float64(1), float64(1), float64(1)},
					[]interface{}{float64(1.98), float64(1.9873), float64(1.98730468)},
					[]interface{}{float64(9999999.99), float64(999999999999999.9999), float64(99999999999999999999999999.99999999)},
					[]interface{}{float64(12345679), float64(123456792), float64(123456792)},
					[]interface{}{float64(21474836), float64(2147483647), float64(2147483647)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from "+tableName+" where A = :1",
		[][]interface{}{
			[]interface{}{0},
			[]interface{}{1},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(0.76), float64(0.7617), float64(0.76171875)},
					[]interface{}{float64(1.98), float64(1.9873), float64(1.98730468)},
					[]interface{}{float64(9999999.99), float64(999999999999999.9999), float64(99999999999999999999999999.99999999)},
					[]interface{}{float64(12345679), float64(123456792), float64(123456792)},
					[]interface{}{float64(21474836), float64(2147483647), float64(2147483647)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// NUMERIC negative
	tableName = "NUMERIC_" + TestTimeString
	err = testExec(t, "create table "+tableName+" ( A NUMERIC(10,2), B NUMERIC(20,4), C NUMERIC(38,8) )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer testDropTable(t, tableName)

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{-9999999.99, -999999999999999.9999, -9999999999999999999999999.99999999},
			[]interface{}{-21474836, -2147483648, -2147483648},
			[]interface{}{-1234567, -123456792, -123456792},
			[]interface{}{-1.98, -1.9873, -1.98730468},
			[]interface{}{-1, -1, -1},
			[]interface{}{-0.76, -0.7617, -0.76171875},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(-21474836), float64(-2147483648), float64(-2147483648)},
					[]interface{}{float64(-9999999.99), float64(-999999999999999.9999), float64(-9999999999999999999999999.99999999)},
					[]interface{}{float64(-1234567), float64(-123456792), float64(-123456792)},
					[]interface{}{float64(-1.98), float64(-1.9873), float64(-1.98730468)},
					[]interface{}{float64(-1), float64(-1), float64(-1)},
					[]interface{}{float64(-0.76), float64(-0.7617), float64(-0.76171875)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from "+tableName+" where A = :1",
		[][]interface{}{
			[]interface{}{-21474836},
			[]interface{}{-9999999.99},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(-1234567), float64(-123456792), float64(-123456792)},
					[]interface{}{float64(-1.98), float64(-1.9873), float64(-1.98730468)},
					[]interface{}{float64(-1), float64(-1), float64(-1)},
					[]interface{}{float64(-0.76), float64(-0.7617), float64(-0.76171875)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// NUMERIC positive
	err = testExec(t, "truncate table "+tableName, nil)
	if err != nil {
		t.Error("truncate error:", err)
	}

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{0, 0, 0},
			[]interface{}{0.76, 0.7617, 0.76171875},
			[]interface{}{1, 1, 1},
			[]interface{}{1.98, 1.9873, 1.98730468},
			[]interface{}{12345679, 123456792, 123456792},
			[]interface{}{21474836, 2147483647, 2147483647},
			[]interface{}{9999999.99, 999999999999999.9999, 99999999999999999999999999.99999999},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(0), float64(0), float64(0)},
					[]interface{}{float64(0.76), float64(0.7617), float64(0.76171875)},
					[]interface{}{float64(1), float64(1), float64(1)},
					[]interface{}{float64(1.98), float64(1.9873), float64(1.98730468)},
					[]interface{}{float64(9999999.99), float64(999999999999999.9999), float64(99999999999999999999999999.99999999)},
					[]interface{}{float64(12345679), float64(123456792), float64(123456792)},
					[]interface{}{float64(21474836), float64(2147483647), float64(2147483647)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from "+tableName+" where A = :1",
		[][]interface{}{
			[]interface{}{0},
			[]interface{}{1},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(0.76), float64(0.7617), float64(0.76171875)},
					[]interface{}{float64(1.98), float64(1.9873), float64(1.98730468)},
					[]interface{}{float64(9999999.99), float64(999999999999999.9999), float64(99999999999999999999999999.99999999)},
					[]interface{}{float64(12345679), float64(123456792), float64(123456792)},
					[]interface{}{float64(21474836), float64(2147483647), float64(2147483647)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// FLOAT negative
	tableName = "FLOAT_" + TestTimeString
	err = testExec(t, "create table "+tableName+
		" ( A FLOAT(28), B FLOAT(32), C FLOAT(38) )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer testDropTable(t, tableName)

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{-9999999.99, -999999999999999.9999, -9999999999999999999999999.99999999},
			[]interface{}{-21474836, -2147483648, -2147483648},
			[]interface{}{-1234567, -123456792, -123456792},
			[]interface{}{-1.98, -1.9873, -1.98730468},
			[]interface{}{-1, -1, -1},
			[]interface{}{-0.76, -0.7617, -0.76171875},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(-21474836), float64(-2147483648), float64(-2147483648)},
					[]interface{}{float64(-9999999.99), float64(-999999999999999.9999), float64(-9999999999999999999999999.99999999)},
					[]interface{}{float64(-1234567), float64(-123456792), float64(-123456792)},
					[]interface{}{float64(-1.98), float64(-1.9873), float64(-1.98730468)},
					[]interface{}{float64(-1), float64(-1), float64(-1)},
					[]interface{}{float64(-0.76), float64(-0.7617), float64(-0.76171875)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from "+tableName+" where A = :1",
		[][]interface{}{
			[]interface{}{-21474836},
			[]interface{}{-9999999.99},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(-1234567), float64(-123456792), float64(-123456792)},
					[]interface{}{float64(-1.98), float64(-1.9873), float64(-1.98730468)},
					[]interface{}{float64(-1), float64(-1), float64(-1)},
					[]interface{}{float64(-0.76), float64(-0.7617), float64(-0.76171875)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// FLOAT positive
	err = testExec(t, "truncate table "+tableName, nil)
	if err != nil {
		t.Error("truncate error:", err)
	}

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{0, 0, 0},
			[]interface{}{0.76, 0.7617, 0.76171875},
			[]interface{}{1, 1, 1},
			[]interface{}{1.98, 1.9873, 1.98730468},
			[]interface{}{12345679, 123456792, 123456792},
			[]interface{}{21474836, 2147483647, 2147483647},
			[]interface{}{9999999.99, 999999999999999.9999, 99999999999999999999999999.99999999},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(0), float64(0), float64(0)},
					[]interface{}{float64(0.76), float64(0.7617), float64(0.76171875)},
					[]interface{}{float64(1), float64(1), float64(1)},
					[]interface{}{float64(1.98), float64(1.9873), float64(1.98730468)},
					[]interface{}{float64(9999999.99), float64(999999999999999.9999), float64(99999999999999999999999999.99999999)},
					[]interface{}{float64(12345679), float64(123456792), float64(123456792)},
					[]interface{}{float64(21474836), float64(2147483647), float64(2147483647)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from "+tableName+" where A = :1",
		[][]interface{}{
			[]interface{}{0},
			[]interface{}{1},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(0.76), float64(0.7617), float64(0.76171875)},
					[]interface{}{float64(1.98), float64(1.9873), float64(1.98730468)},
					[]interface{}{float64(9999999.99), float64(999999999999999.9999), float64(99999999999999999999999999.99999999)},
					[]interface{}{float64(12345679), float64(123456792), float64(123456792)},
					[]interface{}{float64(21474836), float64(2147483647), float64(2147483647)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// INTEGER negative
	tableName = "INTEGER_" + TestTimeString
	err = testExec(t, "create table "+tableName+" ( A INTEGER, B INTEGER, C INTEGER )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer testDropTable(t, tableName)

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{-9999999.99, -999999999999999.9999, -999999999999999.99999999},
			[]interface{}{-21474836, -2147483648, -2147483648},
			[]interface{}{-1234567, -123456792, -123456792},
			[]interface{}{-1.98, -1.9873, -1.98730468},
			[]interface{}{-1, -1, -1},
			[]interface{}{-0.76, -0.7617, -0.76171875},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{int64(-21474836), int64(-2147483648), int64(-2147483648)},
					[]interface{}{int64(-10000000), int64(-1000000000000000), int64(-1000000000000000)},
					[]interface{}{int64(-1234567), int64(-123456792), int64(-123456792)},
					[]interface{}{int64(-2), int64(-2), int64(-2)},
					[]interface{}{int64(-1), int64(-1), int64(-1)},
					[]interface{}{int64(-1), int64(-1), int64(-1)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from "+tableName+" where A = :1",
		[][]interface{}{
			[]interface{}{-21474836},
			[]interface{}{-10000000},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{int64(-1234567), int64(-123456792), int64(-123456792)},
					[]interface{}{int64(-2), int64(-2), int64(-2)},
					[]interface{}{int64(-1), int64(-1), int64(-1)},
					[]interface{}{int64(-1), int64(-1), int64(-1)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// INTEGER positive
	err = testExec(t, "truncate table "+tableName, nil)
	if err != nil {
		t.Error("truncate error:", err)
	}

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{0, 0, 0},
			[]interface{}{0.76, 0.7617, 0.76171875},
			[]interface{}{1, 1, 1},
			[]interface{}{1.98, 1.9873, 1.98730468},
			[]interface{}{12345679, 123456792, 123456792},
			[]interface{}{21474836, 2147483647, 2147483647},
			[]interface{}{9999999.99, 999999999999999.9999, 999999999999999.99999999},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{int64(0), int64(0), int64(0)},
					[]interface{}{int64(1), int64(1), int64(1)},
					[]interface{}{int64(1), int64(1), int64(1)},
					[]interface{}{int64(2), int64(2), int64(2)},
					[]interface{}{int64(10000000), int64(1000000000000000), int64(1000000000000000)},
					[]interface{}{int64(12345679), int64(123456792), int64(123456792)},
					[]interface{}{int64(21474836), int64(2147483647), int64(2147483647)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from "+tableName+" where A = :1",
		[][]interface{}{
			[]interface{}{10000000},
			[]interface{}{12345679},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{int64(0), int64(0), int64(0)},
					[]interface{}{int64(1), int64(1), int64(1)},
					[]interface{}{int64(1), int64(1), int64(1)},
					[]interface{}{int64(2), int64(2), int64(2)},
					[]interface{}{int64(21474836), int64(2147483647), int64(2147483647)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// INT negative
	tableName = "INT_" + TestTimeString
	err = testExec(t, "create table "+tableName+" ( A INT, B INT, C INT )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer testDropTable(t, tableName)

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{-9999999.99, -999999999999999.9999, -999999999999999.99999999},
			[]interface{}{-21474836, -2147483648, -2147483648},
			[]interface{}{-1234567, -123456792, -123456792},
			[]interface{}{-1.98, -1.9873, -1.98730468},
			[]interface{}{-1, -1, -1},
			[]interface{}{-0.76, -0.7617, -0.76171875},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{int64(-21474836), int64(-2147483648), int64(-2147483648)},
					[]interface{}{int64(-10000000), int64(-1000000000000000), int64(-1000000000000000)},
					[]interface{}{int64(-1234567), int64(-123456792), int64(-123456792)},
					[]interface{}{int64(-2), int64(-2), int64(-2)},
					[]interface{}{int64(-1), int64(-1), int64(-1)},
					[]interface{}{int64(-1), int64(-1), int64(-1)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from "+tableName+" where A = :1",
		[][]interface{}{
			[]interface{}{-21474836},
			[]interface{}{-10000000},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{int64(-1234567), int64(-123456792), int64(-123456792)},
					[]interface{}{int64(-2), int64(-2), int64(-2)},
					[]interface{}{int64(-1), int64(-1), int64(-1)},
					[]interface{}{int64(-1), int64(-1), int64(-1)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// INT positive
	err = testExec(t, "truncate table "+tableName, nil)
	if err != nil {
		t.Error("truncate error:", err)
	}

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{0, 0, 0},
			[]interface{}{0.76, 0.7617, 0.76171875},
			[]interface{}{1, 1, 1},
			[]interface{}{1.98, 1.9873, 1.98730468},
			[]interface{}{12345679, 123456792, 123456792},
			[]interface{}{21474836, 2147483647, 2147483647},
			[]interface{}{9999999.99, 999999999999999.9999, 999999999999999.99999999},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{int64(0), int64(0), int64(0)},
					[]interface{}{int64(1), int64(1), int64(1)},
					[]interface{}{int64(1), int64(1), int64(1)},
					[]interface{}{int64(2), int64(2), int64(2)},
					[]interface{}{int64(10000000), int64(1000000000000000), int64(1000000000000000)},
					[]interface{}{int64(12345679), int64(123456792), int64(123456792)},
					[]interface{}{int64(21474836), int64(2147483647), int64(2147483647)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from "+tableName+" where A = :1",
		[][]interface{}{
			[]interface{}{10000000},
			[]interface{}{12345679},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{int64(0), int64(0), int64(0)},
					[]interface{}{int64(1), int64(1), int64(1)},
					[]interface{}{int64(1), int64(1), int64(1)},
					[]interface{}{int64(2), int64(2), int64(2)},
					[]interface{}{int64(21474836), int64(2147483647), int64(2147483647)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// SMALLINT negative
	tableName = "SMALLINT_" + TestTimeString
	err = testExec(t, "create table "+tableName+" ( A SMALLINT, B SMALLINT, C SMALLINT )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer testDropTable(t, tableName)

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{-9999999.99, -999999999999999.9999, -999999999999999.99999999},
			[]interface{}{-21474836, -2147483648, -2147483648},
			[]interface{}{-1234567, -123456792, -123456792},
			[]interface{}{-1.98, -1.9873, -1.98730468},
			[]interface{}{-1, -1, -1},
			[]interface{}{-0.76, -0.7617, -0.76171875},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{int64(-21474836), int64(-2147483648), int64(-2147483648)},
					[]interface{}{int64(-10000000), int64(-1000000000000000), int64(-1000000000000000)},
					[]interface{}{int64(-1234567), int64(-123456792), int64(-123456792)},
					[]interface{}{int64(-2), int64(-2), int64(-2)},
					[]interface{}{int64(-1), int64(-1), int64(-1)},
					[]interface{}{int64(-1), int64(-1), int64(-1)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from "+tableName+" where A = :1",
		[][]interface{}{
			[]interface{}{-21474836},
			[]interface{}{-10000000},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{int64(-1234567), int64(-123456792), int64(-123456792)},
					[]interface{}{int64(-2), int64(-2), int64(-2)},
					[]interface{}{int64(-1), int64(-1), int64(-1)},
					[]interface{}{int64(-1), int64(-1), int64(-1)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// SMALLINT positive
	err = testExec(t, "truncate table "+tableName, nil)
	if err != nil {
		t.Error("truncate error:", err)
	}

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{0, 0, 0},
			[]interface{}{0.76, 0.7617, 0.76171875},
			[]interface{}{1, 1, 1},
			[]interface{}{1.98, 1.9873, 1.98730468},
			[]interface{}{12345679, 123456792, 123456792},
			[]interface{}{21474836, 2147483647, 2147483647},
			[]interface{}{9999999.99, 999999999999999.9999, 999999999999999.99999999},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{int64(0), int64(0), int64(0)},
					[]interface{}{int64(1), int64(1), int64(1)},
					[]interface{}{int64(1), int64(1), int64(1)},
					[]interface{}{int64(2), int64(2), int64(2)},
					[]interface{}{int64(10000000), int64(1000000000000000), int64(1000000000000000)},
					[]interface{}{int64(12345679), int64(123456792), int64(123456792)},
					[]interface{}{int64(21474836), int64(2147483647), int64(2147483647)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from "+tableName+" where A = :1",
		[][]interface{}{
			[]interface{}{10000000},
			[]interface{}{12345679},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{int64(0), int64(0), int64(0)},
					[]interface{}{int64(1), int64(1), int64(1)},
					[]interface{}{int64(1), int64(1), int64(1)},
					[]interface{}{int64(2), int64(2), int64(2)},
					[]interface{}{int64(21474836), int64(2147483647), int64(2147483647)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// REAL negative
	tableName = "REAL_" + TestTimeString
	err = testExec(t, "create table "+tableName+
		" ( A REAL, B REAL, C REAL )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer testDropTable(t, tableName)

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{-9999999.99, -999999999999999.9999, -9999999999999999999999999.99999999},
			[]interface{}{-21474836, -2147483648, -2147483648},
			[]interface{}{-1234567, -123456792, -123456792},
			[]interface{}{-1.98, -1.9873, -1.98730468},
			[]interface{}{-1, -1, -1},
			[]interface{}{-0.76, -0.7617, -0.76171875},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(-21474836), float64(-2147483648), float64(-2147483648)},
					[]interface{}{float64(-9999999.99), float64(-999999999999999.9999), float64(-9999999999999999999999999.99999999)},
					[]interface{}{float64(-1234567), float64(-123456792), float64(-123456792)},
					[]interface{}{float64(-1.98), float64(-1.9873), float64(-1.98730468)},
					[]interface{}{float64(-1), float64(-1), float64(-1)},
					[]interface{}{float64(-0.76), float64(-0.7617), float64(-0.76171875)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from "+tableName+" where A = :1",
		[][]interface{}{
			[]interface{}{-21474836},
			[]interface{}{-9999999.99},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(-1234567), float64(-123456792), float64(-123456792)},
					[]interface{}{float64(-1.98), float64(-1.9873), float64(-1.98730468)},
					[]interface{}{float64(-1), float64(-1), float64(-1)},
					[]interface{}{float64(-0.76), float64(-0.7617), float64(-0.76171875)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// REAL positive
	err = testExec(t, "truncate table "+tableName, nil)
	if err != nil {
		t.Error("truncate error:", err)
	}

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{0, 0, 0},
			[]interface{}{0.76, 0.7617, 0.76171875},
			[]interface{}{1, 1, 1},
			[]interface{}{1.98, 1.9873, 1.98730468},
			[]interface{}{12345679, 123456792, 123456792},
			[]interface{}{21474836, 2147483647, 2147483647},
			[]interface{}{9999999.99, 999999999999999.9999, 99999999999999999999999999.99999999},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(0), float64(0), float64(0)},
					[]interface{}{float64(0.76), float64(0.7617), float64(0.76171875)},
					[]interface{}{float64(1), float64(1), float64(1)},
					[]interface{}{float64(1.98), float64(1.9873), float64(1.98730468)},
					[]interface{}{float64(9999999.99), float64(999999999999999.9999), float64(99999999999999999999999999.99999999)},
					[]interface{}{float64(12345679), float64(123456792), float64(123456792)},
					[]interface{}{float64(21474836), float64(2147483647), float64(2147483647)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from "+tableName+" where A = :1",
		[][]interface{}{
			[]interface{}{0},
			[]interface{}{1},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(0.76), float64(0.7617), float64(0.76171875)},
					[]interface{}{float64(1.98), float64(1.9873), float64(1.98730468)},
					[]interface{}{float64(9999999.99), float64(999999999999999.9999), float64(99999999999999999999999999.99999999)},
					[]interface{}{float64(12345679), float64(123456792), float64(123456792)},
					[]interface{}{float64(21474836), float64(2147483647), float64(2147483647)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// BINARY_FLOAT negative
	tableName = "BINARY_FLOAT_" + TestTimeString
	err = testExec(t, "create table "+tableName+
		" ( A BINARY_FLOAT, B BINARY_FLOAT, C BINARY_FLOAT )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer testDropTable(t, tableName)

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{float64(-288230381928101358902502915674136903680), float64(-288230381928101358902502915674136903680), float64(-288230381928101358902502915674136903680)},
			[]interface{}{-2147483648, -2147483648, -2147483648},
			[]interface{}{-123456792, -123456792, -123456792},
			[]interface{}{-1.99999988079071044921875, -1.99999988079071044921875, -1.99999988079071044921875},
			[]interface{}{-1, -1, -1},
			[]interface{}{-0.00415134616196155548095703125, -0.00415134616196155548095703125, -0.00415134616196155548095703125},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(-288230381928101358902502915674136903680), float64(-288230381928101358902502915674136903680), float64(-288230381928101358902502915674136903680)},
					[]interface{}{float64(-2147483648), float64(-2147483648), float64(-2147483648)},
					[]interface{}{float64(-123456792), float64(-123456792), float64(-123456792)},
					[]interface{}{float64(-1.99999988079071044921875), float64(-1.99999988079071044921875), float64(-1.99999988079071044921875)},
					[]interface{}{float64(-1), float64(-1), float64(-1)},
					[]interface{}{float64(-0.00415134616196155548095703125), float64(-0.00415134616196155548095703125), float64(-0.00415134616196155548095703125)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from "+tableName+" where A = :1",
		[][]interface{}{
			[]interface{}{-2147483648},
			[]interface{}{float64(-288230381928101358902502915674136903680)},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(-123456792), float64(-123456792), float64(-123456792)},
					[]interface{}{float64(-1.99999988079071044921875), float64(-1.99999988079071044921875), float64(-1.99999988079071044921875)},
					[]interface{}{float64(-1), float64(-1), float64(-1)},
					[]interface{}{float64(-0.00415134616196155548095703125), float64(-0.00415134616196155548095703125), float64(-0.00415134616196155548095703125)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// BINARY_FLOAT positive
	err = testExec(t, "truncate table "+tableName, nil)
	if err != nil {
		t.Error("truncate error:", err)
	}

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{0, 0, 0},
			[]interface{}{0.00415134616196155548095703125, 0.00415134616196155548095703125, 0.00415134616196155548095703125},
			[]interface{}{1, 1, 1},
			[]interface{}{1.99999988079071044921875, 1.99999988079071044921875, 1.99999988079071044921875},
			[]interface{}{123456792, 123456792, 123456792},
			[]interface{}{2147483648, 2147483648, 2147483648},
			[]interface{}{float64(288230381928101358902502915674136903680), float64(288230381928101358902502915674136903680), float64(288230381928101358902502915674136903680)},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(0), float64(0), float64(0)},
					[]interface{}{float64(0.00415134616196155548095703125), float64(0.00415134616196155548095703125), float64(0.00415134616196155548095703125)},
					[]interface{}{float64(1), float64(1), float64(1)},
					[]interface{}{float64(1.99999988079071044921875), float64(1.99999988079071044921875), float64(1.99999988079071044921875)},
					[]interface{}{float64(123456792), float64(123456792), float64(123456792)},
					[]interface{}{float64(2147483648), float64(2147483648), float64(2147483648)},
					[]interface{}{float64(288230381928101358902502915674136903680), float64(288230381928101358902502915674136903680), float64(288230381928101358902502915674136903680)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from "+tableName+" where A = :1",
		[][]interface{}{
			[]interface{}{0},
			[]interface{}{1},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(0.00415134616196155548095703125), float64(0.00415134616196155548095703125), float64(0.00415134616196155548095703125)},
					[]interface{}{float64(1.99999988079071044921875), float64(1.99999988079071044921875), float64(1.99999988079071044921875)},
					[]interface{}{float64(123456792), float64(123456792), float64(123456792)},
					[]interface{}{float64(2147483648), float64(2147483648), float64(2147483648)},
					[]interface{}{float64(288230381928101358902502915674136903680), float64(288230381928101358902502915674136903680), float64(288230381928101358902502915674136903680)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// BINARY_DOUBLE negative
	tableName = "BINARY_DOUBLE_" + TestTimeString
	err = testExec(t, "create table "+tableName+" ( A BINARY_DOUBLE, B BINARY_DOUBLE, C BINARY_DOUBLE )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer testDropTable(t, tableName)

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{-9999999.99, -999999999999999.9999, -9999999999999999999999999.99999999},
			[]interface{}{-21474836, -2147483648, -2147483648},
			[]interface{}{-1234567, -123456792, -123456792},
			[]interface{}{-1.98, -1.9873, -1.98730468},
			[]interface{}{-1, -1, -1},
			[]interface{}{-0.76, -0.7617, -0.76171875},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(-21474836), float64(-2147483648), float64(-2147483648)},
					[]interface{}{float64(-9999999.99), float64(-999999999999999.9999), float64(-9999999999999999999999999.99999999)},
					[]interface{}{float64(-1234567), float64(-123456792), float64(-123456792)},
					[]interface{}{float64(-1.98), float64(-1.9873), float64(-1.98730468)},
					[]interface{}{float64(-1), float64(-1), float64(-1)},
					[]interface{}{float64(-0.76), float64(-0.7617), float64(-0.76171875)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from "+tableName+" where A = :1",
		[][]interface{}{
			[]interface{}{-21474836},
			[]interface{}{-9999999.99},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(-1234567), float64(-123456792), float64(-123456792)},
					[]interface{}{float64(-1.98), float64(-1.9873), float64(-1.98730468)},
					[]interface{}{float64(-1), float64(-1), float64(-1)},
					[]interface{}{float64(-0.76), float64(-0.7617), float64(-0.76171875)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// BINARY_DOUBLE positive
	err = testExec(t, "truncate table "+tableName, nil)
	if err != nil {
		t.Error("truncate error:", err)
	}

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{0, 0, 0},
			[]interface{}{0.76, 0.7617, 0.76171875},
			[]interface{}{1, 1, 1},
			[]interface{}{1.98, 1.9873, 1.98730468},
			[]interface{}{12345679, 123456792, 123456792},
			[]interface{}{21474836, 2147483647, 2147483647},
			[]interface{}{9999999.99, 999999999999999.9999, 99999999999999999999999999.99999999},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(0), float64(0), float64(0)},
					[]interface{}{float64(0.76), float64(0.7617), float64(0.76171875)},
					[]interface{}{float64(1), float64(1), float64(1)},
					[]interface{}{float64(1.98), float64(1.9873), float64(1.98730468)},
					[]interface{}{float64(9999999.99), float64(999999999999999.9999), float64(99999999999999999999999999.99999999)},
					[]interface{}{float64(12345679), float64(123456792), float64(123456792)},
					[]interface{}{float64(21474836), float64(2147483647), float64(2147483647)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExecRows(t, "delete from "+tableName+" where A = :1",
		[][]interface{}{
			[]interface{}{0},
			[]interface{}{1},
		})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{float64(0.76), float64(0.7617), float64(0.76171875)},
					[]interface{}{float64(1.98), float64(1.9873), float64(1.98730468)},
					[]interface{}{float64(9999999.99), float64(999999999999999.9999), float64(99999999999999999999999999.99999999)},
					[]interface{}{float64(12345679), float64(123456792), float64(123456792)},
					[]interface{}{float64(21474836), float64(2147483647), float64(2147483647)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)
}

func TestFunctionCallNumber(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}

	// https://ss64.com/ora/syntax-datatypes.html

	var execResults testExecResults

	execResultInt64 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"num1": sql.Out{Dest: int64(-32768), In: true}},
			results: map[string]interface{}{"num1": int64(-32767)},
		},
		testExecResult{
			args:    map[string]sql.Out{"num1": sql.Out{Dest: int64(-128), In: true}},
			results: map[string]interface{}{"num1": int64(-127)},
		},
		testExecResult{
			args:    map[string]sql.Out{"num1": sql.Out{Dest: int64(-1), In: true}},
			results: map[string]interface{}{"num1": int64(0)},
		},
		testExecResult{
			args:    map[string]sql.Out{"num1": sql.Out{Dest: int64(0), In: true}},
			results: map[string]interface{}{"num1": int64(1)},
		},
		testExecResult{
			args:    map[string]sql.Out{"num1": sql.Out{Dest: int64(1), In: true}},
			results: map[string]interface{}{"num1": int64(2)},
		},
		testExecResult{
			args:    map[string]sql.Out{"num1": sql.Out{Dest: int64(127), In: true}},
			results: map[string]interface{}{"num1": int64(128)},
		},
		testExecResult{
			args:    map[string]sql.Out{"num1": sql.Out{Dest: int64(32767), In: true}},
			results: map[string]interface{}{"num1": int64(32768)},
		},
	}

	execResultInt64Medium := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"num1": sql.Out{Dest: int64(-2147483648), In: true}},
			results: map[string]interface{}{"num1": int64(-2147483647)},
		},
		testExecResult{
			args:    map[string]sql.Out{"num1": sql.Out{Dest: int64(2147483646), In: true}},
			results: map[string]interface{}{"num1": int64(2147483647)},
		},
	}

	execResultInt64Big := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"num1": sql.Out{Dest: int64(-9223372036854775808), In: true}},
			results: map[string]interface{}{"num1": int64(-9223372036854775807)},
		},
		testExecResult{
			args:    map[string]sql.Out{"num1": sql.Out{Dest: int64(-2147483649), In: true}},
			results: map[string]interface{}{"num1": int64(-2147483648)},
		},
		testExecResult{
			args:    map[string]sql.Out{"num1": sql.Out{Dest: int64(2147483647), In: true}},
			results: map[string]interface{}{"num1": int64(2147483648)},
		},
		testExecResult{
			args:    map[string]sql.Out{"num1": sql.Out{Dest: int64(9223372036854775806), In: true}},
			results: map[string]interface{}{"num1": int64(9223372036854775807)},
		},
	}

	execResultFloat64Int := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"num1": sql.Out{Dest: float64(0), In: true}},
			results: map[string]interface{}{"num1": float64(1)},
		},
		testExecResult{
			args:    map[string]sql.Out{"num1": sql.Out{Dest: float64(1), In: true}},
			results: map[string]interface{}{"num1": float64(2)},
		},
	}

	execResultFloat64IntMedium := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"num1": sql.Out{Dest: float64(-2147483648), In: true}},
			results: map[string]interface{}{"num1": float64(-2147483647)},
		},
		testExecResult{
			args:    map[string]sql.Out{"num1": sql.Out{Dest: float64(-123456792), In: true}},
			results: map[string]interface{}{"num1": float64(-123456791)},
		},
		testExecResult{
			args:    map[string]sql.Out{"num1": sql.Out{Dest: float64(123456792), In: true}},
			results: map[string]interface{}{"num1": float64(123456793)},
		},
		testExecResult{
			args:    map[string]sql.Out{"num1": sql.Out{Dest: float64(2147483646), In: true}},
			results: map[string]interface{}{"num1": float64(2147483647)},
		},
	}

	execResultFloat64IntBig := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"num1": sql.Out{Dest: float64(-288230381928101358902502915674136903680), In: true}},
			results: map[string]interface{}{"num1": float64(-288230381928101358902502915674136903679)},
		},
		testExecResult{
			args:    map[string]sql.Out{"num1": sql.Out{Dest: float64(288230381928101358902502915674136903680), In: true}},
			results: map[string]interface{}{"num1": float64(288230381928101358902502915674136903681)},
		},
	}

	execResultFloat64 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"num1": sql.Out{Dest: float64(-1.9990234375), In: true}},
			results: map[string]interface{}{"num1": float64(-0.9990234375)},
		},
		testExecResult{
			args:    map[string]sql.Out{"num1": sql.Out{Dest: float64(-0.0068359375), In: true}},
			results: map[string]interface{}{"num1": float64(0.9931640625)},
		},
		testExecResult{
			args:    map[string]sql.Out{"num1": sql.Out{Dest: float64(0.0048828125), In: true}},
			results: map[string]interface{}{"num1": float64(1.0048828125)},
		},
		testExecResult{
			args:    map[string]sql.Out{"num1": sql.Out{Dest: float64(1.9990234375), In: true}},
			results: map[string]interface{}{"num1": float64(2.9990234375)},
		},
	}

	execResultFloat64Big := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"num1": sql.Out{Dest: float64(-1.99999988079071044921875), In: true}},
			results: map[string]interface{}{"num1": float64(-0.99999988079071044921875)},
		},
		testExecResult{
			args:    map[string]sql.Out{"num1": sql.Out{Dest: float64(-0.00415134616196155548095703125), In: true}},
			results: map[string]interface{}{"num1": float64(0.99584865383803844451904296875)},
		},
		testExecResult{
			args:    map[string]sql.Out{"num1": sql.Out{Dest: float64(0.00415134616196155548095703125), In: true}},
			results: map[string]interface{}{"num1": float64(1.00415134616196155548095703125)},
		},
		testExecResult{
			args:    map[string]sql.Out{"num1": sql.Out{Dest: float64(1.99999988079071044921875), In: true}},
			results: map[string]interface{}{"num1": float64(2.99999988079071044921875)},
		},
	}

	// NUMBER
	execResults.query = `
declare
	function GET_NUMBER(p_number NUMBER) return NUMBER as
	begin
		return p_number + 1;
	end GET_NUMBER;
begin
	:num1 := GET_NUMBER(:num1);
end;`
	execResults.execResults = execResultInt64
	testRunExecResults(t, execResults)
	execResults.execResults = execResultInt64Medium
	testRunExecResults(t, execResults)
	execResults.execResults = execResultInt64Big
	testRunExecResults(t, execResults)
	execResults.execResults = execResultFloat64Int
	testRunExecResults(t, execResults)
	execResults.execResults = execResultFloat64IntMedium
	testRunExecResults(t, execResults)
	execResults.execResults = execResultFloat64IntBig
	testRunExecResults(t, execResults)
	execResults.execResults = execResultFloat64
	testRunExecResults(t, execResults)
	execResults.execResults = execResultFloat64Big
	testRunExecResults(t, execResults)

	// DEC
	execResults.query = `
declare
	function GET_NUMBER(p_number DEC) return DEC as
	begin
		return p_number + 1;
	end GET_NUMBER;
begin
	:num1 := GET_NUMBER(:num1);
end;`
	execResults.execResults = execResultInt64
	testRunExecResults(t, execResults)
	execResults.execResults = execResultInt64Medium
	testRunExecResults(t, execResults)
	execResults.execResults = execResultInt64Big
	testRunExecResults(t, execResults)
	execResults.execResults = execResultFloat64Int
	testRunExecResults(t, execResults)
	execResults.execResults = execResultFloat64IntMedium
	testRunExecResults(t, execResults)

	// DECIMAL
	execResults.query = `
declare
	function GET_NUMBER(p_number DECIMAL) return DECIMAL as
	begin
		return p_number + 1;
	end GET_NUMBER;
begin
	:num1 := GET_NUMBER(:num1);
end;`
	execResults.execResults = execResultInt64
	testRunExecResults(t, execResults)
	execResults.execResults = execResultInt64Medium
	testRunExecResults(t, execResults)
	execResults.execResults = execResultInt64Big
	testRunExecResults(t, execResults)
	execResults.execResults = execResultFloat64Int
	testRunExecResults(t, execResults)
	execResults.execResults = execResultFloat64IntMedium
	testRunExecResults(t, execResults)

	// NUMERIC
	execResults.query = `
declare
	function GET_NUMBER(p_number NUMERIC) return NUMERIC as
	begin
		return p_number + 1;
	end GET_NUMBER;
begin
	:num1 := GET_NUMBER(:num1);
end;`
	execResults.execResults = execResultInt64
	testRunExecResults(t, execResults)
	execResults.execResults = execResultInt64Medium
	testRunExecResults(t, execResults)
	execResults.execResults = execResultInt64Big
	testRunExecResults(t, execResults)
	execResults.execResults = execResultFloat64Int
	testRunExecResults(t, execResults)
	execResults.execResults = execResultFloat64IntMedium
	testRunExecResults(t, execResults)

	// FLOAT
	execResults.query = `
declare
	function GET_NUMBER(p_number FLOAT) return FLOAT as
	begin
		return p_number + 1;
	end GET_NUMBER;
begin
	:num1 := GET_NUMBER(:num1);
end;`
	execResults.execResults = execResultInt64
	testRunExecResults(t, execResults)
	execResults.execResults = execResultInt64Medium
	testRunExecResults(t, execResults)
	execResults.execResults = execResultInt64Big
	testRunExecResults(t, execResults)
	execResults.execResults = execResultFloat64Int
	testRunExecResults(t, execResults)
	execResults.execResults = execResultFloat64IntMedium
	testRunExecResults(t, execResults)
	execResults.execResults = execResultFloat64
	testRunExecResults(t, execResults)

	// INTEGER
	execResults.query = `
declare
	function GET_NUMBER(p_number INTEGER) return INTEGER as
	begin
		return p_number + 1;
	end GET_NUMBER;
begin
	:num1 := GET_NUMBER(:num1);
end;`
	execResults.execResults = execResultInt64
	testRunExecResults(t, execResults)
	execResults.execResults = execResultInt64Medium
	testRunExecResults(t, execResults)
	execResults.execResults = execResultInt64Big
	testRunExecResults(t, execResults)
	execResults.execResults = execResultFloat64Int
	testRunExecResults(t, execResults)
	execResults.execResults = execResultFloat64IntMedium
	testRunExecResults(t, execResults)

	// INT
	execResults.query = `
declare
	function GET_NUMBER(p_number INT) return INT as
	begin
		return p_number + 1;
	end GET_NUMBER;
begin
	:num1 := GET_NUMBER(:num1);
end;`
	execResults.execResults = execResultInt64
	testRunExecResults(t, execResults)
	execResults.execResults = execResultInt64Medium
	testRunExecResults(t, execResults)
	execResults.execResults = execResultInt64Big
	testRunExecResults(t, execResults)
	execResults.execResults = execResultFloat64Int
	testRunExecResults(t, execResults)
	execResults.execResults = execResultFloat64IntMedium
	testRunExecResults(t, execResults)

	// SMALLINT
	execResults.query = `
declare
	function GET_NUMBER(p_number SMALLINT) return SMALLINT as
	begin
		return p_number + 1;
	end GET_NUMBER;
begin
	:num1 := GET_NUMBER(:num1);
end;`
	execResults.execResults = execResultInt64
	testRunExecResults(t, execResults)
	execResults.execResults = execResultInt64Medium
	testRunExecResults(t, execResults)
	execResults.execResults = execResultInt64Big
	testRunExecResults(t, execResults)
	execResults.execResults = execResultFloat64Int
	testRunExecResults(t, execResults)
	execResults.execResults = execResultFloat64IntMedium
	testRunExecResults(t, execResults)

	// REAL
	execResults.query = `
declare
	function GET_NUMBER(p_number REAL) return REAL as
	begin
		return p_number + 1;
	end GET_NUMBER;
begin
	:num1 := GET_NUMBER(:num1);
end;`
	execResults.execResults = execResultInt64
	testRunExecResults(t, execResults)
	execResults.execResults = execResultInt64Medium
	testRunExecResults(t, execResults)
	execResults.execResults = execResultInt64Big
	testRunExecResults(t, execResults)
	execResults.execResults = execResultFloat64Int
	testRunExecResults(t, execResults)
	execResults.execResults = execResultFloat64IntMedium
	testRunExecResults(t, execResults)
	execResults.execResults = execResultFloat64
	testRunExecResults(t, execResults)

	// BINARY_FLOAT
	execResults.query = `
declare
	function GET_NUMBER(p_number BINARY_FLOAT) return BINARY_FLOAT as
	begin
		return p_number + 1;
	end GET_NUMBER;
begin
	:num1 := GET_NUMBER(:num1);
end;`
	execResults.execResults = execResultInt64
	testRunExecResults(t, execResults)
	execResults.execResults = execResultFloat64Int
	testRunExecResults(t, execResults)

	// BINARY_DOUBLE
	execResults.query = `
declare
	function GET_NUMBER(p_number BINARY_DOUBLE) return BINARY_DOUBLE as
	begin
		return p_number + 1;
	end GET_NUMBER;
begin
	:num1 := GET_NUMBER(:num1);
end;`
	execResults.execResults = execResultInt64
	testRunExecResults(t, execResults)
	execResults.execResults = execResultInt64Medium
	testRunExecResults(t, execResults)
	execResults.execResults = execResultFloat64Int
	testRunExecResults(t, execResults)
	execResults.execResults = execResultFloat64IntMedium
	testRunExecResults(t, execResults)
	execResults.execResults = execResultFloat64
	testRunExecResults(t, execResults)

	// PLS_INTEGER
	execResults.query = `
declare
	function GET_NUMBER(p_number PLS_INTEGER) return PLS_INTEGER as
	begin
		return p_number + 1;
	end GET_NUMBER;
begin
	:num1 := GET_NUMBER(:num1);
end;`
	execResults.execResults = execResultInt64
	testRunExecResults(t, execResults)
	execResults.execResults = execResultInt64Medium
	testRunExecResults(t, execResults)
	execResults.execResults = execResultFloat64Int
	testRunExecResults(t, execResults)
	execResults.execResults = execResultFloat64IntMedium
	testRunExecResults(t, execResults)
}
