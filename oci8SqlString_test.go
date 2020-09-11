package oci8

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

// TestSelectDualNullString checks nulls
func TestSelectDualNullString(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}

	// VARCHAR2(1)
	queryResults := testQueryResults{
		query:        "select cast (null as VARCHAR2(1)) from dual",
		queryResults: []testQueryResult{testQueryResult{results: [][]interface{}{[]interface{}{nil}}}},
	}
	testRunQueryResults(t, queryResults)

	// VARCHAR2(4000)
	queryResults = testQueryResults{
		query:        "select cast (null as VARCHAR2(4000)) from dual",
		queryResults: []testQueryResult{testQueryResult{results: [][]interface{}{[]interface{}{nil}}}},
	}
	testRunQueryResults(t, queryResults)

	// NVARCHAR2(1)
	queryResults = testQueryResults{
		query:        "select cast (null as NVARCHAR2(1)) from dual",
		queryResults: []testQueryResult{testQueryResult{results: [][]interface{}{[]interface{}{nil}}}},
	}
	testRunQueryResults(t, queryResults)

	// NVARCHAR2(2000)
	queryResults = testQueryResults{
		query:        "select cast (null as NVARCHAR2(2000)) from dual",
		queryResults: []testQueryResult{testQueryResult{results: [][]interface{}{[]interface{}{nil}}}},
	}
	testRunQueryResults(t, queryResults)

	// CHAR(1)
	queryResults = testQueryResults{
		query:        "select cast (null as CHAR(1)) from dual",
		queryResults: []testQueryResult{testQueryResult{results: [][]interface{}{[]interface{}{nil}}}},
	}
	testRunQueryResults(t, queryResults)

	// CHAR(2000)
	queryResults = testQueryResults{
		query:        "select cast (null as CHAR(2000)) from dual",
		queryResults: []testQueryResult{testQueryResult{results: [][]interface{}{[]interface{}{nil}}}},
	}
	testRunQueryResults(t, queryResults)

	// NCHAR(1)
	queryResults = testQueryResults{
		query:        "select cast (null as NCHAR(1)) from dual",
		queryResults: []testQueryResult{testQueryResult{results: [][]interface{}{[]interface{}{nil}}}},
	}
	testRunQueryResults(t, queryResults)

	// NCHAR(1000)
	queryResults = testQueryResults{
		query:        "select cast (null as NCHAR(1000)) from dual",
		queryResults: []testQueryResult{testQueryResult{results: [][]interface{}{[]interface{}{nil}}}},
	}
	testRunQueryResults(t, queryResults)

	// CLOB
	queryResults = testQueryResults{
		query:        "select to_clob(null) from dual",
		queryResults: []testQueryResult{testQueryResult{results: [][]interface{}{[]interface{}{nil}}}},
	}
	testRunQueryResults(t, queryResults)

	// NCLOB
	queryResults = testQueryResults{
		query:        "select to_nclob(null) from dual",
		queryResults: []testQueryResult{testQueryResult{results: [][]interface{}{[]interface{}{nil}}}},
	}
	testRunQueryResults(t, queryResults)

	// RAW(1)
	queryResults = testQueryResults{
		query:        "select cast (null as RAW(1)) from dual",
		queryResults: []testQueryResult{testQueryResult{results: [][]interface{}{[]interface{}{nil}}}},
	}
	testRunQueryResults(t, queryResults)

	// RAW(2000)
	queryResults = testQueryResults{
		query:        "select cast (null as RAW(2000)) from dual",
		queryResults: []testQueryResult{testQueryResult{results: [][]interface{}{[]interface{}{nil}}}},
	}
	testRunQueryResults(t, queryResults)

	// BLOB
	queryResults = testQueryResults{
		query:        "select to_blob(null) from dual",
		queryResults: []testQueryResult{testQueryResult{results: [][]interface{}{[]interface{}{nil}}}},
	}
	testRunQueryResults(t, queryResults)
}

// TestSelectDualString checks select dual for string types
func TestSelectDualString(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}

	queryResults := testQueryResults{}

	// test strings no change

	queryResultStrings1 := []testQueryResult{
		testQueryResult{
			args:    []interface{}{""},
			results: [][]interface{}{[]interface{}{nil}},
		},
		testQueryResult{
			args:    []interface{}{"a"},
			results: [][]interface{}{[]interface{}{"a"}},
		},
		testQueryResult{
			args:    []interface{}{"\x00"},
			results: [][]interface{}{[]interface{}{"\x00"}},
		},
	}

	queryResultStrings2000 := []testQueryResult{
		testQueryResult{
			args:    []interface{}{"abc    "},
			results: [][]interface{}{[]interface{}{"abc    "}},
		},
		testQueryResult{
			args:    []interface{}{"    abc"},
			results: [][]interface{}{[]interface{}{"    abc"}},
		},
		testQueryResult{
			args:    []interface{}{"    abc    "},
			results: [][]interface{}{[]interface{}{"    abc    "}},
		},
		testQueryResult{
			args:    []interface{}{"123"},
			results: [][]interface{}{[]interface{}{"123"}},
		},
		testQueryResult{
			args:    []interface{}{"123.456"},
			results: [][]interface{}{[]interface{}{"123.456"}},
		},
		testQueryResult{
			args:    []interface{}{"abcdefghijklmnopqrstuvwxyz"},
			results: [][]interface{}{[]interface{}{"abcdefghijklmnopqrstuvwxyz"}},
		},
		testQueryResult{
			args:    []interface{}{" a b c d e f g h i j k l m n o p q r s t u v w x y z "},
			results: [][]interface{}{[]interface{}{" a b c d e f g h i j k l m n o p q r s t u v w x y z "}},
		},
		testQueryResult{
			args:    []interface{}{"ab\ncd\nef"},
			results: [][]interface{}{[]interface{}{"ab\ncd\nef"}},
		},
		testQueryResult{
			args:    []interface{}{"ab\tcd\tef"},
			results: [][]interface{}{[]interface{}{"ab\tcd\tef"}},
		},
		testQueryResult{
			args:    []interface{}{"ab\x00cd\x00ef"},
			results: [][]interface{}{[]interface{}{"ab\x00cd\x00ef"}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 100)},
			results: [][]interface{}{[]interface{}{strings.Repeat("a", 100)}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 1000)},
			results: [][]interface{}{[]interface{}{strings.Repeat("a", 1000)}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("ab", 1000)},
			results: [][]interface{}{[]interface{}{strings.Repeat("ab", 1000)}},
		},
		testQueryResult{
			args:    []interface{}{"こんにちは 世界 Καλημέρα κόσμε こんにちは안녕하세요góðan dagGrüßgotthyvää päivääyá'át'ééhΓεια σαςВiтаюგამარჯობაनमस्ते你好здравейсвят"},
			results: [][]interface{}{[]interface{}{"こんにちは 世界 Καλημέρα κόσμε こんにちは안녕하세요góðan dagGrüßgotthyvää päivääyá'át'ééhΓεια σαςВiтаюგამარჯობაनमस्ते你好здравейсвят"}},
		},
		testQueryResult{
			args:    []interface{}{"здравейсвят"},
			results: [][]interface{}{[]interface{}{"здравейсвят"}},
		},
		testQueryResult{
			args:    []interface{}{"кодировка"},
			results: [][]interface{}{[]interface{}{"кодировка"}},
		},
	}

	queryResultStrings4000 := []testQueryResult{
		testQueryResult{
			args:    []interface{}{strings.Repeat("abcd", 1000)},
			results: [][]interface{}{[]interface{}{strings.Repeat("abcd", 1000)}},
		},
		testQueryResult{
			args:    []interface{}{testString1},
			results: [][]interface{}{[]interface{}{testString1}},
		},
	}

	queryResultStringsRpad8000 := []testQueryResult{
		testQueryResult{
			results: [][]interface{}{[]interface{}{strings.Repeat("a", 4000) + strings.Repeat("b", 4000)}},
		},
	}

	queryResultStringsFix1000 := []testQueryResult{
		testQueryResult{
			args:    []interface{}{""},
			results: [][]interface{}{[]interface{}{nil}},
		},
		testQueryResult{
			args:    []interface{}{"a"},
			results: [][]interface{}{[]interface{}{"a" + strings.Repeat(" ", 999)}},
		},
		testQueryResult{
			args:    []interface{}{"abc    "},
			results: [][]interface{}{[]interface{}{"abc    " + strings.Repeat(" ", 993)}},
		},
		testQueryResult{
			args:    []interface{}{"    abc"},
			results: [][]interface{}{[]interface{}{"    abc" + strings.Repeat(" ", 993)}},
		},
		testQueryResult{
			args:    []interface{}{"    abc    "},
			results: [][]interface{}{[]interface{}{"    abc    " + strings.Repeat(" ", 989)}},
		},
		testQueryResult{
			args:    []interface{}{"ab\ncd\nef"},
			results: [][]interface{}{[]interface{}{"ab\ncd\nef" + strings.Repeat(" ", 992)}},
		},
		testQueryResult{
			args:    []interface{}{"ab\tcd\tef"},
			results: [][]interface{}{[]interface{}{"ab\tcd\tef" + strings.Repeat(" ", 992)}},
		},
		testQueryResult{
			args:    []interface{}{"ab\x00cd\x00ef"},
			results: [][]interface{}{[]interface{}{"ab\x00cd\x00ef" + strings.Repeat(" ", 992)}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 10)},
			results: [][]interface{}{[]interface{}{strings.Repeat("a", 10) + strings.Repeat(" ", 990)}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 100)},
			results: [][]interface{}{[]interface{}{strings.Repeat("a", 100) + strings.Repeat(" ", 900)}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 1000)},
			results: [][]interface{}{[]interface{}{strings.Repeat("a", 1000)}},
		},
	}

	queryResultStringsFix2000 := []testQueryResult{
		testQueryResult{
			args:    []interface{}{""},
			results: [][]interface{}{[]interface{}{nil}},
		},
		testQueryResult{
			args:    []interface{}{"a"},
			results: [][]interface{}{[]interface{}{"a" + strings.Repeat(" ", 1999)}},
		},
		testQueryResult{
			args:    []interface{}{"abc    "},
			results: [][]interface{}{[]interface{}{"abc    " + strings.Repeat(" ", 1993)}},
		},
		testQueryResult{
			args:    []interface{}{"    abc"},
			results: [][]interface{}{[]interface{}{"    abc" + strings.Repeat(" ", 1993)}},
		},
		testQueryResult{
			args:    []interface{}{"    abc    "},
			results: [][]interface{}{[]interface{}{"    abc    " + strings.Repeat(" ", 1989)}},
		},
		testQueryResult{
			args:    []interface{}{"ab\ncd\nef"},
			results: [][]interface{}{[]interface{}{"ab\ncd\nef" + strings.Repeat(" ", 1992)}},
		},
		testQueryResult{
			args:    []interface{}{"ab\tcd\tef"},
			results: [][]interface{}{[]interface{}{"ab\tcd\tef" + strings.Repeat(" ", 1992)}},
		},
		testQueryResult{
			args:    []interface{}{"ab\x00cd\x00ef"},
			results: [][]interface{}{[]interface{}{"ab\x00cd\x00ef" + strings.Repeat(" ", 1992)}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 10)},
			results: [][]interface{}{[]interface{}{strings.Repeat("a", 10) + strings.Repeat(" ", 1990)}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 100)},
			results: [][]interface{}{[]interface{}{strings.Repeat("a", 100) + strings.Repeat(" ", 1900)}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 1000)},
			results: [][]interface{}{[]interface{}{strings.Repeat("a", 1000) + strings.Repeat(" ", 1000)}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("ab", 1000)},
			results: [][]interface{}{[]interface{}{strings.Repeat("ab", 1000)}},
		},
	}

	// VARCHAR2(1)
	queryResults.query = "select cast (:1 as VARCHAR2(1)) from dual"
	queryResults.queryResults = queryResultStrings1
	testRunQueryResults(t, queryResults)

	// VARCHAR2(4000)
	queryResults.query = "select cast (:1 as VARCHAR2(4000)) from dual"
	queryResults.queryResults = queryResultStrings1
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultStrings2000
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultStrings4000
	testRunQueryResults(t, queryResults)

	// NVARCHAR2(1)
	queryResults.query = "select cast (:1 as NVARCHAR2(1)) from dual"
	queryResults.queryResults = queryResultStrings1
	testRunQueryResults(t, queryResults)

	// NVARCHAR2(2000)
	queryResults.query = "select cast (:1 as NVARCHAR2(2000)) from dual"
	queryResults.queryResults = queryResultStrings1
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultStrings2000
	testRunQueryResults(t, queryResults)

	// CHAR(1)
	queryResults.query = "select cast (:1 as CHAR(1)) from dual"
	queryResults.queryResults = queryResultStrings1
	testRunQueryResults(t, queryResults)

	// CHAR(2000)
	queryResults.query = "select cast (:1 as CHAR(2000)) from dual"
	queryResults.queryResults = queryResultStringsFix2000
	testRunQueryResults(t, queryResults)

	// NCHAR(1)
	queryResults.query = "select cast (:1 as NCHAR(1)) from dual"
	queryResults.queryResults = queryResultStrings1
	testRunQueryResults(t, queryResults)

	// NCHAR(1000)
	queryResults.query = "select cast (:1 as NCHAR(1000)) from dual"
	queryResults.queryResults = queryResultStringsFix1000
	testRunQueryResults(t, queryResults)

	// CLOB
	queryResults.query = "select to_clob(:1) from dual"
	queryResults.queryResults = queryResultStrings1
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultStrings2000
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultStrings4000
	testRunQueryResults(t, queryResults)
	queryResults.query = "select to_clob(rpad('a', 4000, 'a')) || to_clob(rpad('b', 4000, 'b')) from dual"
	queryResults.queryResults = queryResultStringsRpad8000
	testRunQueryResults(t, queryResults)

	// NCLOB
	queryResults.query = "select to_nclob(:1) from dual"
	queryResults.queryResults = queryResultStrings1
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultStrings2000
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultStrings4000
	testRunQueryResults(t, queryResults)
	queryResults.query = "select to_nclob(rpad('a', 4000, 'a')) || to_nclob(rpad('b', 4000, 'b')) from dual"
	queryResults.queryResults = queryResultStringsRpad8000
	testRunQueryResults(t, queryResults)

	queryResultRaw1 := []testQueryResult{
		testQueryResult{
			args:    []interface{}{[]byte(nil)},
			results: [][]interface{}{[]interface{}{nil}},
		},
		testQueryResult{
			args:    []interface{}{[]byte{}},
			results: [][]interface{}{[]interface{}{nil}},
		},
		testQueryResult{
			args:    []interface{}{[]byte{10}},
			results: [][]interface{}{[]interface{}{[]byte{10}}},
		},
		testQueryResult{
			args:    []interface{}{[]byte{0}},
			results: [][]interface{}{[]interface{}{[]byte{0}}},
		},
	}

	queryResultRaw2000 := []testQueryResult{
		testQueryResult{
			args:    []interface{}{[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
			results: [][]interface{}{[]interface{}{[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}}},
		},
		testQueryResult{
			args:    []interface{}{[]byte{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
			results: [][]interface{}{[]interface{}{[]byte{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}}},
		},
		testQueryResult{
			args:    []interface{}{[]byte{245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255}},
			results: [][]interface{}{[]interface{}{[]byte{245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255}}},
		},
		testQueryResult{
			args:    []interface{}{testByteSlice2000},
			results: [][]interface{}{[]interface{}{testByteSlice2000}},
		},
	}

	// RAW(1)
	queryResults.query = "select cast (:1 as RAW(1)) from dual"
	queryResults.queryResults = queryResultRaw1
	testRunQueryResults(t, queryResults)

	// RAW(2000)
	queryResults.query = "select cast (:1 as RAW(2000)) from dual"
	queryResults.queryResults = queryResultRaw1
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultRaw2000
	testRunQueryResults(t, queryResults)

	// BLOB
	queryResults.query = "select to_blob(:1) from dual"
	queryResults.queryResults = queryResultRaw1
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultRaw2000
	testRunQueryResults(t, queryResults)

	// test strings add to end

	queryResultStringsAddEnd1 := []testQueryResult{
		testQueryResult{
			args:    []interface{}{""},
			results: [][]interface{}{[]interface{}{"xyz"}},
		},
		testQueryResult{
			args:    []interface{}{"a"},
			results: [][]interface{}{[]interface{}{"axyz"}},
		},
		testQueryResult{
			args:    []interface{}{"\x00"},
			results: [][]interface{}{[]interface{}{"\x00xyz"}},
		},
	}

	queryResultStringsAddEnd2000 := []testQueryResult{
		testQueryResult{
			args:    []interface{}{"abc    "},
			results: [][]interface{}{[]interface{}{"abc    xyz"}},
		},
		testQueryResult{
			args:    []interface{}{"    abc"},
			results: [][]interface{}{[]interface{}{"    abcxyz"}},
		},
		testQueryResult{
			args:    []interface{}{"    abc    "},
			results: [][]interface{}{[]interface{}{"    abc    xyz"}},
		},
		testQueryResult{
			args:    []interface{}{"123"},
			results: [][]interface{}{[]interface{}{"123xyz"}},
		},
		testQueryResult{
			args:    []interface{}{"123.456"},
			results: [][]interface{}{[]interface{}{"123.456xyz"}},
		},
		testQueryResult{
			args:    []interface{}{"abcdefghijklmnopqrstuvwxyz"},
			results: [][]interface{}{[]interface{}{"abcdefghijklmnopqrstuvwxyzxyz"}},
		},
		testQueryResult{
			args:    []interface{}{" a b c d e f g h i j k l m n o p q r s t u v w x y z "},
			results: [][]interface{}{[]interface{}{" a b c d e f g h i j k l m n o p q r s t u v w x y z xyz"}},
		},
		testQueryResult{
			args:    []interface{}{"ab\ncd\nef"},
			results: [][]interface{}{[]interface{}{"ab\ncd\nefxyz"}},
		},
		testQueryResult{
			args:    []interface{}{"ab\tcd\tef"},
			results: [][]interface{}{[]interface{}{"ab\tcd\tefxyz"}},
		},
		testQueryResult{
			args:    []interface{}{"ab\x00cd\x00ef"},
			results: [][]interface{}{[]interface{}{"ab\x00cd\x00efxyz"}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 100)},
			results: [][]interface{}{[]interface{}{strings.Repeat("a", 100) + "xyz"}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 1000)},
			results: [][]interface{}{[]interface{}{strings.Repeat("a", 1000) + "xyz"}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("ab", 998)},
			results: [][]interface{}{[]interface{}{strings.Repeat("ab", 998) + "xyz"}},
		},
	}

	queryResultStringsAddEnd4000 := []testQueryResult{
		testQueryResult{
			args:    []interface{}{strings.Repeat("abcd", 999)},
			results: [][]interface{}{[]interface{}{strings.Repeat("abcd", 999) + "xyz"}},
		},
		testQueryResult{
			args:    []interface{}{testString1},
			results: [][]interface{}{[]interface{}{testString1 + "xyz"}},
		},
	}

	queryResultStringsAddEndFix1000 := []testQueryResult{
		testQueryResult{
			args:    []interface{}{""},
			results: [][]interface{}{[]interface{}{"xyz"}},
		},
		testQueryResult{
			args:    []interface{}{"a"},
			results: [][]interface{}{[]interface{}{"a" + strings.Repeat(" ", 999) + "xyz"}},
		},
		testQueryResult{
			args:    []interface{}{"abc    "},
			results: [][]interface{}{[]interface{}{"abc    " + strings.Repeat(" ", 993) + "xyz"}},
		},
		testQueryResult{
			args:    []interface{}{"    abc"},
			results: [][]interface{}{[]interface{}{"    abc" + strings.Repeat(" ", 993) + "xyz"}},
		},
		testQueryResult{
			args:    []interface{}{"    abc    "},
			results: [][]interface{}{[]interface{}{"    abc    " + strings.Repeat(" ", 989) + "xyz"}},
		},
		testQueryResult{
			args:    []interface{}{"ab\ncd\nef"},
			results: [][]interface{}{[]interface{}{"ab\ncd\nef" + strings.Repeat(" ", 992) + "xyz"}},
		},
		testQueryResult{
			args:    []interface{}{"ab\tcd\tef"},
			results: [][]interface{}{[]interface{}{"ab\tcd\tef" + strings.Repeat(" ", 992) + "xyz"}},
		},
		testQueryResult{
			args:    []interface{}{"ab\x00cd\x00ef"},
			results: [][]interface{}{[]interface{}{"ab\x00cd\x00ef" + strings.Repeat(" ", 992) + "xyz"}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 10)},
			results: [][]interface{}{[]interface{}{strings.Repeat("a", 10) + strings.Repeat(" ", 990) + "xyz"}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 100)},
			results: [][]interface{}{[]interface{}{strings.Repeat("a", 100) + strings.Repeat(" ", 900) + "xyz"}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 1000)},
			results: [][]interface{}{[]interface{}{strings.Repeat("a", 1000) + "xyz"}},
		},
	}

	queryResultStringsAddEndFix2000 := []testQueryResult{
		testQueryResult{
			args:    []interface{}{""},
			results: [][]interface{}{[]interface{}{"xyz"}},
		},
		testQueryResult{
			args:    []interface{}{"a"},
			results: [][]interface{}{[]interface{}{"a" + strings.Repeat(" ", 1999) + "xyz"}},
		},
		testQueryResult{
			args:    []interface{}{"abc    "},
			results: [][]interface{}{[]interface{}{"abc    " + strings.Repeat(" ", 1993) + "xyz"}},
		},
		testQueryResult{
			args:    []interface{}{"    abc"},
			results: [][]interface{}{[]interface{}{"    abc" + strings.Repeat(" ", 1993) + "xyz"}},
		},
		testQueryResult{
			args:    []interface{}{"    abc    "},
			results: [][]interface{}{[]interface{}{"    abc    " + strings.Repeat(" ", 1989) + "xyz"}},
		},
		testQueryResult{
			args:    []interface{}{"ab\ncd\nef"},
			results: [][]interface{}{[]interface{}{"ab\ncd\nef" + strings.Repeat(" ", 1992) + "xyz"}},
		},
		testQueryResult{
			args:    []interface{}{"ab\tcd\tef"},
			results: [][]interface{}{[]interface{}{"ab\tcd\tef" + strings.Repeat(" ", 1992) + "xyz"}},
		},
		testQueryResult{
			args:    []interface{}{"ab\x00cd\x00ef"},
			results: [][]interface{}{[]interface{}{"ab\x00cd\x00ef" + strings.Repeat(" ", 1992) + "xyz"}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 10)},
			results: [][]interface{}{[]interface{}{strings.Repeat("a", 10) + strings.Repeat(" ", 1990) + "xyz"}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 100)},
			results: [][]interface{}{[]interface{}{strings.Repeat("a", 100) + strings.Repeat(" ", 1900) + "xyz"}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 1000)},
			results: [][]interface{}{[]interface{}{strings.Repeat("a", 1000) + strings.Repeat(" ", 1000) + "xyz"}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("ab", 1000)},
			results: [][]interface{}{[]interface{}{strings.Repeat("ab", 1000) + "xyz"}},
		},
	}

	// VARCHAR2(1)
	queryResults.query = "select cast (:1 as VARCHAR2(1)) || 'xyz' from dual"
	queryResults.queryResults = queryResultStringsAddEnd1
	testRunQueryResults(t, queryResults)

	// VARCHAR2(4000)
	queryResults.query = "select cast (:1 as VARCHAR2(4000)) || 'xyz' from dual"
	queryResults.queryResults = queryResultStringsAddEnd1
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultStringsAddEnd2000
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultStringsAddEnd4000
	testRunQueryResults(t, queryResults)

	// NVARCHAR2(1)
	queryResults.query = "select cast (:1 as NVARCHAR2(1)) || 'xyz' from dual"
	queryResults.queryResults = queryResultStringsAddEnd1
	testRunQueryResults(t, queryResults)

	// NVARCHAR2(2000)
	queryResults.query = "select cast (:1 as NVARCHAR2(2000)) || 'xyz' from dual"
	queryResults.queryResults = queryResultStringsAddEnd1
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultStringsAddEnd2000
	testRunQueryResults(t, queryResults)

	// CHAR(1)
	queryResults.query = "select cast (:1 as CHAR(1)) || 'xyz' from dual"
	queryResults.queryResults = queryResultStringsAddEnd1
	testRunQueryResults(t, queryResults)

	// CHAR(2000)
	queryResults.query = "select cast (:1 as CHAR(2000)) || 'xyz' from dual"
	queryResults.queryResults = queryResultStringsAddEndFix2000
	testRunQueryResults(t, queryResults)

	// NCHAR(1)
	queryResults.query = "select cast (:1 as NCHAR(1)) || 'xyz' from dual"
	queryResults.queryResults = queryResultStringsAddEnd1
	testRunQueryResults(t, queryResults)

	// NCHAR(1000)
	queryResults.query = "select cast (:1 as NCHAR(1000)) || 'xyz' from dual"
	queryResults.queryResults = queryResultStringsAddEndFix1000
	testRunQueryResults(t, queryResults)

	// CLOB
	queryResults.query = "select to_clob(:1) || 'xyz' from dual"
	queryResults.queryResults = queryResultStringsAddEnd1
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultStringsAddEnd2000
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultStringsAddEnd4000
	testRunQueryResults(t, queryResults)

	// NCLOB
	queryResults.query = "select to_nclob(:1) || 'xyz' from dual"
	queryResults.queryResults = queryResultStringsAddEnd1
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultStringsAddEnd2000
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultStringsAddEnd4000
	testRunQueryResults(t, queryResults)

	// test strings add to front

	queryResultStringsAddFront1 := []testQueryResult{
		testQueryResult{
			args:    []interface{}{""},
			results: [][]interface{}{[]interface{}{"xyz"}},
		},
		testQueryResult{
			args:    []interface{}{"a"},
			results: [][]interface{}{[]interface{}{"xyza"}},
		},
		testQueryResult{
			args:    []interface{}{"\x00"},
			results: [][]interface{}{[]interface{}{"xyz\x00"}},
		},
	}

	queryResultStringsAddFront2000 := []testQueryResult{
		testQueryResult{
			args:    []interface{}{"abc    "},
			results: [][]interface{}{[]interface{}{"xyzabc    "}},
		},
		testQueryResult{
			args:    []interface{}{"    abc"},
			results: [][]interface{}{[]interface{}{"xyz    abc"}},
		},
		testQueryResult{
			args:    []interface{}{"    abc    "},
			results: [][]interface{}{[]interface{}{"xyz    abc    "}},
		},
		testQueryResult{
			args:    []interface{}{"123"},
			results: [][]interface{}{[]interface{}{"xyz123"}},
		},
		testQueryResult{
			args:    []interface{}{"123.456"},
			results: [][]interface{}{[]interface{}{"xyz123.456"}},
		},
		testQueryResult{
			args:    []interface{}{"abcdefghijklmnopqrstuvwxyz"},
			results: [][]interface{}{[]interface{}{"xyzabcdefghijklmnopqrstuvwxyz"}},
		},
		testQueryResult{
			args:    []interface{}{" a b c d e f g h i j k l m n o p q r s t u v w x y z "},
			results: [][]interface{}{[]interface{}{"xyz a b c d e f g h i j k l m n o p q r s t u v w x y z "}},
		},
		testQueryResult{
			args:    []interface{}{"ab\ncd\nef"},
			results: [][]interface{}{[]interface{}{"xyzab\ncd\nef"}},
		},
		testQueryResult{
			args:    []interface{}{"ab\tcd\tef"},
			results: [][]interface{}{[]interface{}{"xyzab\tcd\tef"}},
		},
		testQueryResult{
			args:    []interface{}{"ab\x00cd\x00ef"},
			results: [][]interface{}{[]interface{}{"xyzab\x00cd\x00ef"}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 100)},
			results: [][]interface{}{[]interface{}{"xyz" + strings.Repeat("a", 100)}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 1000)},
			results: [][]interface{}{[]interface{}{"xyz" + strings.Repeat("a", 1000)}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("ab", 998)},
			results: [][]interface{}{[]interface{}{"xyz" + strings.Repeat("ab", 998)}},
		},
	}

	queryResultStringsAddFront4000 := []testQueryResult{
		testQueryResult{
			args:    []interface{}{strings.Repeat("abcd", 999)},
			results: [][]interface{}{[]interface{}{"xyz" + strings.Repeat("abcd", 999)}},
		},
		testQueryResult{
			args:    []interface{}{testString1},
			results: [][]interface{}{[]interface{}{"xyz" + testString1}},
		},
	}

	queryResultStringsAddFrontFix1000 := []testQueryResult{
		testQueryResult{
			args:    []interface{}{""},
			results: [][]interface{}{[]interface{}{"xyz"}},
		},
		testQueryResult{
			args:    []interface{}{"a"},
			results: [][]interface{}{[]interface{}{"xyza" + strings.Repeat(" ", 999)}},
		},
		testQueryResult{
			args:    []interface{}{"abc    "},
			results: [][]interface{}{[]interface{}{"xyzabc    " + strings.Repeat(" ", 993)}},
		},
		testQueryResult{
			args:    []interface{}{"    abc"},
			results: [][]interface{}{[]interface{}{"xyz    abc" + strings.Repeat(" ", 993)}},
		},
		testQueryResult{
			args:    []interface{}{"    abc    "},
			results: [][]interface{}{[]interface{}{"xyz    abc    " + strings.Repeat(" ", 989)}},
		},
		testQueryResult{
			args:    []interface{}{"ab\ncd\nef"},
			results: [][]interface{}{[]interface{}{"xyzab\ncd\nef" + strings.Repeat(" ", 992)}},
		},
		testQueryResult{
			args:    []interface{}{"ab\tcd\tef"},
			results: [][]interface{}{[]interface{}{"xyzab\tcd\tef" + strings.Repeat(" ", 992)}},
		},
		testQueryResult{
			args:    []interface{}{"ab\x00cd\x00ef"},
			results: [][]interface{}{[]interface{}{"xyzab\x00cd\x00ef" + strings.Repeat(" ", 992)}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 10)},
			results: [][]interface{}{[]interface{}{"xyz" + strings.Repeat("a", 10) + strings.Repeat(" ", 990)}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 100)},
			results: [][]interface{}{[]interface{}{"xyz" + strings.Repeat("a", 100) + strings.Repeat(" ", 900)}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 1000)},
			results: [][]interface{}{[]interface{}{"xyz" + strings.Repeat("a", 1000)}},
		},
	}

	queryResultStringsAddFrontFix2000 := []testQueryResult{
		testQueryResult{
			args:    []interface{}{""},
			results: [][]interface{}{[]interface{}{"xyz"}},
		},
		testQueryResult{
			args:    []interface{}{"a"},
			results: [][]interface{}{[]interface{}{"xyza" + strings.Repeat(" ", 1999)}},
		},
		testQueryResult{
			args:    []interface{}{"abc    "},
			results: [][]interface{}{[]interface{}{"xyzabc    " + strings.Repeat(" ", 1993)}},
		},
		testQueryResult{
			args:    []interface{}{"    abc"},
			results: [][]interface{}{[]interface{}{"xyz    abc" + strings.Repeat(" ", 1993)}},
		},
		testQueryResult{
			args:    []interface{}{"    abc    "},
			results: [][]interface{}{[]interface{}{"xyz    abc    " + strings.Repeat(" ", 1989)}},
		},
		testQueryResult{
			args:    []interface{}{"ab\ncd\nef"},
			results: [][]interface{}{[]interface{}{"xyzab\ncd\nef" + strings.Repeat(" ", 1992)}},
		},
		testQueryResult{
			args:    []interface{}{"ab\tcd\tef"},
			results: [][]interface{}{[]interface{}{"xyzab\tcd\tef" + strings.Repeat(" ", 1992)}},
		},
		testQueryResult{
			args:    []interface{}{"ab\x00cd\x00ef"},
			results: [][]interface{}{[]interface{}{"xyzab\x00cd\x00ef" + strings.Repeat(" ", 1992)}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 10)},
			results: [][]interface{}{[]interface{}{"xyz" + strings.Repeat("a", 10) + strings.Repeat(" ", 1990)}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 100)},
			results: [][]interface{}{[]interface{}{"xyz" + strings.Repeat("a", 100) + strings.Repeat(" ", 1900)}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 1000)},
			results: [][]interface{}{[]interface{}{"xyz" + strings.Repeat("a", 1000) + strings.Repeat(" ", 1000)}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("ab", 1000)},
			results: [][]interface{}{[]interface{}{"xyz" + strings.Repeat("ab", 1000)}},
		},
	}

	// VARCHAR2(1)
	queryResults.query = "select 'xyz' || cast (:1 as VARCHAR2(1)) from dual"
	queryResults.queryResults = queryResultStringsAddFront1
	testRunQueryResults(t, queryResults)

	// VARCHAR2(4000)
	queryResults.query = "select 'xyz' || cast (:1 as VARCHAR2(4000)) from dual"
	queryResults.queryResults = queryResultStringsAddFront1
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultStringsAddFront2000
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultStringsAddFront4000
	testRunQueryResults(t, queryResults)

	// NVARCHAR2(1)
	queryResults.query = "select 'xyz' || cast (:1 as NVARCHAR2(1)) from dual"
	queryResults.queryResults = queryResultStringsAddFront1
	testRunQueryResults(t, queryResults)

	// NVARCHAR2(2000)
	queryResults.query = "select 'xyz' || cast (:1 as NVARCHAR2(2000)) from dual"
	queryResults.queryResults = queryResultStringsAddFront1
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultStringsAddFront2000
	testRunQueryResults(t, queryResults)

	// CHAR(1)
	queryResults.query = "select 'xyz' || cast (:1 as CHAR(1)) from dual"
	queryResults.queryResults = queryResultStringsAddFront1
	testRunQueryResults(t, queryResults)

	// CHAR(2000)
	queryResults.query = "select 'xyz' || cast (:1 as CHAR(2000)) from dual"
	queryResults.queryResults = queryResultStringsAddFrontFix2000
	testRunQueryResults(t, queryResults)

	// NCHAR(1)
	queryResults.query = "select 'xyz' || cast (:1 as NCHAR(1)) from dual"
	queryResults.queryResults = queryResultStringsAddFront1
	testRunQueryResults(t, queryResults)

	// NCHAR(1000)
	queryResults.query = "select 'xyz' || cast (:1 as NCHAR(1000)) from dual"
	queryResults.queryResults = queryResultStringsAddFrontFix1000
	testRunQueryResults(t, queryResults)

	// CLOB
	queryResults.query = "select 'xyz' || to_clob(:1) from dual"
	queryResults.queryResults = queryResultStringsAddFront1
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultStringsAddFront2000
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultStringsAddFront4000
	testRunQueryResults(t, queryResults)

	// NCLOB
	queryResults.query = "select 'xyz' || to_nclob(:1) from dual"
	queryResults.queryResults = queryResultStringsAddFront1
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultStringsAddFront2000
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultStringsAddFront4000
	testRunQueryResults(t, queryResults)

	// test strings remove from front

	queryResultStringsRemoveFront1 := []testQueryResult{
		testQueryResult{
			args:    []interface{}{""},
			results: [][]interface{}{[]interface{}{nil}},
		},
		testQueryResult{
			args:    []interface{}{"a"},
			results: [][]interface{}{[]interface{}{nil}},
		},
		testQueryResult{
			args:    []interface{}{"\x00"},
			results: [][]interface{}{[]interface{}{nil}},
		},
	}

	queryResultStringsRemoveFront1Clob := []testQueryResult{
		testQueryResult{
			args:    []interface{}{""},
			results: [][]interface{}{[]interface{}{nil}},
		},
		// the indicator does not return as null, probably because they are empty clobs intead of null
		testQueryResult{
			args:    []interface{}{"a"},
			results: [][]interface{}{[]interface{}{""}},
		},
		testQueryResult{
			args:    []interface{}{"\x00"},
			results: [][]interface{}{[]interface{}{""}},
		},
	}

	queryResultStringsRemoveFront2000 := []testQueryResult{
		testQueryResult{
			args:    []interface{}{"abc"},
			results: [][]interface{}{[]interface{}{"c"}},
		},
		testQueryResult{
			args:    []interface{}{"abc    "},
			results: [][]interface{}{[]interface{}{"c    "}},
		},
		testQueryResult{
			args:    []interface{}{"    abc"},
			results: [][]interface{}{[]interface{}{"  abc"}},
		},
		testQueryResult{
			args:    []interface{}{"    abc    "},
			results: [][]interface{}{[]interface{}{"  abc    "}},
		},
		testQueryResult{
			args:    []interface{}{"123"},
			results: [][]interface{}{[]interface{}{"3"}},
		},
		testQueryResult{
			args:    []interface{}{"123.456"},
			results: [][]interface{}{[]interface{}{"3.456"}},
		},
		testQueryResult{
			args:    []interface{}{"abcdefghijklmnopqrstuvwxyz"},
			results: [][]interface{}{[]interface{}{"cdefghijklmnopqrstuvwxyz"}},
		},
		testQueryResult{
			args:    []interface{}{" a b c d e f g h i j k l m n o p q r s t u v w x y z "},
			results: [][]interface{}{[]interface{}{" b c d e f g h i j k l m n o p q r s t u v w x y z "}},
		},
		testQueryResult{
			args:    []interface{}{"ab\ncd\nef"},
			results: [][]interface{}{[]interface{}{"\ncd\nef"}},
		},
		testQueryResult{
			args:    []interface{}{"ab\tcd\tef"},
			results: [][]interface{}{[]interface{}{"\tcd\tef"}},
		},
		testQueryResult{
			args:    []interface{}{"ab\x00cd\x00ef"},
			results: [][]interface{}{[]interface{}{"\x00cd\x00ef"}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 100)},
			results: [][]interface{}{[]interface{}{strings.Repeat("a", 98)}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 1000)},
			results: [][]interface{}{[]interface{}{strings.Repeat("a", 998)}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("ab", 1000)},
			results: [][]interface{}{[]interface{}{strings.Repeat("ab", 999)}},
		},
	}

	queryResultStringsRemoveFront4000 := []testQueryResult{
		testQueryResult{
			args:    []interface{}{strings.Repeat("ab", 2000)},
			results: [][]interface{}{[]interface{}{strings.Repeat("ab", 1999)}},
		},
		testQueryResult{
			args:    []interface{}{testString1},
			results: [][]interface{}{[]interface{}{testString1[2:]}},
		},
	}

	queryResultStringsRemoveFrontFix1000 := []testQueryResult{
		testQueryResult{
			args:    []interface{}{""},
			results: [][]interface{}{[]interface{}{nil}},
		},
		testQueryResult{
			args:    []interface{}{"a"},
			results: [][]interface{}{[]interface{}{strings.Repeat(" ", 998)}},
		},
		testQueryResult{
			args:    []interface{}{"abc    "},
			results: [][]interface{}{[]interface{}{"c    " + strings.Repeat(" ", 993)}},
		},
		testQueryResult{
			args:    []interface{}{"    abc"},
			results: [][]interface{}{[]interface{}{"  abc" + strings.Repeat(" ", 993)}},
		},
		testQueryResult{
			args:    []interface{}{"    abc    "},
			results: [][]interface{}{[]interface{}{"  abc    " + strings.Repeat(" ", 989)}},
		},
		testQueryResult{
			args:    []interface{}{"ab\ncd\nef"},
			results: [][]interface{}{[]interface{}{"\ncd\nef" + strings.Repeat(" ", 992)}},
		},
		testQueryResult{
			args:    []interface{}{"ab\tcd\tef"},
			results: [][]interface{}{[]interface{}{"\tcd\tef" + strings.Repeat(" ", 992)}},
		},
		testQueryResult{
			args:    []interface{}{"ab\x00cd\x00ef"},
			results: [][]interface{}{[]interface{}{"\x00cd\x00ef" + strings.Repeat(" ", 992)}},
		},

		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 10)},
			results: [][]interface{}{[]interface{}{strings.Repeat("a", 8) + strings.Repeat(" ", 990)}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 100)},
			results: [][]interface{}{[]interface{}{strings.Repeat("a", 98) + strings.Repeat(" ", 900)}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 1000)},
			results: [][]interface{}{[]interface{}{strings.Repeat("a", 998)}},
		},
	}

	queryResultStringsRemoveFrontFix2000 := []testQueryResult{
		testQueryResult{
			args:    []interface{}{""},
			results: [][]interface{}{[]interface{}{nil}},
		},
		testQueryResult{
			args:    []interface{}{"a"},
			results: [][]interface{}{[]interface{}{strings.Repeat(" ", 1998)}},
		},
		testQueryResult{
			args:    []interface{}{"abc    "},
			results: [][]interface{}{[]interface{}{"c    " + strings.Repeat(" ", 1993)}},
		},
		testQueryResult{
			args:    []interface{}{"    abc"},
			results: [][]interface{}{[]interface{}{"  abc" + strings.Repeat(" ", 1993)}},
		},
		testQueryResult{
			args:    []interface{}{"    abc    "},
			results: [][]interface{}{[]interface{}{"  abc    " + strings.Repeat(" ", 1989)}},
		},
		testQueryResult{
			args:    []interface{}{"ab\ncd\nef"},
			results: [][]interface{}{[]interface{}{"\ncd\nef" + strings.Repeat(" ", 1992)}},
		},
		testQueryResult{
			args:    []interface{}{"ab\tcd\tef"},
			results: [][]interface{}{[]interface{}{"\tcd\tef" + strings.Repeat(" ", 1992)}},
		},
		testQueryResult{
			args:    []interface{}{"ab\x00cd\x00ef"},
			results: [][]interface{}{[]interface{}{"\x00cd\x00ef" + strings.Repeat(" ", 1992)}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 10)},
			results: [][]interface{}{[]interface{}{strings.Repeat("a", 8) + strings.Repeat(" ", 1990)}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 100)},
			results: [][]interface{}{[]interface{}{strings.Repeat("a", 98) + strings.Repeat(" ", 1900)}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("a", 1000)},
			results: [][]interface{}{[]interface{}{strings.Repeat("a", 998) + strings.Repeat(" ", 1000)}},
		},
		testQueryResult{
			args:    []interface{}{strings.Repeat("ab", 1000)},
			results: [][]interface{}{[]interface{}{strings.Repeat("ab", 999)}},
		},
	}

	// VARCHAR2(4000)
	queryResults.query = "select substr(cast (:1 as VARCHAR2(4000)), 3) from dual"
	queryResults.queryResults = queryResultStringsRemoveFront1
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultStringsRemoveFront2000
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultStringsRemoveFront4000
	testRunQueryResults(t, queryResults)

	// NVARCHAR2(2000)
	queryResults.query = "select substr(cast (:1 as NVARCHAR2(2000)), 3) from dual"
	queryResults.queryResults = queryResultStringsRemoveFront1
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultStringsRemoveFront2000
	testRunQueryResults(t, queryResults)

	// CHAR(2000)
	queryResults.query = "select substr(cast (:1 as CHAR(2000)), 3) from dual"
	queryResults.queryResults = queryResultStringsRemoveFrontFix2000
	testRunQueryResults(t, queryResults)

	// NCHAR(1000)
	queryResults.query = "select substr(cast (:1 as NCHAR(1000)), 3) from dual"
	queryResults.queryResults = queryResultStringsRemoveFrontFix1000
	testRunQueryResults(t, queryResults)

	// CLOB
	queryResults.query = "select substr(to_clob(:1), 3) from dual"
	queryResults.queryResults = queryResultStringsRemoveFront1Clob
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultStringsRemoveFront2000
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultStringsRemoveFront4000
	testRunQueryResults(t, queryResults)

	// NCLOB
	queryResults.query = "select substr(to_nclob(:1), 3) from dual"
	queryResults.queryResults = queryResultStringsRemoveFront1Clob
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultStringsRemoveFront2000
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultStringsRemoveFront4000
	testRunQueryResults(t, queryResults)

	// more test strings no change

	// ROWID
	ctx, cancel := context.WithTimeout(context.Background(), TestContextTimeout)
	stmt, err := TestDB.PrepareContext(ctx, "select ROWID from dual")
	cancel()
	if err != nil {
		t.Fatal("prepare error:", err)
	}

	var result [][]interface{}
	result, err = testGetRows(t, stmt, nil)
	if err != nil {
		t.Fatal("get rows error:", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}
	if len(result) < 1 {
		t.Fatal("len result less than 1")
	}
	if len(result[0]) < 1 {
		t.Fatal("len result[0] less than 1")
	}
	data, ok := result[0][0].(string)
	if !ok {
		t.Fatal("result not string")
	}
	if len(data) != 18 {
		t.Fatal("result len not equal to 18:", len(data))
	}

	queryResultRune := []testQueryResult{
		testQueryResult{
			args:    []interface{}{'a'},
			results: [][]interface{}{[]interface{}{float64(97)}},
		},
		testQueryResult{
			args:    []interface{}{'z'},
			results: [][]interface{}{[]interface{}{float64(122)}},
		},
	}

	queryResultByte := []testQueryResult{
		testQueryResult{
			args:    []interface{}{byte('a')},
			results: [][]interface{}{[]interface{}{float64(97)}},
		},
		testQueryResult{
			args:    []interface{}{byte('z')},
			results: [][]interface{}{[]interface{}{float64(122)}},
		},
	}

	// Go string
	queryResults.query = "select :1 from dual"
	queryResults.queryResults = queryResultStrings1
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultStrings2000
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultStrings4000
	testRunQueryResults(t, queryResults)

	// Go []byte
	queryResults.queryResults = queryResultRaw1
	testRunQueryResults(t, queryResults)
	queryResults.queryResults = queryResultRaw2000
	testRunQueryResults(t, queryResults)

	// Go rune
	queryResults.queryResults = queryResultRune
	testRunQueryResults(t, queryResults)

	// Go byte
	queryResults.queryResults = queryResultByte
	testRunQueryResults(t, queryResults)
}

// TestDestructiveString checks insert, select, update, and delete of string types
func TestDestructiveString(t *testing.T) {
	if TestDisableDatabase || TestDisableDestructive {
		t.SkipNow()
	}

	// VARCHAR2
	tableName := "VARCHAR2_" + TestTimeString
	err := testExec(t, "create table "+tableName+" ( A VARCHAR2(1), B VARCHAR2(2000), C VARCHAR2(4000) )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer testDropTable(t, tableName)

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{"a", strings.Repeat("a", 2000), strings.Repeat("a", 4000)},
			[]interface{}{"b", strings.Repeat("b", 2000), strings.Repeat("b", 4000)},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults := testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{"a", strings.Repeat("a", 2000), strings.Repeat("a", 4000)},
					[]interface{}{"b", strings.Repeat("b", 2000), strings.Repeat("b", 4000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExec(t, "delete from "+tableName+" where A = :1", []interface{}{"a"})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName,
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{"b", strings.Repeat("b", 2000), strings.Repeat("b", 4000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// NVARCHAR2
	tableName = "NVARCHAR2_" + TestTimeString
	err = testExec(t, "create table "+tableName+" ( A NVARCHAR2(1), B NVARCHAR2(1000), C NVARCHAR2(2000) )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer testDropTable(t, tableName)

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{"a", strings.Repeat("a", 1000), strings.Repeat("a", 2000)},
			[]interface{}{"b", strings.Repeat("b", 1000), strings.Repeat("b", 2000)},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{"a", strings.Repeat("a", 1000), strings.Repeat("a", 2000)},
					[]interface{}{"b", strings.Repeat("b", 1000), strings.Repeat("b", 2000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExec(t, "delete from "+tableName+" where A = :1", []interface{}{"a"})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName,
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{"b", strings.Repeat("b", 1000), strings.Repeat("b", 2000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// CHAR
	tableName = "CHAR_" + TestTimeString
	err = testExec(t, "create table "+tableName+" ( A CHAR(1), B CHAR(1000), C CHAR(2000) )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer testDropTable(t, tableName)

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{"a", strings.Repeat("a", 1000), strings.Repeat("a", 2000)},
			[]interface{}{"b", strings.Repeat("b", 1000), strings.Repeat("b", 2000)},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{"a", strings.Repeat("a", 1000), strings.Repeat("a", 2000)},
					[]interface{}{"b", strings.Repeat("b", 1000), strings.Repeat("b", 2000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExec(t, "delete from "+tableName+" where A = :1", []interface{}{"a"})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName,
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{"b", strings.Repeat("b", 1000), strings.Repeat("b", 2000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExec(t, "delete from "+tableName+" where A = :1", []interface{}{"b"})
	if err != nil {
		t.Error("delete error:", err)
	}

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{"a", strings.Repeat("a", 100), strings.Repeat("a", 200)},
			[]interface{}{"b", strings.Repeat("b", 100), strings.Repeat("b", 200)},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{"a", strings.Repeat("a", 100) + strings.Repeat(" ", 900), strings.Repeat("a", 200) + strings.Repeat(" ", 1800)},
					[]interface{}{"b", strings.Repeat("b", 100) + strings.Repeat(" ", 900), strings.Repeat("b", 200) + strings.Repeat(" ", 1800)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExec(t, "delete from "+tableName+" where A = :1", []interface{}{"a"})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName,
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{"b", strings.Repeat("b", 100) + strings.Repeat(" ", 900), strings.Repeat("b", 200) + strings.Repeat(" ", 1800)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// NCHAR
	tableName = "NCHAR_" + TestTimeString
	err = testExec(t, "create table NCHAR_"+TestTimeString+
		" ( A NCHAR(1), B NCHAR(500), C NCHAR(1000) )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer testDropTable(t, tableName)

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{"a", strings.Repeat("a", 500), strings.Repeat("a", 1000)},
			[]interface{}{"b", strings.Repeat("b", 500), strings.Repeat("b", 1000)},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{"a", strings.Repeat("a", 500), strings.Repeat("a", 1000)},
					[]interface{}{"b", strings.Repeat("b", 500), strings.Repeat("b", 1000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExec(t, "delete from "+tableName+" where A = :1", []interface{}{"a"})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName,
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{"b", strings.Repeat("b", 500), strings.Repeat("b", 1000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExec(t, "delete from "+tableName+" where A = :1", []interface{}{"b"})
	if err != nil {
		t.Error("delete error:", err)
	}

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{"a", strings.Repeat("a", 100), strings.Repeat("a", 200)},
			[]interface{}{"b", strings.Repeat("b", 100), strings.Repeat("b", 200)},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{"a", strings.Repeat("a", 100) + strings.Repeat(" ", 400), strings.Repeat("a", 200) + strings.Repeat(" ", 800)},
					[]interface{}{"b", strings.Repeat("b", 100) + strings.Repeat(" ", 400), strings.Repeat("b", 200) + strings.Repeat(" ", 800)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExec(t, "delete from "+tableName+" where A = :1", []interface{}{"a"})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName,
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{"b", strings.Repeat("b", 100) + strings.Repeat(" ", 400), strings.Repeat("b", 200) + strings.Repeat(" ", 800)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// LONG
	tableName = "LONG_" + TestTimeString
	err = testExec(t, "create table "+tableName+" ( A VARCHAR2(1), B VARCHAR2(2000), C LONG )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer testDropTable(t, tableName)

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{"a", strings.Repeat("a", 2000), strings.Repeat("a", 4000)},
			[]interface{}{"b", strings.Repeat("b", 2000), strings.Repeat("b", 4000)},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{"a", strings.Repeat("a", 2000), strings.Repeat("a", 4000)},
					[]interface{}{"b", strings.Repeat("b", 2000), strings.Repeat("b", 4000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExec(t, "delete from "+tableName+" where A = :1", []interface{}{"a"})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName,
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{"b", strings.Repeat("b", 2000), strings.Repeat("b", 4000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// RAW
	tableName = "RAW_" + TestTimeString
	err = testExec(t, "create table "+tableName+" ( A RAW(1), B RAW(1000), C RAW(2000) )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer testDropTable(t, tableName)

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{[]byte{}, []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []byte{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
			[]interface{}{[]byte{10}, []byte{245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255}, testByteSlice2000},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{[]byte{10}, []byte{245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255}, testByteSlice2000},
					[]interface{}{nil, []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []byte{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExec(t, "delete from "+tableName+" where A = :1", []interface{}{[]byte{10}})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName,
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{nil, []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []byte{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// ROWID
	ctx, cancel := context.WithTimeout(context.Background(), TestContextTimeout)
	stmt, err := TestDB.PrepareContext(ctx, "select ROWID from "+tableName)
	cancel()
	if err != nil {
		t.Fatal("prepare error:", err)
	}

	var result [][]interface{}
	result, err = testGetRows(t, stmt, nil)
	if err != nil {
		t.Fatal("get rows error:", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}
	if len(result) < 1 {
		t.Fatal("len result less than 1")
	}
	if len(result[0]) < 1 {
		t.Fatal("len result[0] less than 1")
	}
	data, ok := result[0][0].(string)
	if !ok {
		t.Fatal("result not string")
	}
	if len(data) != 18 {
		t.Fatal("result len not equal to 18:", len(data))
	}

	// CLOB
	tableName = "CLOB_" + TestTimeString
	err = testExec(t, "create table "+tableName+" ( A VARCHAR2(100), B CLOB, C CLOB )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer testDropTable(t, tableName)

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{"a", strings.Repeat("a", 2000), strings.Repeat("a", 4000)},
			[]interface{}{"b", strings.Repeat("b", 6000), strings.Repeat("b", 8000)},
			[]interface{}{"c", strings.Repeat("c", 12000), strings.Repeat("c", 16000)},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{"a", strings.Repeat("a", 2000), strings.Repeat("a", 4000)},
					[]interface{}{"b", strings.Repeat("b", 6000), strings.Repeat("b", 8000)},
					[]interface{}{"c", strings.Repeat("c", 12000), strings.Repeat("c", 16000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExec(t, "delete from "+tableName+" where A = :1", []interface{}{"a"})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName,
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{"b", strings.Repeat("b", 6000), strings.Repeat("b", 8000)},
					[]interface{}{"c", strings.Repeat("c", 12000), strings.Repeat("c", 16000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// NCLOB
	tableName = "NCLOB_" + TestTimeString
	err = testExec(t, "create table "+tableName+" ( A VARCHAR2(100), B NCLOB, C NCLOB )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer testDropTable(t, tableName)

	err = testExecRows(t, "insert into "+tableName+"( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{"a", strings.Repeat("a", 2000), strings.Repeat("a", 4000)},
			[]interface{}{"b", strings.Repeat("b", 6000), strings.Repeat("b", 8000)},
			[]interface{}{"c", strings.Repeat("c", 12000), strings.Repeat("c", 16000)},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{"a", strings.Repeat("a", 2000), strings.Repeat("a", 4000)},
					[]interface{}{"b", strings.Repeat("b", 6000), strings.Repeat("b", 8000)},
					[]interface{}{"c", strings.Repeat("c", 12000), strings.Repeat("c", 16000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExec(t, "delete from "+tableName+" where A = :1", []interface{}{"a"})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName,
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{"b", strings.Repeat("b", 6000), strings.Repeat("b", 8000)},
					[]interface{}{"c", strings.Repeat("c", 12000), strings.Repeat("c", 16000)},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	// BLOB
	tableName = "BLOB_" + TestTimeString
	err = testExec(t, "create table "+tableName+" ( A VARCHAR2(100), B BLOB, C BLOB )", nil)
	if err != nil {
		t.Fatal("create table error:", err)
	}

	defer testDropTable(t, tableName)

	err = testExecRows(t, "insert into "+tableName+" ( A, B, C ) values (:1, :2, :3)",
		[][]interface{}{
			[]interface{}{"a", []byte{}, []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
			[]interface{}{"b", []byte{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, []byte{245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255}},
		})
	if err != nil {
		t.Error("insert error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{"a", nil, []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
					[]interface{}{"b", []byte{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, []byte{245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255}},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

	err = testExec(t, "delete from "+tableName+" where A = :1", []interface{}{"a"})
	if err != nil {
		t.Error("delete error:", err)
	}

	queryResults = testQueryResults{
		query: "select A, B, C from " + tableName + " order by A",
		queryResults: []testQueryResult{
			testQueryResult{
				results: [][]interface{}{
					[]interface{}{"b", []byte{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, []byte{245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255}},
				},
			},
		},
	}
	testRunQueryResults(t, queryResults)

}

func TestFunctionCallString(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}

	var execResults testExecResults

	// test strings no change

	execResultStrings2000 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "", In: true}},
			results: map[string]interface{}{"string1": ""},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "a", In: true}},
			results: map[string]interface{}{"string1": "a"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "\x00", In: true}},
			results: map[string]interface{}{"string1": "\x00"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "abc    ", In: true}},
			results: map[string]interface{}{"string1": "abc    "},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "    abc", In: true}},
			results: map[string]interface{}{"string1": "    abc"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "    abc    ", In: true}},
			results: map[string]interface{}{"string1": "    abc    "},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "123", In: true}},
			results: map[string]interface{}{"string1": "123"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "123.456", In: true}},
			results: map[string]interface{}{"string1": "123.456"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "abcdefghijklmnopqrstuvwxyz", In: true}},
			results: map[string]interface{}{"string1": "abcdefghijklmnopqrstuvwxyz"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: " a b c d e f g h i j k l m n o p q r s t u v w x y z ", In: true}},
			results: map[string]interface{}{"string1": " a b c d e f g h i j k l m n o p q r s t u v w x y z "},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "ab\ncd\nef", In: true}},
			results: map[string]interface{}{"string1": "ab\ncd\nef"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "ab\tcd\tef", In: true}},
			results: map[string]interface{}{"string1": "ab\tcd\tef"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "ab\x00cd\x00ef", In: true}},
			results: map[string]interface{}{"string1": "ab\x00cd\x00ef"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: strings.Repeat("a", 100), In: true}},
			results: map[string]interface{}{"string1": strings.Repeat("a", 100)},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: strings.Repeat("a", 1000), In: true}},
			results: map[string]interface{}{"string1": strings.Repeat("a", 1000)},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: strings.Repeat("ab", 1000), In: true}},
			results: map[string]interface{}{"string1": strings.Repeat("ab", 1000)},
		},
	}

	execResultStrings4000 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: strings.Repeat("abcd", 1000), In: true}},
			results: map[string]interface{}{"string1": strings.Repeat("abcd", 1000)},
		},
	}

	execResultStrings16383 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: strings.Repeat("b", 16383), In: true}},
			results: map[string]interface{}{"string1": strings.Repeat("b", 16383)},
		},
	}

	execResultStrings32767 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: strings.Repeat("c", 32767), In: true}},
			results: map[string]interface{}{"string1": strings.Repeat("c", 32767)},
		},
	}

	// VARCHAR2
	execResults.query = `
declare
	function GET_STRING(p_string VARCHAR2) return VARCHAR2 as
	begin
		return p_string;
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultStrings2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStrings4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStrings16383
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStrings32767
	testRunExecResults(t, execResults)

	// NVARCHAR2
	execResults.query = `
declare
	function GET_STRING(p_string NVARCHAR2) return NVARCHAR2 as
	begin
		return p_string;
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultStrings2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStrings4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStrings16383
	testRunExecResults(t, execResults)

	// CHAR
	execResults.query = `
declare
	function GET_STRING(p_string CHAR) return CHAR as
	begin
		return p_string;
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultStrings2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStrings4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStrings16383
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStrings32767
	testRunExecResults(t, execResults)

	// NCHAR
	execResults.query = `
declare
	function GET_STRING(p_string NCHAR) return NCHAR as
	begin
		return p_string;
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultStrings2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStrings4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStrings16383
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStrings32767
	testRunExecResults(t, execResults)

	// CLOB
	execResults.query = `
declare
	function GET_STRING(p_string CLOB) return CLOB as
	begin
		return p_string;
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultStrings2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStrings4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStrings16383
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStrings32767
	testRunExecResults(t, execResults)

	// NCLOB
	execResults.query = `
declare
	function GET_STRING(p_string NCLOB) return NCLOB as
	begin
		return p_string;
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultStrings2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStrings4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStrings16383
	testRunExecResults(t, execResults)

	execResultRaw2000 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: []byte(nil), In: true}},
			results: map[string]interface{}{"string1": []byte(nil)},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: []byte{}, In: true}},
			results: map[string]interface{}{"string1": []byte(nil)},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: []byte{10}, In: true}},
			results: map[string]interface{}{"string1": []byte{10}},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: []byte{0}, In: true}},
			results: map[string]interface{}{"string1": []byte{0}},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, In: true}},
			results: map[string]interface{}{"string1": []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: []byte{245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255}, In: true}},
			results: map[string]interface{}{"string1": []byte{245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255}},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: testByteSlice2000, In: true}},
			results: map[string]interface{}{"string1": testByteSlice2000},
		},
	}

	execResultRaw4000 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: testByteSlice4000, In: true}},
			results: map[string]interface{}{"string1": testByteSlice4000},
		},
	}

	execResultRaw16383 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: testByteSlice16383, In: true}},
			results: map[string]interface{}{"string1": testByteSlice16383},
		},
	}

	execResultRaw32767 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: testByteSlice32767, In: true}},
			results: map[string]interface{}{"string1": testByteSlice32767},
		},
	}

	// RAW
	execResults.query = `
declare
	function GET_STRING(p_string RAW) return RAW as
	begin
		return p_string;
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultRaw2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultRaw4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultRaw16383
	testRunExecResults(t, execResults)
	execResults.execResults = execResultRaw32767
	testRunExecResults(t, execResults)

	// BLOB
	execResults.query = `
declare
	function GET_STRING(p_string BLOB) return BLOB as
	begin
		return p_string;
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultRaw2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultRaw4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultRaw16383
	testRunExecResults(t, execResults)
	execResults.execResults = execResultRaw32767
	testRunExecResults(t, execResults)

	// test strings add to end

	execResultStringsAddEnd2000 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "", In: true}},
			results: map[string]interface{}{"string1": "xyz"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "a", In: true}},
			results: map[string]interface{}{"string1": "axyz"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "\x00", In: true}},
			results: map[string]interface{}{"string1": "\x00xyz"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "abc    ", In: true}},
			results: map[string]interface{}{"string1": "abc    xyz"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "    abc", In: true}},
			results: map[string]interface{}{"string1": "    abcxyz"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "    abc    ", In: true}},
			results: map[string]interface{}{"string1": "    abc    xyz"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "123", In: true}},
			results: map[string]interface{}{"string1": "123xyz"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "123.456", In: true}},
			results: map[string]interface{}{"string1": "123.456xyz"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "abcdefghijklmnopqrstuvwxyz", In: true}},
			results: map[string]interface{}{"string1": "abcdefghijklmnopqrstuvwxyzxyz"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: " a b c d e f g h i j k l m n o p q r s t u v w x y z ", In: true}},
			results: map[string]interface{}{"string1": " a b c d e f g h i j k l m n o p q r s t u v w x y z xyz"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "ab\ncd\nef", In: true}},
			results: map[string]interface{}{"string1": "ab\ncd\nefxyz"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "ab\tcd\tef", In: true}},
			results: map[string]interface{}{"string1": "ab\tcd\tefxyz"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "ab\x00cd\x00ef", In: true}},
			results: map[string]interface{}{"string1": "ab\x00cd\x00efxyz"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: strings.Repeat("a", 100), In: true}},
			results: map[string]interface{}{"string1": strings.Repeat("a", 100) + "xyz"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: strings.Repeat("a", 1000), In: true}},
			results: map[string]interface{}{"string1": strings.Repeat("a", 1000) + "xyz"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: strings.Repeat("ab", 1000), In: true}},
			results: map[string]interface{}{"string1": strings.Repeat("ab", 1000) + "xyz"},
		},
	}

	execResultStringsAddEnd4000 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: strings.Repeat("abcd", 999), In: true}},
			results: map[string]interface{}{"string1": strings.Repeat("abcd", 999) + "xyz"},
		},
	}

	execResultStringsAddEnd16383 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: strings.Repeat("b", 16380), In: true}},
			results: map[string]interface{}{"string1": strings.Repeat("b", 16380) + "xyz"},
		},
	}

	execResultStringsAddEnd32767 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: strings.Repeat("c", 32764), In: true}},
			results: map[string]interface{}{"string1": strings.Repeat("c", 32764) + "xyz"},
		},
	}

	// VARCHAR2
	execResults.query = `
declare
	function GET_STRING(p_string VARCHAR2) return VARCHAR2 as
	begin
		return p_string || 'xyz';
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultStringsAddEnd2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddEnd4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddEnd16383
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddEnd32767
	testRunExecResults(t, execResults)

	// NVARCHAR2
	execResults.query = `
declare
	function GET_STRING(p_string NVARCHAR2) return NVARCHAR2 as
	begin
		return p_string || 'xyz';
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultStringsAddEnd2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddEnd4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddEnd16383
	testRunExecResults(t, execResults)

	// CHAR
	execResults.query = `
declare
	function GET_STRING(p_string CHAR) return CHAR as
	begin
		return p_string || 'xyz';
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultStringsAddEnd2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddEnd4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddEnd16383
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddEnd32767
	testRunExecResults(t, execResults)

	// NCHAR
	execResults.query = `
declare
	function GET_STRING(p_string NCHAR) return NCHAR as
	begin
		return p_string || 'xyz';
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultStringsAddEnd2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddEnd4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddEnd16383
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddEnd32767
	testRunExecResults(t, execResults)

	// CLOB
	execResults.query = `
declare
	function GET_STRING(p_string CLOB) return CLOB as
	begin
		return p_string || 'xyz';
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultStringsAddEnd2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddEnd4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddEnd16383
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddEnd32767
	testRunExecResults(t, execResults)

	// NCLOB
	execResults.query = `
declare
	function GET_STRING(p_string NCLOB) return NCLOB as
	begin
		return p_string || 'xyz';
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultStringsAddEnd2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddEnd4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddEnd16383
	testRunExecResults(t, execResults)

	execResultRawAddEnd2000 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: []byte(nil), In: true}},
			results: map[string]interface{}{"string1": []byte{120, 121, 122}},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: []byte{}, In: true}},
			results: map[string]interface{}{"string1": []byte{120, 121, 122}},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: []byte{10}, In: true}},
			results: map[string]interface{}{"string1": []byte{10, 120, 121, 122}},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: []byte{0}, In: true}},
			results: map[string]interface{}{"string1": []byte{0, 120, 121, 122}},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, In: true}},
			results: map[string]interface{}{"string1": []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 120, 121, 122}},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: []byte{245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255}, In: true}},
			results: map[string]interface{}{"string1": []byte{245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255, 120, 121, 122}},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: testByteSlice2000[:1997], In: true}},
			results: map[string]interface{}{"string1": append(testByteSlice2000[:1997], []byte{120, 121, 122}...)},
		},
	}

	execResultRawAddEnd4000 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: testByteSlice4000[:3997], In: true}},
			results: map[string]interface{}{"string1": append(testByteSlice4000[:3997], []byte{120, 121, 122}...)},
		},
	}

	execResultRawAddEnd16383 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: testByteSlice16383[:16380], In: true}},
			results: map[string]interface{}{"string1": append(testByteSlice16383[:16380], []byte{120, 121, 122}...)},
		},
	}

	execResultRawAddEnd32767 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: testByteSlice32767[:32764], In: true}},
			results: map[string]interface{}{"string1": append(testByteSlice32767[:32764], []byte{120, 121, 122}...)},
		},
	}

	// RAW
	execResults.query = `
declare
	function GET_STRING(p_string RAW) return RAW as
	begin
		return UTL_RAW.CONCAT(p_string, UTL_RAW.CAST_TO_RAW('xyz'));
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultRawAddEnd2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultRawAddEnd4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultRawAddEnd16383
	testRunExecResults(t, execResults)
	execResults.execResults = execResultRawAddEnd32767
	testRunExecResults(t, execResults)

	// BLOB
	execResults.query = `
declare
	function GET_STRING(p_string BLOB) return BLOB as
		l_blob BLOB;
	begin
		if p_string is null then
			l_blob := UTL_RAW.CAST_TO_RAW('xyz');
			return l_blob;
		end if;
		l_blob := p_string;
		DBMS_LOB.APPEND(l_blob, UTL_RAW.CAST_TO_RAW('xyz'));
		return l_blob;
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultRawAddEnd2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultRawAddEnd4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultRawAddEnd16383
	testRunExecResults(t, execResults)
	execResults.execResults = execResultRawAddEnd32767
	testRunExecResults(t, execResults)

	// test strings add to front

	execResultStringsAddFront2000 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "", In: true}},
			results: map[string]interface{}{"string1": "xyz"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "a", In: true}},
			results: map[string]interface{}{"string1": "xyza"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "\x00", In: true}},
			results: map[string]interface{}{"string1": "xyz\x00"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "abc    ", In: true}},
			results: map[string]interface{}{"string1": "xyzabc    "},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "    abc", In: true}},
			results: map[string]interface{}{"string1": "xyz    abc"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "    abc    ", In: true}},
			results: map[string]interface{}{"string1": "xyz    abc    "},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "123", In: true}},
			results: map[string]interface{}{"string1": "xyz123"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "123.456", In: true}},
			results: map[string]interface{}{"string1": "xyz123.456"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "abcdefghijklmnopqrstuvwxyz", In: true}},
			results: map[string]interface{}{"string1": "xyzabcdefghijklmnopqrstuvwxyz"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: " a b c d e f g h i j k l m n o p q r s t u v w x y z ", In: true}},
			results: map[string]interface{}{"string1": "xyz a b c d e f g h i j k l m n o p q r s t u v w x y z "},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "ab\ncd\nef", In: true}},
			results: map[string]interface{}{"string1": "xyzab\ncd\nef"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "ab\tcd\tef", In: true}},
			results: map[string]interface{}{"string1": "xyzab\tcd\tef"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "ab\x00cd\x00ef", In: true}},
			results: map[string]interface{}{"string1": "xyzab\x00cd\x00ef"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: strings.Repeat("a", 100), In: true}},
			results: map[string]interface{}{"string1": "xyz" + strings.Repeat("a", 100)},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: strings.Repeat("a", 1000), In: true}},
			results: map[string]interface{}{"string1": "xyz" + strings.Repeat("a", 1000)},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: strings.Repeat("ab", 998), In: true}},
			results: map[string]interface{}{"string1": "xyz" + strings.Repeat("ab", 998)},
		},
	}

	execResultStringsAddFront4000 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: strings.Repeat("abcd", 999), In: true}},
			results: map[string]interface{}{"string1": "xyz" + strings.Repeat("abcd", 999)},
		},
	}

	execResultStringsAddFront16383 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: strings.Repeat("b", 16380), In: true}},
			results: map[string]interface{}{"string1": "xyz" + strings.Repeat("b", 16380)},
		},
	}

	execResultStringsAddFront32767 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: strings.Repeat("c", 32764), In: true}},
			results: map[string]interface{}{"string1": "xyz" + strings.Repeat("c", 32764)},
		},
	}

	// VARCHAR2
	execResults.query = `
declare
	function GET_STRING(p_string VARCHAR2) return VARCHAR2 as
	begin
		return 'xyz' || p_string;
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultStringsAddFront2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddFront4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddFront16383
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddFront32767
	testRunExecResults(t, execResults)

	// NVARCHAR2
	execResults.query = `
declare
	function GET_STRING(p_string NVARCHAR2) return NVARCHAR2 as
	begin
		return 'xyz' || p_string;
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultStringsAddFront2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddFront4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddFront16383
	testRunExecResults(t, execResults)

	// CHAR
	execResults.query = `
declare
	function GET_STRING(p_string CHAR) return CHAR as
	begin
		return 'xyz' || p_string;
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultStringsAddFront2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddFront4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddFront16383
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddFront32767
	testRunExecResults(t, execResults)

	// NCHAR
	execResults.query = `
declare
	function GET_STRING(p_string NCHAR) return NCHAR as
	begin
		return 'xyz' || p_string;
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultStringsAddFront2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddFront4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddFront16383
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddFront32767
	testRunExecResults(t, execResults)

	// CLOB
	execResults.query = `
declare
	function GET_STRING(p_string CLOB) return CLOB as
	begin
		return 'xyz' || p_string;
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultStringsAddFront2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddFront4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddFront16383
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddFront32767
	testRunExecResults(t, execResults)

	// NCLOB
	execResults.query = `
declare
	function GET_STRING(p_string NCLOB) return NCLOB as
	begin
		return 'xyz' || p_string;
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultStringsAddFront2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddFront4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsAddFront16383
	testRunExecResults(t, execResults)

	execResultRawAddFront2000 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: []byte(nil), In: true}},
			results: map[string]interface{}{"string1": []byte{120, 121, 122}},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: []byte{}, In: true}},
			results: map[string]interface{}{"string1": []byte{120, 121, 122}},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: []byte{10}, In: true}},
			results: map[string]interface{}{"string1": []byte{120, 121, 122, 10}},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: []byte{0}, In: true}},
			results: map[string]interface{}{"string1": []byte{120, 121, 122, 0}},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, In: true}},
			results: map[string]interface{}{"string1": []byte{120, 121, 122, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: []byte{245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255}, In: true}},
			results: map[string]interface{}{"string1": []byte{120, 121, 122, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255}},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: testByteSlice2000[:1997], In: true}},
			results: map[string]interface{}{"string1": append([]byte{120, 121, 122}, testByteSlice2000[:1997]...)},
		},
	}

	execResultRawAddFront4000 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: testByteSlice4000[:3997], In: true}},
			results: map[string]interface{}{"string1": append([]byte{120, 121, 122}, testByteSlice4000[:3997]...)},
		},
	}

	execResultRawAddFront16383 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: testByteSlice16383[:16380], In: true}},
			results: map[string]interface{}{"string1": append([]byte{120, 121, 122}, testByteSlice16383[:16380]...)},
		},
	}

	execResultRawAddFront32767 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: testByteSlice32767[:32764], In: true}},
			results: map[string]interface{}{"string1": append([]byte{120, 121, 122}, testByteSlice32767[:32764]...)},
		},
	}

	// RAW
	execResults.query = `
declare
	function GET_STRING(p_string RAW) return RAW as
	begin
		return UTL_RAW.CONCAT(UTL_RAW.CAST_TO_RAW('xyz'), p_string);
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultRawAddFront2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultRawAddFront4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultRawAddFront16383
	testRunExecResults(t, execResults)
	execResults.execResults = execResultRawAddFront32767
	testRunExecResults(t, execResults)

	// BLOB
	execResults.query = `
declare
	function GET_STRING(p_string BLOB) return BLOB as
		l_blob BLOB;
	begin
		if p_string is null then
			l_blob := UTL_RAW.CAST_TO_RAW('xyz');
			return l_blob;
		end if;
		l_blob := UTL_RAW.CAST_TO_RAW('xyz');
		DBMS_LOB.APPEND(l_blob, p_string);
		return l_blob;
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultRawAddFront2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultRawAddFront4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultRawAddFront16383
	testRunExecResults(t, execResults)
	execResults.execResults = execResultRawAddFront32767
	testRunExecResults(t, execResults)

	// test strings remove from front

	execResultStringsRemoveFront2000 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "", In: true}},
			results: map[string]interface{}{"string1": ""},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "a", In: true}},
			results: map[string]interface{}{"string1": ""},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "\x00", In: true}},
			results: map[string]interface{}{"string1": ""},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "abc", In: true}},
			results: map[string]interface{}{"string1": "c"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "abc    ", In: true}},
			results: map[string]interface{}{"string1": "c    "},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "    abc", In: true}},
			results: map[string]interface{}{"string1": "  abc"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "    abc    ", In: true}},
			results: map[string]interface{}{"string1": "  abc    "},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "123", In: true}},
			results: map[string]interface{}{"string1": "3"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "123.456", In: true}},
			results: map[string]interface{}{"string1": "3.456"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "abcdefghijklmnopqrstuvwxyz", In: true}},
			results: map[string]interface{}{"string1": "cdefghijklmnopqrstuvwxyz"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: " a b c d e f g h i j k l m n o p q r s t u v w x y z ", In: true}},
			results: map[string]interface{}{"string1": " b c d e f g h i j k l m n o p q r s t u v w x y z "},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "ab\ncd\nef", In: true}},
			results: map[string]interface{}{"string1": "\ncd\nef"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "ab\tcd\tef", In: true}},
			results: map[string]interface{}{"string1": "\tcd\tef"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: "ab\x00cd\x00ef", In: true}},
			results: map[string]interface{}{"string1": "\x00cd\x00ef"},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: strings.Repeat("a", 100), In: true}},
			results: map[string]interface{}{"string1": strings.Repeat("a", 98)},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: strings.Repeat("a", 1000), In: true}},
			results: map[string]interface{}{"string1": strings.Repeat("a", 998)},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: strings.Repeat("ab", 1000), In: true}},
			results: map[string]interface{}{"string1": strings.Repeat("ab", 999)},
		},
	}

	execResultStringsRemoveFront4000 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: strings.Repeat("abcd", 1000), In: true}},
			results: map[string]interface{}{"string1": "cd" + strings.Repeat("abcd", 999)},
		},
	}

	execResultStringsRemoveFront16383 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: strings.Repeat("b", 16383), In: true}},
			results: map[string]interface{}{"string1": strings.Repeat("b", 16381)},
		},
	}

	execResultStringsRemoveFront32767 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: strings.Repeat("c", 32767), In: true}},
			results: map[string]interface{}{"string1": strings.Repeat("c", 32765)},
		},
	}

	// VARCHAR2
	execResults.query = `
declare
	function GET_STRING(p_string VARCHAR2) return VARCHAR2 as
	begin
		return substr(p_string, 3);
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultStringsRemoveFront2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsRemoveFront4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsRemoveFront16383
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsRemoveFront32767
	testRunExecResults(t, execResults)

	// NVARCHAR2
	execResults.query = `
declare
	function GET_STRING(p_string NVARCHAR2) return NVARCHAR2 as
	begin
		return substr(p_string, 3);
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultStringsRemoveFront2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsRemoveFront4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsRemoveFront16383
	testRunExecResults(t, execResults)

	// CHAR
	execResults.query = `
declare
	function GET_STRING(p_string CHAR) return CHAR as
	begin
		return substr(p_string, 3);
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultStringsRemoveFront2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsRemoveFront4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsRemoveFront16383
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsRemoveFront32767
	testRunExecResults(t, execResults)

	// NCHAR
	execResults.query = `
declare
	function GET_STRING(p_string NCHAR) return NCHAR as
	begin
		return substr(p_string, 3);
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultStringsRemoveFront2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsRemoveFront4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsRemoveFront16383
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsRemoveFront32767
	testRunExecResults(t, execResults)

	// CLOB
	execResults.query = `
declare
	function GET_STRING(p_string CLOB) return CLOB as
	begin
		return substr(p_string, 3);
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultStringsRemoveFront2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsRemoveFront4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsRemoveFront16383
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsRemoveFront32767
	testRunExecResults(t, execResults)

	// NCLOB
	execResults.query = `
declare
	function GET_STRING(p_string NCLOB) return NCLOB as
	begin
		return substr(p_string, 3);
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultStringsRemoveFront2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsRemoveFront4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultStringsRemoveFront16383
	testRunExecResults(t, execResults)

	execResultRawRemoveFront2000 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: []byte(nil), In: true}},
			results: map[string]interface{}{"string1": []byte(nil)},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: []byte{}, In: true}},
			results: map[string]interface{}{"string1": []byte(nil)},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: []byte{10}, In: true}},
			results: map[string]interface{}{"string1": []byte(nil)},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: []byte{0}, In: true}},
			results: map[string]interface{}{"string1": []byte(nil)},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, In: true}},
			results: map[string]interface{}{"string1": []byte{2, 3, 4, 5, 6, 7, 8, 9}},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: []byte{245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255}, In: true}},
			results: map[string]interface{}{"string1": []byte{247, 248, 249, 250, 251, 252, 253, 254, 255}},
		},
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: testByteSlice2000, In: true}},
			results: map[string]interface{}{"string1": testByteSlice2000[2:]},
		},
	}

	execResultRawRemoveFront4000 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: testByteSlice4000, In: true}},
			results: map[string]interface{}{"string1": testByteSlice4000[2:]},
		},
	}

	execResultRawRemoveFront16383 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: testByteSlice16383, In: true}},
			results: map[string]interface{}{"string1": testByteSlice16383[2:]},
		},
	}

	execResultRawRemoveFront32767 := []testExecResult{
		testExecResult{
			args:    map[string]sql.Out{"string1": sql.Out{Dest: testByteSlice32767, In: true}},
			results: map[string]interface{}{"string1": testByteSlice32767[2:]},
		},
	}

	// RAW
	execResults.query = `
declare
	function GET_STRING(p_string RAW) return RAW as
	begin
		if p_string is null or UTL_RAW.LENGTH(p_string) < 3 then
			return null;
		end if;
		return UTL_RAW.SUBSTR(p_string, 3);
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultRawRemoveFront2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultRawRemoveFront4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultRawRemoveFront16383
	testRunExecResults(t, execResults)
	execResults.execResults = execResultRawRemoveFront32767
	testRunExecResults(t, execResults)

	// BLOB
	execResults.query = `
declare
	function GET_STRING(p_string BLOB) return BLOB as
	begin
		return DBMS_LOB.SUBSTR(p_string, 32767, 3);
	end GET_STRING;
begin
	:string1 := GET_STRING(:string1);
end;`
	execResults.execResults = execResultRawRemoveFront2000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultRawRemoveFront4000
	testRunExecResults(t, execResults)
	execResults.execResults = execResultRawRemoveFront16383
	testRunExecResults(t, execResults)
	execResults.execResults = execResultRawRemoveFront32767
	testRunExecResults(t, execResults)
}
