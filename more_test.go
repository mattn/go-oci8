package oci8

// ( . oracle.sh ;DSN='user:pass@:0/(description=(address_list=(address=(protocol=tcp)(host=192.168.1.1)(port=1521)))(connect_data=(sid=SID)))?isolation=SERIALIZABLE'  go test )

import (
	"bytes"
	"database/sql"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"
)

type dbc interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Prepare(query string) (*sql.Stmt, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

func TestTruncate(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}
	_, err := TestDB.Exec("truncate table foo")
	if err != nil {
		panic(err)
	}
}

func sqlstest(d dbc, t *testing.T, sql string, p ...interface{}) map[string]interface{} {
	rows, err := NewS(d.Query(sql, p...))
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		rows.Close()
		t.Fatal("no row returned:", rows.Err())
	}
	err = rows.Scan()
	if err != nil {
		rows.Close()
		t.Fatal(err)
	}
	res := rows.Map()
	//res := rows.Row()
	err = rows.Close()
	if err != nil {
		rows.Close()
		t.Fatal(err)
	}
	return res
}

func sqlstestv(d dbc, t *testing.T, sql string, p ...interface{}) []interface{} {
	rows, err := NewS(d.Query(sql, p...))
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		rows.Close()
		t.Fatal("no row returned:", rows.Err())
	}
	err = rows.Scan()
	if err != nil {
		rows.Close()
		t.Fatal(err)
	}
	//res := rows.Map()
	res := rows.Row()
	err = rows.Close()
	if err != nil {
		rows.Close()
		t.Fatal(err)
	}
	return res
}

func TestInterval1(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}
	n := time.Duration(1234567898123456789)
	r := sqlstest(TestDB, t, "select NUMTODSINTERVAL( :0 / 1000000000, 'SECOND') as intervalds from dual", int64(n))
	if n != time.Duration(r["INTERVALDS"].(int64)) {
		t.Fatal(r, "!=", n)
	}
}

func TestInterval2(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}
	n := time.Duration(-1234567898123456789)
	r := sqlstest(TestDB, t, "select NUMTODSINTERVAL( :0 / 1000000000, 'SECOND') as intervalds from dual", int64(n))
	if n != time.Duration(r["INTERVALDS"].(int64)) {
		t.Fatal(r, "!=", n)
	}
}

func TestInterval3(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}
	n := int64(1234567890)
	r := sqlstest(TestDB, t, "select NUMTOYMINTERVAL( :0, 'MONTH') as intervalym from dual", n)
	if n != r["INTERVALYM"].(int64) {
		t.Fatal(r, "!=", n)
	}
}

func TestInterval4(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}
	n := int64(-1234567890)
	r := sqlstest(TestDB, t, "select NUMTOYMINTERVAL( :0, 'MONTH') as intervalym from dual", n)
	if n != r["INTERVALYM"].(int64) {
		t.Fatal(r, "!=", n)
	}
}

func TestIntervals5(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}
	n1 := time.Duration(987)
	n2 := time.Duration(-65)
	n3 := int64(4332)
	n4 := int64(-1239872)
	r := sqlstest(TestDB, t, "select NUMTODSINTERVAL( :0 / 1000000000, 'SECOND') as i1, NUMTODSINTERVAL( :1 / 1000000000, 'SECOND') as i2, NUMTOYMINTERVAL( :2, 'MONTH') as i3, NUMTOYMINTERVAL( :3, 'MONTH') as i4 from dual", n1, n2, n3, n4)
	if n1 != time.Duration(r["I1"].(int64)) {
		t.Fatal(r["I1"], "!=", n1)
	}
	if n2 != time.Duration(r["I2"].(int64)) {
		t.Fatal(r["I2"], "!=", n2)
	}
	if n3 != r["I3"].(int64) {
		t.Fatal(r["I3"], "!=", n3)
	}
	if n4 != r["I4"].(int64) {
		t.Fatal(r["I4"], "!=", n4)
	}
}

func TestQuestionMark(t *testing.T) {
	// skip for now
	t.SkipNow()
	a, b := 4, 5
	c := "zz"
	r := sqlstest(TestDB, t, "select ? as v1, ? as v2, ? as v3 from dual", a, b, c)
	if fmt.Sprintf("%v", r["V1"]) != fmt.Sprintf("%v", a) {
		t.Fatal(r["V1"], "!=", a)
	}
	if fmt.Sprintf("%v", r["V2"]) != fmt.Sprintf("%v", b) {
		t.Fatal(r["V2"], "!=", b)
	}
	if fmt.Sprintf("%v", r["V3"]) != fmt.Sprintf("%v", c) {
		t.Fatal(r["V3"], "!=", c)
	}
}

func TestString3(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}
	//n := "こんにちは 世界 Καλημέρα κόσμε こんにちは안녕하세요góðan dagGrüßgotthyvää päivääyá'át'ééhΓεια σαςВiтаюგამარჯობაनमस्ते你好здравейсвят"
	//this test depends of database charset !!!!
	n := "здравейсвят"
	r := sqlstest(TestDB, t, "select :0 as str from dual", n)
	if n != r["STR"].(string) {
		t.Fatal(r["STR"], "!=", n)
	}
}

func TestFooLargeBlob(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}
	n := make([]byte, 600000)
	for i := 0; i < len(n); i++ {
		n[i] = byte(rand.Int31n(256))
	}

	id := "idlblob"
	TestDB.Exec("insert into foo( c21, cend) values( :1, :2)", n, id)

	r := sqlstest(TestDB, t, "select c21 from foo where cend= :1", id)
	if !bytes.Equal(n, r["C21"].([]byte)) {
		t.Fatal(r["C21"], "!=", n)
	}
}

func TestSmallBlob(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}
	n := make([]byte, 6)
	for i := 0; i < len(n); i++ {
		n[i] = byte(rand.Int31n(256))
	}

	id := "idsblob"
	TestDB.Exec("insert into foo( c21, cend) values( :1, :2)", n, id)

	r := sqlstest(TestDB, t, "select c21 from foo where cend=:1", id)
	if !bytes.Equal(n, r["C21"].([]byte)) {
		t.Fatal(r["C21"], "!=", n)
	}
}

func TestFooRowid(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}
	n := "Z"
	id := "idSmallClob"
	_, e := TestDB.Exec("insert into foo( c19, cend) values( :1, :2)", n, id)
	if e != nil {
		t.Fatal(e)
	}

	sqlstest(TestDB, t, "select rowid from foo")
}

//this test fail if transactions are readonly
func TestTransaction1(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}
	tx, e := TestDB.Begin()
	if e != nil {
		t.Fatal(e)
	}

	_, e = tx.Exec("insert into foo( c1) values( :1)", "123abc")
	if e != nil {
		t.Fatal(e)
	}

	_, e = tx.Exec("update foo set c1='ertertetert'")
	if e != nil {
		t.Fatal(e)
	}

	e = tx.Commit()
	if e != nil {
		t.Fatal(e)
	}
}

func TestBigClob(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}
	n := "Abc" + strings.Repeat("1234567890", 2000) + "xyZ"

	id := "idBigClob"
	TestDB.Exec("insert into foo( c19, cend) values( :1, :2)", n, id)

	r := sqlstest(TestDB, t, "select c19 from foo where cend= :1", id)
	if n != r["C19"].(string) {
		println(3)
		t.Fatal(r["C19"], "!=", n)
	}
}

func TestSmallClob(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}
	n := "Z"
	id := "idSmallClob"
	TestDB.Exec("insert into foo( c19, cend) values( :1, :2)", n, id)

	r := sqlstest(TestDB, t, "select c19 from foo where cend= :1", id)
	if n != r["C19"].(string) {
		t.Fatal(r["C19"], "!=", n)
	}
}

func TestNvarchar(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}
	n := "Zкирddd"
	id := "idNvarchar"
	TestDB.Exec("insert into foo( c2, cend) values( :1, :2)", n, id)

	r := sqlstest(TestDB, t, "select c2 from foo where cend= :1", id)
	if n != r["C2"].(string) {
		t.Fatal(r["C2"], "!=", n)
	}
}

func TestNchar(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}
	f := "C18"
	n := "XXкирda"
	id := "idbdNC18"
	TestDB.Exec("insert into foo( "+f+", cend) values( :1, :2)", n, id)

	r := sqlstest(TestDB, t, "select "+f+" from foo where cend= :1", id)
	if strings.TrimRight(n, " ") != strings.TrimRight(r[f].(string), " ") {
		t.Fatal(r[f], "!=", n)
	}
}

func TestChar(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}
	f := "C17"
	n := "XXкирda"
	id := "idbdC17"
	TestDB.Exec("insert into foo( "+f+", cend) values( :1, :2)", n, id)

	r := sqlstest(TestDB, t, "select "+f+" from foo where cend= :1", id)
	if strings.TrimRight(n, " ") != strings.TrimRight(r[f].(string), " ") {
		t.Fatal(r[f], "!=", n)
	}
}

func TestDate(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}
	f := "C6"

	const fm = "2006-01-02 15:04:05.999999999 -07:00"

	n, err := time.Parse(fm, "2014-10-23 04:56:12.123456789 +09:06")
	if err != nil {
		t.Fatal(err)
	}

	id := "idbdate" + f
	TestDB.Exec("insert into foo( "+f+", cend) values( :1, :2)", n, id)

	sqlstest(TestDB, t, "select "+f+" from foo where cend= :1", id)
}

func TestTimestamp(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}
	f := "C9"

	const fm = "2006-01-02 15:04:05.999999999 -07:00"

	n, err := time.Parse(fm, "2014-10-23 04:56:12.123456789 +09:06")
	if err != nil {
		t.Fatal(err)
	}

	id := "idTstamp" + f
	TestDB.Exec("insert into foo( "+f+", cend) values( :1, :2)", n, id)

	sqlstest(TestDB, t, "select "+f+" from foo where cend= :1", id)
}

func TestTimestampTz(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}
	f := "C10"

	const fm = "2006-01-02 15:04:05.999999999 -07:00"

	n, err := time.Parse(fm, "2014-10-23 04:56:12.123456789 +09:06")
	if err != nil {
		t.Fatal(err)
	}

	id := "idTs" + f
	TestDB.Exec("insert into foo( "+f+", cend) values( :1, :2)", n, id)

	sqlstest(TestDB, t, "select "+f+" from foo where cend= :1", id)
}

func TestTimestampLtz(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}
	f := "C11"

	const fm = "2006-01-02 15:04:05.999999999 -07:00"

	n, err := time.Parse(fm, "2014-10-23 04:56:12.123456000 +09:06")
	if err != nil {
		t.Fatal(err)
	}

	id := "idTs" + f
	TestDB.Exec("insert into foo( "+f+", cend) values( :1, :2)", n, id)

	sqlstest(TestDB, t, "select "+f+" from foo where cend= :1", id)
}

func TestTimeZones(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}
	zones := getZones()
	db := TestDB
	seen := make(map[string]bool, len(zones)*2)
	for _, z0 := range zones {
		loc, err := time.LoadLocation(z0)
		z1 := loc.String() // for me z0 is always == z1

		if err != nil {
			continue
		}
		tt := time.Date(2015, 12, 31, 23, 59, 59, 123456789, loc)
		z2, _ := tt.Zone() // sometimes z1 != z2 e.g. EST5EDT v.s. EDT

		if _, exists := seen[z0]; !exists {
			seen[z0] = true
			handleZone(z0, &tt, db, t)
		}
		if _, exists := seen[z1]; !exists {
			seen[z1] = true
			handleZone(z1, &tt, db, t)
		}
		if _, exists := seen[z2]; !exists {
			seen[z2] = true
			handleZone(z2, &tt, db, t)
		}
	}
}

func handleZone(zone string, tt *time.Time, db *sql.DB, t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}
	r := sqlstest(TestDB, t, "select :0 as time from dual", *tt)
	if !tt.Equal(r["TIME"].(time.Time)) {
		t.Fatal(r, "!=", tt)
	}
}

func getZones() []string {
	// mkdir foo; cd foo; unzip $GOROOT/lib/time/zoneinfo.zip; find . -type f | sed 's/^../"/;s/$/",/' | grep -v '^"Factory",$' | sort -u | fmt -w 80 > ../zone.names; cd ..; rm -rf foo
	return []string{
		"Africa/Abidjan", "Africa/Accra", "Africa/Addis_Ababa", "Africa/Algiers",
		"Africa/Asmara", "Africa/Asmera", "Africa/Bamako", "Africa/Bangui",
		"Africa/Banjul", "Africa/Bissau", "Africa/Blantyre", "Africa/Brazzaville",
		"Africa/Bujumbura", "Africa/Cairo", "Africa/Casablanca", "Africa/Ceuta",
		"Africa/Conakry", "Africa/Dakar", "Africa/Dar_es_Salaam", "Africa/Djibouti",
		"Africa/Douala", "Africa/El_Aaiun", "Africa/Freetown", "Africa/Gaborone",
		"Africa/Harare", "Africa/Johannesburg", "Africa/Juba", "Africa/Kampala",
		"Africa/Khartoum", "Africa/Kigali", "Africa/Kinshasa", "Africa/Lagos",
		"Africa/Libreville", "Africa/Lome", "Africa/Luanda", "Africa/Lubumbashi",
		"Africa/Lusaka", "Africa/Malabo", "Africa/Maputo", "Africa/Maseru",
		"Africa/Mbabane", "Africa/Mogadishu", "Africa/Monrovia", "Africa/Nairobi",
		"Africa/Ndjamena", "Africa/Niamey", "Africa/Nouakchott", "Africa/Ouagadougou",
		"Africa/Porto-Novo", "Africa/Sao_Tome", "Africa/Timbuktu", "Africa/Tripoli",
		"Africa/Tunis", "Africa/Windhoek", "America/Adak", "America/Anchorage",
		"America/Anguilla", "America/Antigua", "America/Araguaina",
		"America/Argentina/Buenos_Aires", "America/Argentina/Catamarca",
		"America/Argentina/ComodRivadavia", "America/Argentina/Cordoba",
		"America/Argentina/Jujuy", "America/Argentina/La_Rioja",
		"America/Argentina/Mendoza", "America/Argentina/Rio_Gallegos",
		"America/Argentina/Salta", "America/Argentina/San_Juan",
		"America/Argentina/San_Luis", "America/Argentina/Tucuman",
		"America/Argentina/Ushuaia", "America/Aruba", "America/Asuncion",
		"America/Atikokan", "America/Atka", "America/Bahia", "America/Bahia_Banderas",
		"America/Barbados", "America/Belem", "America/Belize", "America/Blanc-Sablon",
		"America/Boa_Vista", "America/Bogota", "America/Boise", "America/Buenos_Aires",
		"America/Cambridge_Bay", "America/Campo_Grande", "America/Cancun",
		"America/Caracas", "America/Catamarca", "America/Cayenne", "America/Cayman",
		"America/Chicago", "America/Chihuahua", "America/Coral_Harbour",
		"America/Cordoba", "America/Costa_Rica", "America/Creston",
		"America/Cuiaba", "America/Curacao", "America/Danmarkshavn",
		"America/Dawson", "America/Dawson_Creek", "America/Denver",
		"America/Detroit", "America/Dominica", "America/Edmonton",
		"America/Eirunepe", "America/El_Salvador", "America/Ensenada",
		"America/Fortaleza", "America/Fort_Wayne", "America/Glace_Bay",
		"America/Godthab", "America/Goose_Bay", "America/Grand_Turk",
		"America/Grenada", "America/Guadeloupe", "America/Guatemala",
		"America/Guayaquil", "America/Guyana", "America/Halifax", "America/Havana",
		"America/Hermosillo", "America/Indiana/Indianapolis", "America/Indiana/Knox",
		"America/Indiana/Marengo", "America/Indiana/Petersburg",
		"America/Indianapolis", "America/Indiana/Tell_City", "America/Indiana/Vevay",
		"America/Indiana/Vincennes", "America/Indiana/Winamac", "America/Inuvik",
		"America/Iqaluit", "America/Jamaica", "America/Jujuy", "America/Juneau",
		"America/Kentucky/Louisville", "America/Kentucky/Monticello",
		"America/Knox_IN", "America/Kralendijk", "America/La_Paz", "America/Lima",
		"America/Los_Angeles", "America/Louisville", "America/Lower_Princes",
		"America/Maceio", "America/Managua", "America/Manaus", "America/Marigot",
		"America/Martinique", "America/Matamoros", "America/Mazatlan",
		"America/Mendoza", "America/Menominee", "America/Merida", "America/Metlakatla",
		"America/Mexico_City", "America/Miquelon", "America/Moncton",
		"America/Monterrey", "America/Montevideo", "America/Montreal",
		"America/Montserrat", "America/Nassau", "America/New_York", "America/Nipigon",
		"America/Nome", "America/Noronha", "America/North_Dakota/Beulah",
		"America/North_Dakota/Center", "America/North_Dakota/New_Salem",
		"America/Ojinaga", "America/Panama", "America/Pangnirtung",
		"America/Paramaribo", "America/Phoenix", "America/Port-au-Prince",
		"America/Porto_Acre", "America/Port_of_Spain", "America/Porto_Velho",
		"America/Puerto_Rico", "America/Rainy_River", "America/Rankin_Inlet",
		"America/Recife", "America/Regina", "America/Resolute", "America/Rio_Branco",
		"America/Rosario", "America/Santa_Isabel", "America/Santarem",
		"America/Santiago", "America/Santo_Domingo", "America/Sao_Paulo",
		"America/Scoresbysund", "America/Shiprock", "America/Sitka",
		"America/St_Barthelemy", "America/St_Johns", "America/St_Kitts",
		"America/St_Lucia", "America/St_Thomas", "America/St_Vincent",
		"America/Swift_Current", "America/Tegucigalpa", "America/Thule",
		"America/Thunder_Bay", "America/Tijuana", "America/Toronto",
		"America/Tortola", "America/Vancouver", "America/Virgin",
		"America/Whitehorse", "America/Winnipeg", "America/Yakutat",
		"America/Yellowknife", "Antarctica/Casey", "Antarctica/Davis",
		"Antarctica/DumontDUrville", "Antarctica/Macquarie", "Antarctica/Mawson",
		"Antarctica/McMurdo", "Antarctica/Palmer", "Antarctica/Rothera",
		"Antarctica/South_Pole", "Antarctica/Syowa", "Antarctica/Troll",
		"Antarctica/Vostok", "Arctic/Longyearbyen", "Asia/Aden", "Asia/Almaty",
		"Asia/Amman", "Asia/Anadyr", "Asia/Aqtau", "Asia/Aqtobe", "Asia/Ashgabat",
		"Asia/Ashkhabad", "Asia/Baghdad", "Asia/Bahrain", "Asia/Baku", "Asia/Bangkok",
		"Asia/Beirut", "Asia/Bishkek", "Asia/Brunei", "Asia/Calcutta", "Asia/Chita",
		"Asia/Choibalsan", "Asia/Chongqing", "Asia/Chungking", "Asia/Colombo",
		"Asia/Dacca", "Asia/Damascus", "Asia/Dhaka", "Asia/Dili", "Asia/Dubai",
		"Asia/Dushanbe", "Asia/Gaza", "Asia/Harbin", "Asia/Hebron", "Asia/Ho_Chi_Minh",
		"Asia/Hong_Kong", "Asia/Hovd", "Asia/Irkutsk", "Asia/Istanbul",
		"Asia/Jakarta", "Asia/Jayapura", "Asia/Jerusalem", "Asia/Kabul",
		"Asia/Kamchatka", "Asia/Karachi", "Asia/Kashgar", "Asia/Kathmandu",
		"Asia/Katmandu", "Asia/Khandyga", "Asia/Kolkata", "Asia/Krasnoyarsk",
		"Asia/Kuala_Lumpur", "Asia/Kuching", "Asia/Kuwait", "Asia/Macao", "Asia/Macau",
		"Asia/Magadan", "Asia/Makassar", "Asia/Manila", "Asia/Muscat", "Asia/Nicosia",
		"Asia/Novokuznetsk", "Asia/Novosibirsk", "Asia/Omsk", "Asia/Oral",
		"Asia/Phnom_Penh", "Asia/Pontianak", "Asia/Pyongyang", "Asia/Qatar",
		"Asia/Qyzylorda", "Asia/Rangoon", "Asia/Riyadh", "Asia/Saigon",
		"Asia/Sakhalin", "Asia/Samarkand", "Asia/Seoul", "Asia/Shanghai",
		"Asia/Singapore", "Asia/Srednekolymsk", "Asia/Taipei", "Asia/Tashkent",
		"Asia/Tbilisi", "Asia/Tehran", "Asia/Tel_Aviv", "Asia/Thimbu", "Asia/Thimphu",
		"Asia/Tokyo", "Asia/Ujung_Pandang", "Asia/Ulaanbaatar", "Asia/Ulan_Bator",
		"Asia/Urumqi", "Asia/Ust-Nera", "Asia/Vientiane", "Asia/Vladivostok",
		"Asia/Yakutsk", "Asia/Yekaterinburg", "Asia/Yerevan", "Atlantic/Azores",
		"Atlantic/Bermuda", "Atlantic/Canary", "Atlantic/Cape_Verde",
		"Atlantic/Faeroe", "Atlantic/Faroe", "Atlantic/Jan_Mayen", "Atlantic/Madeira",
		"Atlantic/Reykjavik", "Atlantic/South_Georgia", "Atlantic/Stanley",
		"Atlantic/St_Helena", "Australia/ACT", "Australia/Adelaide",
		"Australia/Brisbane", "Australia/Broken_Hill", "Australia/Canberra",
		"Australia/Currie", "Australia/Darwin", "Australia/Eucla", "Australia/Hobart",
		"Australia/LHI", "Australia/Lindeman", "Australia/Lord_Howe",
		"Australia/Melbourne", "Australia/North", "Australia/NSW", "Australia/Perth",
		"Australia/Queensland", "Australia/South", "Australia/Sydney",
		"Australia/Tasmania", "Australia/Victoria", "Australia/West",
		"Australia/Yancowinna", "Brazil/Acre", "Brazil/DeNoronha", "Brazil/East",
		"Brazil/West", "Canada/Atlantic", "Canada/Central", "Canada/Eastern",
		"Canada/East-Saskatchewan", "Canada/Mountain", "Canada/Newfoundland",
		"Canada/Pacific", "Canada/Saskatchewan", "Canada/Yukon", "CET",
		"Chile/Continental", "Chile/EasterIsland", "CST6CDT", "Cuba", "EET",
		"Egypt", "Eire", "EST", "EST5EDT", "Etc/GMT", "Etc/GMT-0", "Etc/GMT+0",
		"Etc/GMT0", "Etc/GMT-1", "Etc/GMT+1", "Etc/GMT-10", "Etc/GMT+10",
		"Etc/GMT-11", "Etc/GMT+11", "Etc/GMT-12", "Etc/GMT+12", "Etc/GMT-13",
		"Etc/GMT-14", "Etc/GMT-2", "Etc/GMT+2", "Etc/GMT-3", "Etc/GMT+3", "Etc/GMT-4",
		"Etc/GMT+4", "Etc/GMT-5", "Etc/GMT+5", "Etc/GMT-6", "Etc/GMT+6", "Etc/GMT-7",
		"Etc/GMT+7", "Etc/GMT-8", "Etc/GMT+8", "Etc/GMT-9", "Etc/GMT+9",
		"Etc/Greenwich", "Etc/UCT", "Etc/Universal", "Etc/UTC", "Etc/Zulu",
		"Europe/Amsterdam", "Europe/Andorra", "Europe/Athens", "Europe/Belfast",
		"Europe/Belgrade", "Europe/Berlin", "Europe/Bratislava", "Europe/Brussels",
		"Europe/Bucharest", "Europe/Budapest", "Europe/Busingen", "Europe/Chisinau",
		"Europe/Copenhagen", "Europe/Dublin", "Europe/Gibraltar", "Europe/Guernsey",
		"Europe/Helsinki", "Europe/Isle_of_Man", "Europe/Istanbul", "Europe/Jersey",
		"Europe/Kaliningrad", "Europe/Kiev", "Europe/Lisbon", "Europe/Ljubljana",
		"Europe/London", "Europe/Luxembourg", "Europe/Madrid", "Europe/Malta",
		"Europe/Mariehamn", "Europe/Minsk", "Europe/Monaco", "Europe/Moscow",
		"Europe/Nicosia", "Europe/Oslo", "Europe/Paris", "Europe/Podgorica",
		"Europe/Prague", "Europe/Riga", "Europe/Rome", "Europe/Samara",
		"Europe/San_Marino", "Europe/Sarajevo", "Europe/Simferopol", "Europe/Skopje",
		"Europe/Sofia", "Europe/Stockholm", "Europe/Tallinn", "Europe/Tirane",
		"Europe/Tiraspol", "Europe/Uzhgorod", "Europe/Vaduz", "Europe/Vatican",
		"Europe/Vienna", "Europe/Vilnius", "Europe/Volgograd", "Europe/Warsaw",
		"Europe/Zagreb", "Europe/Zaporozhye", "Europe/Zurich", "GB", "GB-Eire",
		"GMT", "GMT-0", "GMT+0", "GMT0", "Greenwich", "Hongkong", "HST", "Iceland",
		"Indian/Antananarivo", "Indian/Chagos", "Indian/Christmas", "Indian/Cocos",
		"Indian/Comoro", "Indian/Kerguelen", "Indian/Mahe", "Indian/Maldives",
		"Indian/Mauritius", "Indian/Mayotte", "Indian/Reunion", "Iran", "Israel",
		"Jamaica", "Japan", "Kwajalein", "Libya", "MET", "Mexico/BajaNorte",
		"Mexico/BajaSur", "Mexico/General", "MST", "MST7MDT", "Navajo", "NZ",
		"NZ-CHAT", "Pacific/Apia", "Pacific/Auckland", "Pacific/Bougainville",
		"Pacific/Chatham", "Pacific/Chuuk", "Pacific/Easter", "Pacific/Efate",
		"Pacific/Enderbury", "Pacific/Fakaofo", "Pacific/Fiji", "Pacific/Funafuti",
		"Pacific/Galapagos", "Pacific/Gambier", "Pacific/Guadalcanal", "Pacific/Guam",
		"Pacific/Honolulu", "Pacific/Johnston", "Pacific/Kiritimati", "Pacific/Kosrae",
		"Pacific/Kwajalein", "Pacific/Majuro", "Pacific/Marquesas", "Pacific/Midway",
		"Pacific/Nauru", "Pacific/Niue", "Pacific/Norfolk", "Pacific/Noumea",
		"Pacific/Pago_Pago", "Pacific/Palau", "Pacific/Pitcairn", "Pacific/Pohnpei",
		"Pacific/Ponape", "Pacific/Port_Moresby", "Pacific/Rarotonga",
		"Pacific/Saipan", "Pacific/Samoa", "Pacific/Tahiti", "Pacific/Tarawa",
		"Pacific/Tongatapu", "Pacific/Truk", "Pacific/Wake", "Pacific/Wallis",
		"Pacific/Yap", "Poland", "Portugal", "PRC", "PST8PDT", "ROC", "ROK",
		"Singapore", "Turkey", "UCT", "Universal", "US/Alaska", "US/Aleutian",
		"US/Arizona", "US/Central", "US/Eastern", "US/East-Indiana", "US/Hawaii",
		"US/Indiana-Starke", "US/Michigan", "US/Mountain", "US/Pacific",
		"US/Pacific-New", "US/Samoa", "UTC", "WET", "W-SU", "Zulu",
	}
}

func TestColumnTypeScanType(t *testing.T) {
	if TestDisableDatabase {
		t.SkipNow()
	}
	timeVar := time.Date(2015, 12, 31, 23, 59, 59, 123456789, time.UTC)
	intVar := int64(0)
	floatVar := float64(0)

	rows, err := TestDB.Query("select :0 as int64 ,:1 as float64 , :2 as time from dual",
		&intVar, &floatVar, &timeVar)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	ct, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range ct {
		switch c.Name() {
		case "INT64":
			if c.ScanType() != reflect.TypeOf(intVar) {
				t.Fatalf("scan type error, expect %v, get %v", reflect.TypeOf(intVar), c.ScanType())
			}
		case "TIME":
			if c.ScanType() != reflect.TypeOf(timeVar) {
				t.Fatalf("scan type error, expect %v, get %v", reflect.TypeOf(timeVar), c.ScanType())
			}
		case "FLOAT64":
			if c.ScanType() != reflect.TypeOf(floatVar) {
				t.Fatalf("scan type error, expect %v, get %v", reflect.TypeOf(floatVar), c.ScanType())
			}
		}
	}
}
