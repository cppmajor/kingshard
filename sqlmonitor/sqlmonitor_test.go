package sqlmonitor

import (
	"os"
	"testing"
	"time"
)

func testCheckList(t *testing.T, l []string, checkList ...string) {
	if len(l) != len(checkList) {
		t.Fatal("invalid list len", len(l), len(checkList))
	}

	for i := 0; i < len(l); i++ {
		if l[i] != checkList[i] {
			t.Fatal("invalid list item", l[i], i)
		}
	}
}

func insertData(m *SqlMonitor) {
	execInfo_1 := SqlExecInfo{
		DBHost:      "127.0.0.1:3306",
		SchemaName:  "schema_test",
		UserName:    "user_test",
		Sql:         "select a from table where b > 5 and d = 0",
		SeenTime:    time.Now().Unix(),
		ExecTime:    1.1,
		SuccessExec: true,
	}
	execInfo_2 := SqlExecInfo{
		DBHost:      "127.0.0.1:3306",
		SchemaName:  "schema_test",
		UserName:    "user_test",
		Sql:         "select a from table2 where b > 5 and d = 0",
		SeenTime:    time.Now().Unix(),
		ExecTime:    22.2,
		SuccessExec: true,
	}
	execInfo_3 := SqlExecInfo{
		DBHost:      "127.0.0.1:3306",
		SchemaName:  "schema_test3",
		UserName:    "user_test3",
		Sql:         "select a from table where b > 5 and d = 0",
		SeenTime:    time.Now().Unix(),
		ExecTime:    3.3,
		SuccessExec: true,
	}
	execInfo_4 := SqlExecInfo{
		DBHost:      "127.0.0.1:3306",
		SchemaName:  "schema_test",
		UserName:    "user_test",
		Sql:         "select a from table",
		SeenTime:    time.Now().Unix(),
		ExecTime:    4.4,
		SuccessExec: true,
	}
	execInfo_5 := SqlExecInfo{
		DBHost:      "127.0.0.1:3306",
		SchemaName:  "schema_test",
		UserName:    "user_test",
		Sql:         "select a from table",
		SeenTime:    time.Now().Unix(),
		ExecTime:    4.4,
		SuccessExec: false,
	}
	execInfo_6 := SqlExecInfo{
		DBHost:      "127.0.0.1:3306",
		SchemaName:  "schema_test",
		UserName:    "user_test",
		Sql:         "select a from \n table where b > 15 \n and d = 10",
		SeenTime:    time.Now().Unix(),
		ExecTime:    10.1,
		SuccessExec: true,
	}

	for i := 0; i < 10; i++ {
		m.Record(execInfo_1)
		m.Record(execInfo_2)
		m.Record(execInfo_3)
		m.Record(execInfo_4)
		m.Record(execInfo_5)
		m.Record(execInfo_6)
	}
}

func TestSqlMonitor_FullAsyncMode(t *testing.T) {
	var m SqlMonitor
	names, results := m.GetQueryStats(0, 0)
	if nil != names {
		t.Fatal(results, nil)
	}
	if nil != results {
		t.Fatal(results, nil)
	}

	m.Start(fullAsyncMode, 10, 10, "", false)
	names, results = m.GetQueryStats(0, 0)
	testCheckList(t, names,
		"host_addr", "schema_name", "user_name",
		"digest", "digest_text",
		"count_star", "count_err",
		"first_seen", "last_seen",
		"sum_time", "min_time", "max_time",
		"count_1ms", "count_10ms", "count_100ms", "count_1s", "count_5s", "count_others")
	if 0 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	insertData(&m)

	time.Sleep(time.Second)

	names, results = m.GetQueryStats(0, 0)
	testCheckList(t, names,
		"host_addr", "schema_name", "user_name",
		"digest", "digest_text",
		"count_star", "count_err",
		"first_seen", "last_seen",
		"sum_time", "min_time", "max_time",
		"count_1ms", "count_10ms", "count_100ms", "count_1s", "count_5s", "count_others")

	if 4 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetQueryStats(0, 0)
	if 4 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetQueryStats(2, 0)
	if 2 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetQueryStats(2, 4)
	if 0 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetResetQueryStats(1)
	if 1 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetResetQueryStats(0)
	if 3 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetResetQueryStats(0)
	if 0 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	m.Stop()
}

func TestSqlMonitor_AsyncMode(t *testing.T) {
	var m SqlMonitor
	names, results := m.GetQueryStats(0, 0)
	if nil != names {
		t.Fatal(results, nil)
	}
	if nil != results {
		t.Fatal(results, nil)
	}

	m.Start(asyncMode, 10, 10, "", false)
	names, results = m.GetQueryStats(0, 0)
	testCheckList(t, names,
		"host_addr", "schema_name", "user_name",
		"digest", "digest_text",
		"count_star", "count_err",
		"first_seen", "last_seen",
		"sum_time", "min_time", "max_time",
		"count_1ms", "count_10ms", "count_100ms", "count_1s", "count_5s", "count_others")
	if 0 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	insertData(&m)

	time.Sleep(time.Second)

	names, results = m.GetQueryStats(0, 0)
	testCheckList(t, names,
		"host_addr", "schema_name", "user_name",
		"digest", "digest_text",
		"count_star", "count_err",
		"first_seen", "last_seen",
		"sum_time", "min_time", "max_time",
		"count_1ms", "count_10ms", "count_100ms", "count_1s", "count_5s", "count_others")

	if 4 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetQueryStats(0, 0)
	if 4 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetQueryStats(2, 0)
	if 2 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetQueryStats(2, 4)
	if 0 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetResetQueryStats(1)
	if 1 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetResetQueryStats(0)
	if 3 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetResetQueryStats(0)
	if 0 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	m.Stop()
}

func TestSqlMonitor_SyncMode(t *testing.T) {
	var m SqlMonitor
	m.Start(syncMode, 10, 10, "", false)

	insertData(&m)

	names, results := m.GetQueryStats(0, 0)
	testCheckList(t, names,
		"host_addr", "schema_name", "user_name",
		"digest", "digest_text",
		"count_star", "count_err",
		"first_seen", "last_seen",
		"sum_time", "min_time", "max_time",
		"count_1ms", "count_10ms", "count_100ms", "count_1s", "count_5s", "count_others")

	//for _, vs := range results {
	//	for j, value := range vs {
	//		print(names[j], ":", value.(string), " ")
	//	}
	//	println("")
	//}

	if 4 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetQueryStats(0, 0)
	if 4 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetQueryStats(2, 0)
	if 2 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetQueryStats(2, 4)
	if 0 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetResetQueryStats(1)
	if 1 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetResetQueryStats(0)
	if 3 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetResetQueryStats(0)
	if 0 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	m.Stop()
}

func TestSqlMonitor_CacheRecoverFromDisk(t *testing.T) {
	f, err := os.OpenFile("/tmp/sql_cache", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if nil != err {
		t.Fatal("test file:/tmp/sql_cache create failed.")
	}

	f.WriteString(
		"127.0.0.1:3306\tschema_test\tuser_test\tF90D6CBB4E74BE44\t40\t0\t1528619691\t1528619762\t888.000\t22.200\t22.200\t0\t0\t40\t0\t0\t0\tselect a from table2 where b > ? and d = ?\n" +
			"127.0.0.1:3306\tschema_test3\tuser_test3\t487D8BAAAAE115EB\t40\t0\t1528619691\t1528619762\t132.000\t3.300\t3.300\t0\t40\t0\t0\t0\t0\tselect a from table where b > ? and d = ?\n" +
			"127.0.0.1:3306\tschema_test\tuser_test\t7BF5842BC9156479\t80\t40\t1528619691\t1528619762\t352.000\t4.400\t4.400\t0\t80\t0\t0\t0\t0\tselect a from table\n" +
			"127.0.0.1:3306\tschema_test\tuser_test\t487D8BAAAAE115EB\t80\t0\t1528619691\t1528619762\t448.000\t1.100\t10.100\t0\t40\t40\t0\t0\t0\tselect a from table where b > ? and d = ?\n")
	f.Close()

	var m SqlMonitor
	m.Start(syncMode, 10, 10, "/tmp/sql_cache", false)

	names, results := m.GetQueryStats(0, 0)
	testCheckList(t, names,
		"host_addr", "schema_name", "user_name",
		"digest", "digest_text",
		"count_star", "count_err",
		"first_seen", "last_seen",
		"sum_time", "min_time", "max_time",
		"count_1ms", "count_10ms", "count_100ms", "count_1s", "count_5s", "count_others")

	if 4 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	insertData(&m)

	_, results = m.GetQueryStats(0, 0)
	if 4 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetQueryStats(2, 0)
	if 2 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetQueryStats(2, 4)
	if 0 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetResetQueryStats(1)
	if 1 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetResetQueryStats(0)
	if 3 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetResetQueryStats(0)
	if 0 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	m.Stop()
}

func TestSqlMonitor_SetCacheSize(t *testing.T) {
	var m SqlMonitor
	m.Start(syncMode, 10, 10, "", false)

	insertData(&m)

	names, results := m.GetQueryStats(0, 0)
	testCheckList(t, names,
		"host_addr", "schema_name", "user_name",
		"digest", "digest_text",
		"count_star", "count_err",
		"first_seen", "last_seen",
		"sum_time", "min_time", "max_time",
		"count_1ms", "count_10ms", "count_100ms", "count_1s", "count_5s", "count_others")

	if 4 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetQueryStats(0, 0)
	if 4 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	m.SetCacheSize(2)
	_, results = m.GetQueryStats(0, 0)
	if 2 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	insertData(&m)
	m.SetCacheSize(10)
	_, results = m.GetQueryStats(0, 0)
	if 2 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	insertData(&m)
	_, results = m.GetQueryStats(0, 0)
	if 4 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetQueryStats(2, 0)
	if 2 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetQueryStats(2, 4)
	if 0 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetResetQueryStats(1)
	if 1 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetResetQueryStats(0)
	if 3 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetResetQueryStats(0)
	if 0 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	m.Stop()
}

func TestSqlMonitor_SetChanSize(t *testing.T) {
	var m SqlMonitor
	m.Start(asyncMode, 10, 10, "", false)

	insertData(&m)
	time.Sleep(time.Second)

	names, results := m.GetQueryStats(0, 0)
	testCheckList(t, names,
		"host_addr", "schema_name", "user_name",
		"digest", "digest_text",
		"count_star", "count_err",
		"first_seen", "last_seen",
		"sum_time", "min_time", "max_time",
		"count_1ms", "count_10ms", "count_100ms", "count_1s", "count_5s", "count_others")

	if 4 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetQueryStats(0, 0)
	if 4 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	if 10 != cap(m.sqlExecInfoChan) {
		t.Fatal("invalid chan cap", cap(m.sqlExecInfoChan))
	}

	insertData(&m)
	m.SetChanSize(2)
	time.Sleep(time.Second)

	_, results = m.GetQueryStats(0, 0)
	if 4 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	if 2 != cap(m.sqlExecInfoChan) {
		t.Fatal("invalid chan cap", cap(m.sqlExecInfoChan))
	}

	insertData(&m)
	m.SetChanSize(10)
	time.Sleep(time.Second)
	_, results = m.GetQueryStats(0, 0)
	if 4 != len(results) {
		for _, vs := range results {
			for j, value := range vs {
				print(names[j], ":", value.(string), " ")
			}
			println("")
		}
		t.Fatal("invalid results len", len(results))
	}

	insertData(&m)
	time.Sleep(time.Second)
	_, results = m.GetQueryStats(0, 0)
	if 4 != len(results) {
		for _, vs := range results {
			for j, value := range vs {
				print(names[j], ":", value.(string), " ")
			}
			println("")
		}
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetQueryStats(2, 0)
	if 2 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetQueryStats(2, 4)
	if 0 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetResetQueryStats(1)
	if 1 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetResetQueryStats(0)
	if 3 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	_, results = m.GetResetQueryStats(0)
	if 0 != len(results) {
		t.Fatal("invalid results len", len(results))
	}

	m.Stop()
}
