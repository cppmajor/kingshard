package sqlmonitor

import (
	"bufio"
	"fmt"
	"github.com/percona/go-mysql/query"
	"os"
	"strconv"
	"strings"
	"time"
)

type SqlExecInfo struct {
	Sql         string
	SuccessExec bool
	DBHost      string
	SchemaName  string
	UserName    string
	ExecTime    float64
	SeenTime    int64 // unix timestamp, the moment when the query was routed through the proxy
}

type SqlStats struct {
	/*
		hostAddr
			- MySQL host address
		schema_name
			- the schema that is currently being queried
		user_name
			- the username with which the MySQL client connected to ProxySQL
		digest
			- a hexadecimal hash that uniquely represents a query with its parameters stripped
		digest_text
			- the actual text with its parameters stripped
		count_star
			- the total number of times the query has been executed (with different values for the parameters)
		count_err
			- the total number of times the error query
		first_seen
			- unix timestamp, the first moment when the query was routed through the proxy
		last_seen
			- unix timestamp, the last moment (so far) when the query was routed through the proxy
		sum_time
			- the total time in milliseconds spent executing queries of this type.
			  This is particularly useful to figure out where the most time is spent in your application's workload,
			  and provides a good starting point for where to improve
		min_time, max_time
			- the range of durations to expect when executing such a query.
			  min_time is the minimal execution time seen so far, while max_time represents the maximal execution time,
			  both in milliseconds.
		count_1ms, count_10ms, count_100ms, count_1s, count_5s, count_others
			- count_1ms is the number of times the query has been executed in 1 milliseconds (<= 1ms), other are the same,
			  count_10ms : (> 1ms and <= 10ms), count_100ms : (> 10ms and <= 100ms), count_1s : (> 100ms and <= 1s),
			  count_5s : (> 1s and <= 5s), count_others : (> 5s)
	*/

	hostAddr   string
	schemaName string
	userName   string
	digest     string
	digestText string

	countStar  int64
	countError int64

	firstSeen int64
	lastSeen  int64

	sumTime float64
	minTime float64
	maxTime float64

	count1ms    int64
	count10ms   int64
	count100ms  int64
	count1s     int64
	count5s     int64
	countOthers int64
}

/*
	mode full asynchronous:
		Sql chan (when chan full, not block, Sql drop) -> thread (write cache)
	mode asynchronous:
		Sql chan (when chan full, block) -> thread (write cache)
	mode synchronous:
		write cache directly
*/
const (
	fullAsyncMode = iota
	asyncMode
	syncMode
)

const (
	uninited = iota
	inited
	exiting
	exited
	running
)

type SqlMonitor struct {
	sqlExecInfoChan     chan SqlExecInfo
	sqlDigestCache      *LRUCache
	sqlDigestResetCache *LRUCache

	mode        int
	successOnly bool
	dataPath    string
	status      int
}

func (m *SqlMonitor) Start(mode int, cacheSize int, chanSize int, dataPath string, successOnly bool) {
	m.status = inited

	m.successOnly = successOnly
	switch mode {
	case fullAsyncMode:
		m.mode = fullAsyncMode
	case asyncMode:
		m.mode = asyncMode
	case syncMode:
		m.mode = syncMode
	default:
		m.mode = fullAsyncMode
	}

	if 0 == chanSize {
		chanSize = 4096
	}
	if 0 == cacheSize {
		cacheSize = 10000
	} else if -1 == cacheSize { // not limit
		cacheSize = 0
	}

	m.sqlDigestCache = NewLRUCache(cacheSize)
	m.sqlDigestResetCache = NewLRUCache(cacheSize)
	m.sqlExecInfoChan = make(chan SqlExecInfo, chanSize)
	m.dataPath = dataPath

	m.recoverCacheFormFile()

	if syncMode != m.mode {
		go m.statisticsThread()
	}
}

func (m *SqlMonitor) Stop() {
	m.status = exiting
	close(m.sqlExecInfoChan)

	for m.mode != syncMode && exited != m.status {
		time.Sleep(time.Millisecond)
	}

	m.saveCacheToFile()
	m.sqlDigestCache.Clear()
	m.sqlDigestResetCache.Clear()
}

func (m *SqlMonitor) SetSuccessOnly(successOnly bool) {
	m.successOnly = successOnly
}

func (m *SqlMonitor) SetCacheSize(cacheSize int) {
	m.sqlDigestCache.SetMaxEntries(cacheSize)
	m.sqlDigestResetCache.SetMaxEntries(cacheSize)
}

func (m *SqlMonitor) SetChanSize(chanSize int) {
	oldChan := m.sqlExecInfoChan
	m.sqlExecInfoChan = make(chan SqlExecInfo, chanSize)
	close(oldChan)
}

func (m *SqlMonitor) SetCachePath(dataPath string) {
	m.dataPath = dataPath
}

func (m *SqlMonitor) Record(sqlExecInfo SqlExecInfo) bool {
	if m.successOnly && false == sqlExecInfo.SuccessExec {
		return true
	}

	defer func() {
		if e := recover(); e != nil {
			return
		}
	}()

	switch m.mode {
	case fullAsyncMode:
		select {
		case m.sqlExecInfoChan <- sqlExecInfo:
		default:
			return false
		}

	case asyncMode:
		m.sqlExecInfoChan <- sqlExecInfo

	case syncMode:
		m.statistics(&sqlExecInfo)

	default:
		return false
	}

	return true
}

func (m *SqlMonitor) GetQueryStats(limit int, offset int) ([]string, [][]interface{}) {
	if uninited == m.status {
		return nil, nil
	}

	if 0 == limit {
		limit = m.sqlDigestCache.Len()
	}

	return valListToSQLStats(m.sqlDigestCache.GetVals(limit, offset))
}

func (m *SqlMonitor) GetResetQueryStats(limit int) ([]string, [][]interface{}) {
	if uninited == m.status {
		return nil, nil
	}

	if 0 == limit {
		limit = m.sqlDigestResetCache.Len()
	}

	var valList []interface{}

	for i := 0; i < limit; i++ {
		if val := m.sqlDigestResetCache.RemoveOldest(); nil != val {
			valList = append(valList, val)
		} else {
			break
		}
	}

	return valListToSQLStats(valList)
}

func valListToSQLStats(valList []interface{}) ([]string, [][]interface{}) {
	var names []string = []string{
		"host_addr", "schema_name", "user_name",
		"digest", "digest_text",
		"count_star", "count_err",
		"first_seen", "last_seen",
		"sum_time", "min_time", "max_time",
		"count_1ms", "count_10ms", "count_100ms", "count_1s", "count_5s", "count_others"}
	var rows [][]string

	for _, val := range valList {
		rows = append(rows, []string{
			val.(SqlStats).hostAddr,
			val.(SqlStats).schemaName,
			val.(SqlStats).userName,
			val.(SqlStats).digest,
			val.(SqlStats).digestText,
			fmt.Sprintf("%d", val.(SqlStats).countStar),
			fmt.Sprintf("%d", val.(SqlStats).countError),
			fmt.Sprintf("%d", val.(SqlStats).firstSeen),
			fmt.Sprintf("%d", val.(SqlStats).lastSeen),
			fmt.Sprintf("%.3f", val.(SqlStats).sumTime),
			fmt.Sprintf("%.3f", val.(SqlStats).minTime),
			fmt.Sprintf("%.3f", val.(SqlStats).maxTime),
			fmt.Sprintf("%d", val.(SqlStats).count1ms),
			fmt.Sprintf("%d", val.(SqlStats).count10ms),
			fmt.Sprintf("%d", val.(SqlStats).count100ms),
			fmt.Sprintf("%d", val.(SqlStats).count1s),
			fmt.Sprintf("%d", val.(SqlStats).count5s),
			fmt.Sprintf("%d", val.(SqlStats).countOthers),
		})
	}

	var values [][]interface{} = make([][]interface{}, len(rows))
	for i := range rows {
		values[i] = make([]interface{}, len(names))
		for j := range rows[i] {
			values[i][j] = rows[i][j]
		}
	}

	return names, values
}

func (m *SqlMonitor) statisticsThread() {
	m.status = running
	for {
		if sqlExecInfo, ok := <-m.sqlExecInfoChan; ok {
			m.statistics(&sqlExecInfo)
		} else {
			if exiting == m.status {
				break
			}
			time.Sleep(time.Millisecond)
		}
	}

	m.status = exited
}

func (m *SqlMonitor) statistics(sqlExecInfo *SqlExecInfo) {
	sqlDigestText := query.Fingerprint(sqlExecInfo.Sql)
	sqlDigest := query.Id(sqlDigestText)

	addToCache(m.sqlDigestCache, sqlExecInfo, sqlDigest, sqlDigestText)
	addToCache(m.sqlDigestResetCache, sqlExecInfo, sqlDigest, sqlDigestText)
}

func addToCache(sqlDigestCache *LRUCache, sqlExecInfo *SqlExecInfo, sqlDigest string, sqlDigestText string) {
	cacheKey := sqlExecInfo.DBHost + "," + sqlExecInfo.SchemaName + "," + sqlExecInfo.UserName + "," + sqlDigest

	val, ok := sqlDigestCache.Get(cacheKey)
	if !ok {
		newStats := SqlStats{
			hostAddr:   sqlExecInfo.DBHost,
			schemaName: sqlExecInfo.SchemaName,
			userName:   sqlExecInfo.UserName,
			firstSeen:  sqlExecInfo.SeenTime,
			lastSeen:   sqlExecInfo.SeenTime,
			digest:     sqlDigest,
			digestText: sqlDigestText,
		}

		newStats.countStar = 1
		if !sqlExecInfo.SuccessExec {
			newStats.countError = 1
		}

		newStats.maxTime = sqlExecInfo.ExecTime
		newStats.minTime = sqlExecInfo.ExecTime
		newStats.sumTime = sqlExecInfo.ExecTime

		if 1.0 >= sqlExecInfo.ExecTime {
			newStats.count1ms = 1
		} else if 10.0 >= sqlExecInfo.ExecTime {
			newStats.count10ms = 1
		} else if 100.0 >= sqlExecInfo.ExecTime {
			newStats.count100ms = 1
		} else if 1000.0 >= sqlExecInfo.ExecTime {
			newStats.count1s = 1
		} else if 5000.0 >= sqlExecInfo.ExecTime {
			newStats.count5s = 1
		} else if 5000.0 < sqlExecInfo.ExecTime {
			newStats.countOthers = 1
		}

		sqlDigestCache.Add(cacheKey, newStats)
	}

	switch stats := val.(type) {
	case SqlStats:
		if sqlExecInfo.SeenTime > stats.lastSeen {
			stats.lastSeen = sqlExecInfo.SeenTime
		}

		stats.countStar += 1
		if !sqlExecInfo.SuccessExec {
			stats.countError += 1
		}

		if sqlExecInfo.ExecTime > stats.maxTime {
			stats.maxTime = sqlExecInfo.ExecTime
		} else if sqlExecInfo.ExecTime < stats.minTime {
			stats.minTime = sqlExecInfo.ExecTime
		}
		stats.sumTime += sqlExecInfo.ExecTime

		if 1.0 >= sqlExecInfo.ExecTime {
			stats.count1ms += 1
		} else if 10.0 >= sqlExecInfo.ExecTime {
			stats.count10ms += 1
		} else if 100.0 >= sqlExecInfo.ExecTime {
			stats.count100ms += 1
		} else if 1000.0 >= sqlExecInfo.ExecTime {
			stats.count1s += 1
		} else if 5000.0 >= sqlExecInfo.ExecTime {
			stats.count5s += 1
		} else if 5000.0 < sqlExecInfo.ExecTime {
			stats.countOthers += 1
		}

		sqlDigestCache.Add(cacheKey, stats)
	}
}

func (m *SqlMonitor) saveCacheToFile() {
	if uninited == m.status || "" == m.dataPath {
		return
	}

	cacheFile, err := os.OpenFile(m.dataPath, os.O_CREATE|os.O_WRONLY, 0666)
	if nil != err {
		return
	}
	defer cacheFile.Close()

	_, sqlStats := m.GetQueryStats(0, 0)
	for i := len(sqlStats) - 1; i >= 0; i-- {
		stats := sqlStats[i]
		cacheFile.WriteString(fmt.Sprintf(
			"%s\t%s\t%s\t%s\t%s\t"+
				"%s\t%s\t%s\t%s\t%s\t"+
				"%s\t%s\t%s\t%s\t%s\t"+
				"%s\t%s\t%s\n",
			stats[0], stats[1], stats[2], stats[3], stats[5],
			stats[6], stats[7], stats[8], stats[9], stats[10],
			stats[11], stats[12], stats[13], stats[14], stats[15],
			stats[16], stats[17], stats[4],
		))
	}
}

func (m *SqlMonitor) recoverCacheFormFile() {
	if "" == m.dataPath {
		return
	}

	cacheFile, err := os.OpenFile(m.dataPath, os.O_RDONLY, 0666)
	if nil != err {
		return
	}
	defer cacheFile.Close()

	scanner := bufio.NewScanner(cacheFile)
	for scanner.Scan() {
		valueList := strings.Split(scanner.Text(), "\t")
		if 18 != len(valueList) {
			continue
		}
		var stats SqlStats
		stats.hostAddr = valueList[0]
		stats.schemaName = valueList[1]
		stats.userName = valueList[2]
		stats.digest = valueList[3]
		stats.countStar, err = strconv.ParseInt(valueList[4], 10, 64)
		if nil != err {
			continue
		}
		stats.countError, err = strconv.ParseInt(valueList[5], 10, 64)
		if nil != err {
			continue
		}
		stats.firstSeen, err = strconv.ParseInt(valueList[6], 10, 64)
		if nil != err {
			continue
		}
		stats.lastSeen, err = strconv.ParseInt(valueList[7], 10, 64)
		if nil != err {
			continue
		}
		stats.sumTime, err = strconv.ParseFloat(valueList[8], 64)
		if nil != err {
			continue
		}
		stats.minTime, err = strconv.ParseFloat(valueList[9], 64)
		if nil != err {
			continue
		}
		stats.maxTime, err = strconv.ParseFloat(valueList[10], 64)
		if nil != err {
			continue
		}
		stats.count1ms, err = strconv.ParseInt(valueList[11], 10, 64)
		if nil != err {
			continue
		}
		stats.count10ms, err = strconv.ParseInt(valueList[12], 10, 64)
		if nil != err {
			continue
		}
		stats.count100ms, err = strconv.ParseInt(valueList[13], 10, 64)
		if nil != err {
			continue
		}
		stats.count1s, err = strconv.ParseInt(valueList[14], 10, 64)
		if nil != err {
			continue
		}
		stats.count5s, err = strconv.ParseInt(valueList[15], 10, 64)
		if nil != err {
			continue
		}
		stats.countOthers, err = strconv.ParseInt(valueList[16], 10, 64)
		if nil != err {
			continue
		}
		stats.digestText = valueList[17]

		if nil == err {
			cacheKey := stats.hostAddr + "," + stats.schemaName + "," + stats.userName + "," + stats.digest
			m.sqlDigestCache.Add(cacheKey, stats)
		}
	}
}
