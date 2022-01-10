package replayutil

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"github.com/SkyAPM/go2sky"
	"github.com/SkyAPM/go2sky/reporter"
	language_agent "github.com/SkyAPM/go2sky/reporter/grpc/language-agent"
	"github.com/pingcap/tidb/parser/terror"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
)

// RecordReplayer is used to replay sql
var RecordReplayer *recordReplayer

// Sessions is a map
var Sessions map[string]*sessionManager

var (
	tracerKey = struct{}{}
	SwTracer  *go2sky.Tracer
)

func init() {
	swReporter, err := reporter.NewGRPCReporter("127.0.0.1:11800")
	if err != nil {
		terror.Log(err)
		return
	}
	SwTracer, err = go2sky.NewTracer("tidb_replay", go2sky.WithReporter(swReporter))
	if err != nil {
		terror.Log(err)
		return
	}
}

// StartReplay starts replay
func StartReplay(filename string, store kv.Storage, speed float64) {
	fmt.Println("start replaying...")
	RecordReplayer = newRecordPlayer(filename, store, speed)
	go RecordReplayer.start()
}

// StopReplay stops replay
func StopReplay() {
	fmt.Println("stop replaying...")
	if RecordReplayer != nil {
		RecordReplayer.close <- struct{}{}
	}
}

func newRecordPlayer(filename string, store kv.Storage, speed float64) *recordReplayer {
	r := &recordReplayer{
		speed:    speed,
		store:    store,
		fileName: filename,
		close:    make(chan struct{}),
	}
	return r
}

type recordReplayer struct {
	speed    float64
	store    kv.Storage
	close    chan struct{}
	fileName string
	scanner  *bufio.Scanner
}

type sessionManager struct {
	s     session.Session
	sqlCh chan string
	exit  chan int
}

func  (r *recordReplayer)shadowHelper()  {
	unit := false
	metaTS := time.Now().Unix()
	var snapshot string
	if unit {
		snapshot = time.Unix(metaTS/1e6, 0).Format("2006-01-02 15:04:05")
	} else {
		snapshot = time.Unix(metaTS, 0).Format("2006-01-02 15:04:05")
	}
	snapshot = snapshot
	dir, _ := os.Getwd()
	DB, err := sql.Open("mysql", "root@tcp(127.0.0.1:4000)/test")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	sql := fmt.Sprintf("BACKUP DATABASE `test` TO 'local://%s/testbackup';", dir)
	_, err = DB.Exec(sql)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	sql = fmt.Sprintf("RESTORE DATABASE * FROM 'local://%s/testbackup';", dir)
	_, err = DB.Exec(sql)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
}

func (r *recordReplayer) start() {
	fmt.Println("build shadow databases")
	r.shadowHelper()
	return
	f, err := os.OpenFile(r.fileName, os.O_RDONLY, os.ModePerm)
	defer f.Close()
	if err != nil {
		fmt.Printf("Open file error %s\n", err.Error())
		return
	}

	r.scanner = bufio.NewScanner(f)
	Sessions = make(map[string]*sessionManager)
	start := time.Now()
	var exit bool
	var sleepTime float64
	for r.scanner.Scan() {
		select {
		case <-r.close:
			for _, se := range Sessions {
				se.exit <- 1
			}
			exit = true
		default:
		}
		if exit {
			break
		}
		text := r.scanner.Text()
		record := strings.SplitN(text, " ", 5)
		if len(record) < 4 {
			fmt.Printf("invalid sql log %v, len:%d\n", record, len(record))
			continue
		}
		unit := record[1]
		metaTs := record[0]
		ts, _ := strconv.ParseFloat(metaTs, 10)
		switch unit {
		case "s":
			sleepTime = (ts - time.Since(start).Seconds()*1e6) / r.speed
			if sleepTime > 0 {
				time.Sleep(time.Duration(sleepTime/r.speed) * time.Millisecond)
			}
		case "ms":
			sleepTime = ts - time.Since(start).Seconds()
			fmt.Println(sleepTime / r.speed)
			if sleepTime > 0 {
				time.Sleep(time.Duration(sleepTime/r.speed) * time.Second)
			}
		}
		if s, exist := Sessions[record[0]]; !exist {
			se, err := session.CreateSession(r.store)
			if record[2] != "" {
				se.GetSessionVars().CurrentDB = record[2] + "_test"
			}
			sm := &sessionManager{
				s:     se,
				sqlCh: make(chan string, 100),
				exit:  make(chan int),
			}
			if err != nil {
				log.Info("init replay session fail")
				return
			}
			Sessions[record[0]] = sm
			go sm.replay()
			sm.sqlCh <- record[3]
		} else {
			s.sqlCh <- record[3]
		}
	}
}

func (m sessionManager) replay() {
	defer func() {
		fmt.Println("stop replay!")
		close(m.sqlCh)
		close(m.exit)
	}()
	var exit bool
	for {
		if exit {
			break
		}
		select {
		case <-m.exit:
			exit = true
		case sql := <-m.sqlCh:
			m.replayExecuteSQL(sql)
		}
	}
}

func (m *sessionManager) replayExecuteSQL(sql string) error {
	ctx := context.Background()
	ctx = context.WithValue(ctx, tracerKey, SwTracer)
	span, nCtx, err := SwTracer.CreateEntrySpan(ctx, "TiDB/Command/Query", func() (string, error) {
		return "", nil
	})
	if err != nil {
		terror.Log(err)
	} else {
		defer span.End()
		span.Tag("sql", sql)
		span.SetSpanLayer(language_agent.SpanLayer_Database)
		ctx = nCtx
	}

	args := strings.Split(sql, "[arguments: ")
	if len(args) > 1 {
		argument := strings.Split(args[1][:len(args[1])-1], ", ")
		sql = helper(args[0], argument)
	}
	stmts, err := m.s.Parse(ctx, sql)
	if err != nil {
		return err
	}
	for _, stmt := range stmts {
		m.s.ExecuteStmt(ctx, stmt)
	}
	return nil
}

func helper(sql string, args []string) string {
	newsql := ""
	i := 0
	if len(args) > 1 {
		args[0] = args[0][1:]
		args[len(args)-1] = strings.TrimRight(args[len(args)-1], ")")
	}
	for _, b := range []byte(sql) {
		if b == byte('?') {
			newsql += args[i]
			i++
		} else {
			newsql += string(b)
		}
	}
	return newsql
}
