package replayutil

import (
	"bufio"
	"context"
	"fmt"
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

// StartReplay starts replay
func StartReplay(filename string, store kv.Storage) {
	RecordReplayer = newRecordPlayer(filename, store)
	go RecordReplayer.start()
}

// StopReplay stops replay
func StopReplay() {
	RecordReplayer.close <- struct{}{}
}

func newRecordPlayer(filename string, store kv.Storage) *recordReplayer {
	r := &recordReplayer{
		store:    store,
		fileName: filename,
		close:    make(chan struct{}),
	}
	return r
}

type recordReplayer struct {
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

func (r *recordReplayer) start() {
	f, err := os.OpenFile(r.fileName, os.O_RDONLY, os.ModePerm)
	defer f.Close()
	if err != nil {
		fmt.Printf("Open file error %s\n", err.Error())
		return
	}

	r.scanner = bufio.NewScanner(f)
	Sessions = make(map[string]*sessionManager)
	start := time.Now()
	for r.scanner.Scan() {
		select {
		case <-r.close:
			break
		default:
		}
		text := r.scanner.Text()
		record := strings.SplitN(text, " ", 4)
		if len(record) < 4 {
			fmt.Printf("invalid sql log %v, len:%d\n", record, len(record))
			continue
		}
		ts, _ := strconv.ParseFloat(record[1], 10)
		if sleepTime := ts - time.Since(start).Seconds(); sleepTime > 0 {
			time.Sleep(time.Duration(sleepTime) * time.Second)
		}
		if s, exist := Sessions[record[0]]; !exist {
			se, err := session.CreateSession(r.store)
			fmt.Println(record[2])
			if record[2] != "" {
				se.GetSessionVars().CurrentDB = record[2]
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

func (m sessionManager) replay() error {
	defer func() {
		close(m.sqlCh)
		close(m.exit)
	}()
	for {
		select {
		case sql := <-m.sqlCh:
			m.replayExecuteSQL(sql)
		case <-m.exit:
			break
		}
	}
}

func (m *sessionManager) replayExecuteSQL(sql string) error {
	ctx := context.Background()
	args := strings.Split(sql, "[arguments: ")
	if len(args) > 1 {
		argument := strings.Split(args[1][:len(args[1])-1], ", ")
		sql = helper(args[0], argument)
	}
	fmt.Println(sql)
	fmt.Println("Current DB:", m.s.GetSessionVars().CurrentDB)
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
