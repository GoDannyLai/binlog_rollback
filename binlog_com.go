package main

import (
	"path/filepath"
	//"fmt"
	"sync"

	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	sliceKits "github.com/toolkits/slice"
)

type BinEventHandlingIndx struct {
	EventIdx uint64
	lock     sync.RWMutex
	finished bool
}

var G_HandlingBinEventIndex *BinEventHandlingIndx

type MaxBinEventIdx struct {
	MaxEventIdx uint64
	lock        sync.RWMutex
}

func (this *MaxBinEventIdx) SetMaxBinEventIdx(idx uint64) {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.MaxEventIdx = idx
}

var g_MaxBin_Event_Idx *MaxBinEventIdx

type BinEventDbTableQuery struct {
	database string
	table    string
	sql      string
}

type MyBinEvent struct {
	MyPos       mysql.Position //this is the end position
	EventIdx    uint64
	BinEvent    *replication.RowsEvent
	StartPos    uint32 // this is the start position
	IfRowsEvent bool
	SqlType     string // insert, update, delete
	Timestamp   uint32
	TrxIndex    uint64
	TrxStatus   int // 0:begin, 1: commit, 2: rollback, -1: in_progress
}

func CheckBinHeaderCondition(cfg *ConfCmd, header *replication.EventHeader, currentBinlog *string) int {
	// process: 0, continue: 1, break: 2

	myPos := mysql.Position{Name: *currentBinlog, Pos: header.LogPos}
	//fmt.Println(cfg.StartFilePos, cfg.IfSetStopFilePos, myPos)
	if cfg.IfSetStartFilePos {
		cmpRe := myPos.Compare(cfg.StartFilePos)
		if cmpRe == -1 {
			return C_reContinue
		}
	}

	if cfg.IfSetStopFilePos {
		cmpRe := myPos.Compare(cfg.StopFilePos)
		if cmpRe == 1 {
			return C_reBreak
		}
	}
	//fmt.Println(cfg.StartDatetime, cfg.StopDatetime, header.Timestamp)
	if cfg.IfSetStartDateTime {

		if header.Timestamp < cfg.StartDatetime {
			return C_reContinue
		}
	}

	if cfg.IfSetStopDateTime {
		if header.Timestamp > cfg.StopDatetime {
			return C_reBreak
		}
	}
	if cfg.FilterSqlLen == 0 {
		return C_reProcess
	}

	if header.EventType == replication.WRITE_ROWS_EVENTv1 || header.EventType == replication.WRITE_ROWS_EVENTv2 {
		if sliceKits.ContainsString(cfg.FilterSql, "insert") {
			return C_reProcess
		} else {
			return C_reContinue
		}
	}

	if header.EventType == replication.UPDATE_ROWS_EVENTv1 || header.EventType == replication.UPDATE_ROWS_EVENTv2 {
		if sliceKits.ContainsString(cfg.FilterSql, "update") {
			return C_reProcess
		} else {
			return C_reContinue
		}
	}

	if header.EventType == replication.DELETE_ROWS_EVENTv1 || header.EventType == replication.DELETE_ROWS_EVENTv2 {
		if sliceKits.ContainsString(cfg.FilterSql, "delete") {
			return C_reProcess
		} else {
			return C_reContinue
		}
	}

	return C_reProcess
}

func (this *MyBinEvent) CheckBinEvent(cfg *ConfCmd, ev *replication.BinlogEvent, currentBinlog *string) int {

	myPos := mysql.Position{Name: *currentBinlog, Pos: ev.Header.LogPos}
	switch ev.Header.EventType {
	case replication.ROTATE_EVENT:
		rotatEvent := ev.Event.(*replication.RotateEvent)
		*currentBinlog = string(rotatEvent.NextLogName)
		if cfg.ToLastLog && cfg.Mode == "repl" && cfg.WorkType == "stats" {
			return C_reContinue
		}
		myPos.Name = string(rotatEvent.NextLogName)
		myPos.Pos = uint32(rotatEvent.Position)
		if cfg.IfSetStartFilePos {
			cmpRe := myPos.Compare(cfg.StartFilePos)
			if cmpRe == -1 {
				return C_reContinue
			}
		}

		if cfg.IfSetStopFilePos {
			cmpRe := myPos.Compare(cfg.StopFilePos)
			if cmpRe == 1 {
				return C_reBreak
			}
		}
		this.IfRowsEvent = false

	case replication.WRITE_ROWS_EVENTv1,
		replication.UPDATE_ROWS_EVENTv1,
		replication.DELETE_ROWS_EVENTv1,
		replication.WRITE_ROWS_EVENTv2,
		replication.UPDATE_ROWS_EVENTv2,
		replication.DELETE_ROWS_EVENTv2:

		//replication.XID_EVENT,
		//replication.TABLE_MAP_EVENT:

		wrEvent := ev.Event.(*replication.RowsEvent)
		db := string(wrEvent.Table.Schema)
		tb := string(wrEvent.Table.Table)
		if len(cfg.Databases) > 0 {
			if !sliceKits.ContainsString(cfg.Databases, db) {
				return C_reContinue
			}
		}
		if len(cfg.Tables) > 0 {
			if !sliceKits.ContainsString(cfg.Tables, tb) {
				return C_reContinue
			}
		}

		this.BinEvent = wrEvent
		this.IfRowsEvent = true

	case replication.QUERY_EVENT:

		this.IfRowsEvent = false
		//queryEvent := ev.Event.(*replication.QueryEvent)
		//db = string(queryEvent.Schema)
		//sql = string(queryEvent.Query)
	case replication.XID_EVENT:
		this.IfRowsEvent = false

	case replication.MARIADB_GTID_EVENT:
		this.IfRowsEvent = false

	default:
		this.IfRowsEvent = false
		return C_reContinue
	}

	return C_reProcess
}

func GetFirstBinlogPosToParse(cfg *ConfCmd) (string, int64) {
	var binlog string
	var pos int64
	if cfg.StartFile != "" {
		binlog = filepath.Join(cfg.BinlogDir, cfg.StartFile)
	} else {
		binlog = cfg.GivenBinlogFile
	}
	if cfg.StartPos != 0 {
		pos = int64(cfg.StartPos)
	} else {
		pos = 4
	}

	return binlog, pos
}
