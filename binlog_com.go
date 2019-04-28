package main

import (
	"dannytools/constvar"
	"dannytools/dsql"
	"dannytools/ehand"
	"dannytools/logging"

	//"os"
	"path/filepath"
	"strings"
	"time"

	//"github.com/davecgh/go-spew/spew"

	"fmt"
	"sync"

	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
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
	TrxStatus   int           // 0:begin, 1: commit, 2: rollback, -1: in_progress
	QuerySql    *dsql.SqlInfo // for ddl and binlog which is not row format
	OrgSql      string        // for ddl and binlog which is not row format
}

func CheckBinHeaderCondition(cfg *ConfCmd, header *replication.EventHeader, currentBinlog string) int {
	// process: 0, continue: 1, break: 2

	myPos := mysql.Position{Name: currentBinlog, Pos: header.LogPos}
	//fmt.Println(cfg.StartFilePos, cfg.IfSetStopFilePos, myPos)
	if cfg.IfSetStartFilePos {
		cmpRe := myPos.Compare(cfg.StartFilePos)
		if cmpRe == -1 {
			return C_reContinue
		}
	}

	if cfg.IfSetStopFilePos {
		cmpRe := myPos.Compare(cfg.StopFilePos)
		if cmpRe >= 0 {
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
		if header.Timestamp >= cfg.StopDatetime {
			return C_reBreak
		}
	}
	if cfg.FilterSqlLen == 0 {
		return C_reProcess
	}

	if header.EventType == replication.WRITE_ROWS_EVENTv1 || header.EventType == replication.WRITE_ROWS_EVENTv2 {
		if cfg.IsTargetDml("insert") {
			return C_reProcess
		} else {
			return C_reContinue
		}
	}

	if header.EventType == replication.UPDATE_ROWS_EVENTv1 || header.EventType == replication.UPDATE_ROWS_EVENTv2 {
		if cfg.IsTargetDml("update") {
			return C_reProcess
		} else {
			return C_reContinue
		}
	}

	if header.EventType == replication.DELETE_ROWS_EVENTv1 || header.EventType == replication.DELETE_ROWS_EVENTv2 {
		if cfg.IsTargetDml("delete") {
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
		gLogger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("log rotate %s", myPos.String()), logging.INFO)
		if cfg.IfSetStartFilePos {
			cmpRe := myPos.Compare(cfg.StartFilePos)
			if cmpRe == -1 {
				return C_reContinue
			}
		}

		if cfg.IfSetStopFilePos {
			cmpRe := myPos.Compare(cfg.StopFilePos)
			if cmpRe >= 0 {
				return C_reBreak
			}
		}
		this.IfRowsEvent = false
		return C_reContinue

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
		if !cfg.IsTargetTable(db, tb) {
			return C_reContinue
		}
		/*
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
		*/

		this.BinEvent = wrEvent
		this.IfRowsEvent = true

	case replication.QUERY_EVENT:

		this.IfRowsEvent = false

		queryEvent := ev.Event.(*replication.QueryEvent)
		db := string(queryEvent.Schema) // for DML/DDL, schema is not empty if connection has default database, ie use db

		sqlStr := string(queryEvent.Query)
		this.OrgSql = sqlStr
		lowerSqlStr := strings.TrimSpace(strings.ToLower(sqlStr))

		// for mysql, begin of transaction, it write a query event with sql 'BEGIN'
		// for DML, row or statement, a trx is composed of gtid event,  query event(BEGIN), query event/Rows_query event, xid event
		// for DDL, a trx is composed of gtid event, query event
		// query event may be composed of use database, DML/DDL sql
		if lowerSqlStr != "begin" && lowerSqlStr != "commit" {
			if db == "" && gUseDatabase != "" {
				db = gUseDatabase
			}
			//ev.Dump(os.Stdout)
			//tidb.parser not support create trigger statement
			parsedResult, lastUseDb, err := dsql.ParseSqlsForSqlInfo(gSqlParser, sqlStr, db)
			//spew.Dump(parsedResult)
			if err != nil {
				if cfg.IgnoreParsedErrRegexp != nil && cfg.IgnoreParsedErrRegexp.MatchString(lowerSqlStr) {
					gLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, fmt.Sprintf("\nerror to parse sql from query event, binlog=%s, time=%s, sql=%s",
						myPos.String(), time.Unix(int64(ev.Header.Timestamp), 0).Format(constvar.DATETIME_FORMAT_NOSPACE), sqlStr),
						logging.ERROR, ehand.ERR_ERROR)
					return C_reContinue
				} else {
					//exit program here
					gLogger.WriteToLogByFieldsErrorExtramsgExit(err, fmt.Sprintf("\nerror to parse sql from query event, binlog=%s, time=%s, sql=%s",
						myPos.String(), time.Unix(int64(ev.Header.Timestamp), 0).Format(constvar.DATETIME_FORMAT_NOSPACE), sqlStr),
						logging.ERROR, ehand.ERR_ERROR)
					return C_reBreak
				}

			}

			// have new use database
			if lastUseDb != "" && gUseDatabase != lastUseDb {
				gUseDatabase = lastUseDb
			}

			// skip other query event, we only care Table DDL and DML
			if len(parsedResult) != 1 {
				gLogger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("skip it, parsed result for query event is nil or more than one(len(parsedResult)=%d), binlog=%s, time=%s, sql=%s",
					len(parsedResult), myPos.String(), time.Unix(int64(ev.Header.Timestamp), 0).Format(constvar.DATETIME_FORMAT_NOSPACE), sqlStr), logging.WARNING)
				return C_reContinue
			}

			// should only one result

			if parsedResult[0].IsDml() {
				if !cfg.ParseStatementSql {
					//gLogger.WriteToLogByFieldsNormalOnlyMsg("not parse dml", logging.INFO)
					return C_reContinue
				}
				if !cfg.IsTargetDml(parsedResult[0].GetDmlName()) {
					//gLogger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("dml %s not target", parsedResult[0].GetDmlName()), logging.INFO)
					return C_reContinue
				}

				ifAnyTargetTable := false
				for j := range parsedResult[0].Tables {
					if parsedResult[0].Tables[j].Database == "" {
						parsedResult[0].Tables[j].Database = db
					}

					if cfg.IsTargetTable(parsedResult[0].Tables[j].Database, parsedResult[0].Tables[j].Table) {
						ifAnyTargetTable = true
					}

				}
				if !ifAnyTargetTable {
					//gLogger.WriteToLogByFieldsNormalOnlyMsg("not target table", logging.INFO)
					return C_reContinue
				}

			}

			this.QuerySql = parsedResult[0].Copy()
			//gLogger.WriteToLogByFieldsNormalOnlyMsg("should be processed", logging.INFO)

		}

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
