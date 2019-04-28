package main

import (
	"context"
	"dannytools/ehand"
	"dannytools/logging"
	"fmt"
	"strings"

	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

/*
type ReplBinlogStreamer struct {
	cfg ConfCmd
	binSyncCfg replication.BinlogSyncerConfig
	replBinSyncer *replication.BinlogSyncer
	replBinStreamer *replication.BinlogStreamer
}
*/

func ParserAllBinEventsFromRepl(cfg *ConfCmd, eventChan chan MyBinEvent, statChan chan BinEventStats, orgSqlChan chan OrgSqlPrint) {

	defer close(eventChan)
	defer close(statChan)
	defer close(orgSqlChan)
	replStreamer := NewReplBinlogStreamer(cfg)
	gLogger.WriteToLogByFieldsNormalOnlyMsg("start to get binlog from mysql", logging.INFO)
	SendBinlogEventRepl(cfg, replStreamer, eventChan, statChan, orgSqlChan)
	gLogger.WriteToLogByFieldsNormalOnlyMsg("finish getting binlog from mysql", logging.INFO)

}

func NewReplBinlogStreamer(cfg *ConfCmd) *replication.BinlogStreamer {
	replCfg := replication.BinlogSyncerConfig{
		ServerID:                uint32(cfg.ServerId),
		Flavor:                  cfg.MysqlType,
		Host:                    cfg.Host,
		Port:                    uint16(cfg.Port),
		User:                    cfg.User,
		Password:                cfg.Passwd,
		Charset:                 "utf8",
		SemiSyncEnabled:         false,
		TimestampStringLocation: gBinlogTimeLocation,
		ParseTime:               false, //donot parse mysql datetime/time column into go time structure, take it as string
		UseDecimal:              false, // sqlbuilder not support decimal type
	}

	replSyncer := replication.NewBinlogSyncer(replCfg)

	syncPosition := mysql.Position{Name: cfg.StartFile, Pos: uint32(cfg.StartPos)}
	replStreamer, err := replSyncer.StartSync(syncPosition)
	if err != nil {
		gLogger.WriteToLogByFieldsErrorExtramsgExit(err, fmt.Sprintf("error replication from master %s:%d ",
			cfg.Host, cfg.Port), logging.ERROR, ehand.ERR_MYSQL_CONNECTION)
	}
	return replStreamer
}

func SendBinlogEventRepl(cfg *ConfCmd, streamer *replication.BinlogStreamer, eventChan chan MyBinEvent, statChan chan BinEventStats, orgSqlChan chan OrgSqlPrint) {
	//defer close(statChan)
	//defer close(eventChan)

	var (
		chkRe         int
		currentBinlog string = cfg.StartFile
		binEventIdx   uint64 = 0
		trxIndex      uint64 = 0
		trxStatus     int    = 0
		sqlLower      string = ""

		db      string = ""
		tb      string = ""
		sql     string = ""
		sqlType string = ""
		rowCnt  uint32 = 0

		tbMapPos uint32 = 0

		justStart   bool = true
		orgSqlEvent *replication.RowsQueryEvent
	)

	//defer g_MaxBin_Event_Idx.SetMaxBinEventIdx()
	for {
		ev, err := streamer.GetEvent(context.Background())
		if err != nil {
			gLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, "error to get binlog event", logging.ERROR, ehand.ERR_MYSQL_REPL)
			break
		}

		if !cfg.IfSetStopParsPoint && !cfg.IfSetStopDateTime && !justStart {
			//just parse one binlog. the first event is rotate event

			if ev.Header.EventType == replication.ROTATE_EVENT {
				break
			}
		}
		justStart = false

		if ev.Header.EventType == replication.TABLE_MAP_EVENT {
			tbMapPos = ev.Header.LogPos - ev.Header.EventSize // avoid mysqlbing mask the row event as unknown table row event
		}
		ev.RawData = []byte{} // we donnot need raw data

		chkRe = CheckBinHeaderCondition(cfg, ev.Header, currentBinlog)

		if chkRe == C_reBreak {
			break
		} else if chkRe == C_reContinue {
			continue
		} else if chkRe == C_reFileEnd {
			continue
		}

		if cfg.IfWriteOrgSql && ev.Header.EventType == replication.ROWS_QUERY_EVENT {
			orgSqlEvent = ev.Event.(*replication.RowsQueryEvent)
			orgSqlChan <- OrgSqlPrint{Binlog: currentBinlog, DateTime: ev.Header.Timestamp,
				StartPos: ev.Header.LogPos - ev.Header.EventSize, StopPos: ev.Header.LogPos, QuerySql: string(orgSqlEvent.Query)}
			continue
		}

		oneMyEvent := &MyBinEvent{MyPos: mysql.Position{Name: currentBinlog, Pos: ev.Header.LogPos},
			StartPos: tbMapPos}
		//StartPos: ev.Header.LogPos - ev.Header.EventSize}
		chkRe = oneMyEvent.CheckBinEvent(cfg, ev, &currentBinlog)
		//gLogger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("check binlog event result %d", chkRe), logging.INFO)
		if chkRe == C_reContinue {
			continue
		} else if chkRe == C_reBreak {
			break
		} else if chkRe == C_reProcess {

			db, tb, sqlType, sql, rowCnt = GetDbTbAndQueryAndRowCntFromBinevent(ev)

			if sqlType == "query" {
				sqlLower = strings.ToLower(sql)

				if sqlLower == "begin" {
					trxStatus = C_trxBegin
					trxIndex++
				} else if sqlLower == "commit" {
					trxStatus = C_trxCommit
				} else if sqlLower == "rollback" {
					trxStatus = C_trxRollback
				} else if oneMyEvent.QuerySql != nil && oneMyEvent.QuerySql.IsDml() {
					trxStatus = C_trxProcess
					rowCnt = 1
				}

			} else {
				trxStatus = C_trxProcess
			}

			if cfg.WorkType != "stats" {
				ifSendEvent := false
				if oneMyEvent.IfRowsEvent {

					tbKey := GetAbsTableName(string(oneMyEvent.BinEvent.Table.Schema),
						string(oneMyEvent.BinEvent.Table.Table))
					if _, ok := G_TablesColumnsInfo.tableInfos[tbKey]; ok {
						ifSendEvent = true
					} else {
						gLogger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("no table struct found for %s, it maybe dropped, skip it. RowsEvent position:%s",
							tbKey, oneMyEvent.MyPos.String()), logging.ERROR)

					}

				} else if cfg.WorkType == "2sql" && cfg.ParseStatementSql && oneMyEvent.QuerySql != nil && oneMyEvent.QuerySql.IsDml() {
					ifSendEvent = true
					//gLogger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("send dml event: %s", spew.Sdump(oneMyEvent.QuerySql)), logging.INFO)
				}
				if ifSendEvent {
					binEventIdx++
					oneMyEvent.EventIdx = binEventIdx
					oneMyEvent.SqlType = sqlType
					oneMyEvent.Timestamp = ev.Header.Timestamp
					oneMyEvent.TrxIndex = trxIndex
					oneMyEvent.TrxStatus = trxStatus
					eventChan <- *oneMyEvent

				}
			}

			// output analysis result whatever the WorkType is
			if sqlType != "" {

				if sqlType == "query" {
					if oneMyEvent.QuerySql != nil {
						//gLogger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("send dml/ddl event for statchan: %s", spew.Sdump(oneMyEvent.QuerySql)), logging.INFO)
						statChan <- BinEventStats{Timestamp: ev.Header.Timestamp, Binlog: currentBinlog, StartPos: ev.Header.LogPos - ev.Header.EventSize, StopPos: ev.Header.LogPos,
							Database: oneMyEvent.QuerySql.GetDatabasesAll(","), Table: oneMyEvent.QuerySql.GetFullTablesAll(","), QuerySql: sql,
							RowCnt: rowCnt, QueryType: sqlType, ParsedSqlInfo: oneMyEvent.QuerySql.Copy()}
					} else {
						statChan <- BinEventStats{Timestamp: ev.Header.Timestamp, Binlog: currentBinlog, StartPos: ev.Header.LogPos - ev.Header.EventSize, StopPos: ev.Header.LogPos,
							Database: db, Table: tb, QuerySql: sql, RowCnt: rowCnt, QueryType: sqlType}
					}

				} else {
					statChan <- BinEventStats{Timestamp: ev.Header.Timestamp, Binlog: currentBinlog, StartPos: tbMapPos, StopPos: ev.Header.LogPos,
						Database: db, Table: tb, QuerySql: sql, RowCnt: rowCnt, QueryType: sqlType}
				}

			}

		} else if chkRe == C_reFileEnd {
			continue
		} else {
			gLogger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("this should not happen: return value of CheckBinEvent() is %d\n", chkRe),
				logging.WARNING)

		}

	}
}
