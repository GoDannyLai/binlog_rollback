package main

import (
	"bytes"
	"dannytools/ehand"
	"dannytools/logging"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/toolkits/file"
	//"github.com/davecgh/go-spew/spew"
)

var (
	fileBinEventHandlingIndex uint64 = 0
	fileTrxIndex              uint64 = 0
)

type BinFileParser struct {
	parser *replication.BinlogParser
}

func (this BinFileParser) MyParseAllBinlogFiles(cfg *ConfCmd, evChan chan MyBinEvent, statChan chan BinEventStats, orgSqlChan chan OrgSqlPrint) {

	defer close(evChan)
	defer close(statChan)
	defer close(orgSqlChan)
	gLogger.WriteToLogByFieldsNormalOnlyMsg("start to parse binlog from local files", logging.INFO)
	binlog, binpos := GetFirstBinlogPosToParse(cfg)
	binBaseName, binBaseIndx := GetBinlogBasenameAndIndex(binlog)
	for {
		if cfg.IfSetStopFilePos {
			if cfg.StopFilePos.Compare(mysql.Position{Name: filepath.Base(binlog), Pos: 4}) < 1 {
				break
			}
		}
		gLogger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("start to parse %s %d\n", binlog, binpos), logging.INFO)

		result, err := this.MyParseOneBinlogFile(cfg, binlog, evChan, statChan, orgSqlChan)
		if err != nil {
			gLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, "error to parse binlog", logging.ERROR, ehand.ERR_BINLOG_EVENT)
			break
		}
		if result == C_reBreak {
			break
		} else if result == C_reFileEnd {
			if !cfg.IfSetStopParsPoint {
				//just parse one binlog
				break
			}
			binlog = filepath.Join(cfg.BinlogDir, GetNextBinlog(binBaseName, binBaseIndx))
			if !file.IsFile(binlog) {
				gLogger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("%s not exists nor a file\n", binlog), logging.WARNING)
				break
			}
			binBaseIndx++
			binpos = 4
		} else {
			gLogger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("this should not happen: return value of MyParseOneBinlog is %d\n",
				result), logging.WARNING)
			break
		}

	}
	gLogger.WriteToLogByFieldsNormalOnlyMsg("finish parsing binlog from local files", logging.INFO)
	//g_MaxBin_Event_Idx.SetMaxBinEventIdx(fileBinEventHandlingIndex)

}

func (this BinFileParser) MyParseOneBinlogFile(cfg *ConfCmd, name string, evChan chan MyBinEvent, statChan chan BinEventStats, orgSqlChan chan OrgSqlPrint) (int, error) {
	// process: 0, continue: 1, break: 2
	f, err := os.Open(name)
	if f != nil {
		defer f.Close()
	}
	if err != nil {
		gLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, "fail to open "+name, logging.ERROR, ehand.ERR_FILE_OPEN)
		return C_reBreak, errors.Trace(err)
	}

	fileTypeBytes := int64(4)

	b := make([]byte, fileTypeBytes)
	if _, err = f.Read(b); err != nil {
		gLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, "fail to read "+name, logging.ERROR, ehand.ERR_FILE_READ)
		return C_reBreak, errors.Trace(err)
	} else if !bytes.Equal(b, replication.BinLogFileHeader) {
		gLogger.WriteToLogByFieldsNormalOnlyMsg(name+" is not a valid binlog file, head 4 bytes must fe'bin' ", logging.WARNING)
		return C_reBreak, errors.Trace(err)
	}

	// must not seek to other position, otherwise the program may panic because formatevent, table map event is skipped
	if _, err = f.Seek(fileTypeBytes, os.SEEK_SET); err != nil {
		gLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, fmt.Sprintf("error seek %s to %d", name, fileTypeBytes),
			logging.ERROR, ehand.ERR_FILE_SEEK)
		return C_reBreak, errors.Trace(err)
	}
	var binlog string = filepath.Base(name)
	return this.MyParseReader(cfg, f, evChan, &binlog, statChan, orgSqlChan)
}

func (this BinFileParser) MyParseReader(cfg *ConfCmd, r io.Reader, evChan chan MyBinEvent, binlog *string, statChan chan BinEventStats, orgSqlChan chan OrgSqlPrint) (int, error) {
	// process: 0, continue: 1, break: 2, EOF: 3

	var (
		err         error
		n           int64
		db          string = ""
		tb          string = ""
		sql         string = ""
		sqlType     string = ""
		rowCnt      uint32 = 0
		trxStatus   int    = 0
		sqlLower    string = ""
		tbMapPos    uint32 = 0
		orgSqlEvent *replication.RowsQueryEvent
	)

	for {
		headBuf := make([]byte, replication.EventHeaderSize)

		if _, err = io.ReadFull(r, headBuf); err == io.EOF {
			return C_reFileEnd, nil
		} else if err != nil {
			gLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, "fail to read binlog event header of "+*binlog,
				logging.ERROR, ehand.ERR_FILE_READ)
			return C_reBreak, errors.Trace(err)
		}

		var h *replication.EventHeader
		h, err = this.parser.ParseHeader(headBuf)
		if err != nil {
			gLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, "fail to parse binlog event header of "+*binlog,
				logging.ERROR, ehand.ERR_BINEVENT_HEADER)
			return C_reBreak, errors.Trace(err)
		}
		//fmt.Printf("parsing %s %d %s\n", *binlog, h.LogPos, GetDatetimeStr(int64(h.Timestamp), int64(0), DATETIME_FORMAT))

		if h.EventSize <= uint32(replication.EventHeaderSize) {
			err = errors.Errorf("invalid event header, event size is %d, too small", h.EventSize)
			gLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, "", logging.ERROR, ehand.ERR_BINEVENT_HEADER)
			return C_reBreak, err

		}

		var buf bytes.Buffer
		if n, err = io.CopyN(&buf, r, int64(h.EventSize)-int64(replication.EventHeaderSize)); err != nil {
			err = errors.Errorf("get event body err %v, need %d - %d, but got %d", err, h.EventSize, replication.EventHeaderSize, n)
			gLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, "", logging.ERROR, ehand.ERR_BINEVENT_BODY)
			return C_reBreak, err
		}

		//h.Dump(os.Stdout)

		data := buf.Bytes()
		var rawData []byte
		rawData = append(rawData, headBuf...)
		rawData = append(rawData, data...)

		eventLen := int(h.EventSize) - replication.EventHeaderSize

		if len(data) != eventLen {
			err = errors.Errorf("invalid data size %d in event %s, less event length %d", len(data), h.EventType, eventLen)
			gLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, "", logging.ERROR, ehand.ERR_BINEVENT_BODY)
			return C_reBreak, err
		}

		var e replication.Event
		e, err = this.parser.ParseEvent(h, data, rawData)
		if err != nil {
			gLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, "fail to parse binlog event body of "+*binlog,
				logging.ERROR, ehand.ERR_BINEVENT_BODY)
			return C_reBreak, errors.Trace(err)
		}
		if h.EventType == replication.TABLE_MAP_EVENT {
			tbMapPos = h.LogPos - h.EventSize // avoid mysqlbing mask the row event as unknown table row event
		}
		//e.Dump(os.Stdout)
		//can not advance this check, because we need to parse table map event or table may not found. Also we must seek ahead the read file position
		chRe := CheckBinHeaderCondition(cfg, h, binlog)
		if chRe == C_reBreak {
			return C_reBreak, nil
		} else if chRe == C_reContinue {
			continue
		} else if chRe == C_reFileEnd {
			return C_reFileEnd, nil
		}
		if h.EventType == replication.ROWS_QUERY_EVENT {
			orgSqlEvent = e.(*replication.RowsQueryEvent)
			orgSqlChan <- OrgSqlPrint{Binlog: *binlog, DateTime: h.Timestamp,
				StartPos: h.LogPos - h.EventSize, StopPos: h.LogPos, QuerySql: string(orgSqlEvent.Query)}
			continue
		}

		//binEvent := &replication.BinlogEvent{RawData: rawData, Header: h, Event: e}
		binEvent := &replication.BinlogEvent{Header: h, Event: e} // we donnot need raw data
		oneMyEvent := &MyBinEvent{MyPos: mysql.Position{Name: *binlog, Pos: h.LogPos},
			StartPos: tbMapPos}
		//StartPos: h.LogPos - h.EventSize}
		chRe = oneMyEvent.CheckBinEvent(cfg, binEvent, binlog)
		if chRe == C_reBreak {
			return C_reBreak, nil
		} else if chRe == C_reContinue {
			continue
		} else if chRe == C_reFileEnd {
			return C_reFileEnd, nil
		} else if chRe == C_reProcess {

			// output analysis result whatever the WorkType is
			db, tb, sqlType, sql, rowCnt = GetDbTbAndQueryAndRowCntFromBinevent(binEvent)
			if sqlType == "query" {
				sqlLower = strings.ToLower(sql)

				if sqlLower == "begin" {
					trxStatus = C_trxBegin
					fileTrxIndex++
				} else if sqlLower == "commit" {
					trxStatus = C_trxCommit
				} else if sqlLower == "rollback" {
					trxStatus = C_trxRollback
				}
			} else {
				trxStatus = C_trxProcess
			}

			if cfg.WorkType != "stats" && oneMyEvent.IfRowsEvent {

				tbKey := GetAbsTableName(string(oneMyEvent.BinEvent.Table.Schema),
					string(oneMyEvent.BinEvent.Table.Table))
				if _, ok := G_TablesColumnsInfo.tableInfos[tbKey]; ok {
					fileBinEventHandlingIndex++
					oneMyEvent.EventIdx = fileBinEventHandlingIndex
					oneMyEvent.SqlType = sqlType
					oneMyEvent.Timestamp = h.Timestamp
					oneMyEvent.TrxIndex = fileTrxIndex
					oneMyEvent.TrxStatus = trxStatus
					evChan <- *oneMyEvent
				} /*else {
					fmt.Printf("no table struct found for %s, it maybe dropped, skip it. RowsEvent position:%s", tbKey, oneMyEvent.MyPos.String())
				}*/

			}

			if sqlType != "" {
				if sqlType == "query" {
					statChan <- BinEventStats{Timestamp: h.Timestamp, Binlog: *binlog, StartPos: h.LogPos - h.EventSize, StopPos: h.LogPos,
						Database: db, Table: tb, QuerySql: sql, RowCnt: rowCnt, QueryType: sqlType}
				} else {
					statChan <- BinEventStats{Timestamp: h.Timestamp, Binlog: *binlog, StartPos: tbMapPos, StopPos: h.LogPos,
						Database: db, Table: tb, QuerySql: sql, RowCnt: rowCnt, QueryType: sqlType}
				}

			}

		}

	}

	return C_reFileEnd, nil
}
