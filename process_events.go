package main

import (
	"bufio"
	"dannytools/constvar"
	"dannytools/ehand"
	"dannytools/logging"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	SQL "github.com/dropbox/godropbox/database/sqlbuilder"
	//sliceKits "github.com/toolkits/slice"
)

var G_Time_Column_Types []string = []string{"timestamp", "datetime"}

type ExtraSqlInfoOfPrint struct {
	schema    string
	table     string
	binlog    string
	startpos  uint32
	endpos    uint32
	datetime  string
	trxIndex  uint64
	trxStatus int
}

type ForwardRollbackSqlOfPrint struct {
	sqls    []string
	sqlInfo ExtraSqlInfoOfPrint
}

var (
	ForwardSqlFileNamePrefix  string = "forward"
	RollbackSqlFileNamePrefix string = "rollback"
)

func PrintExtraInfoForForwardRollbackupSql(cfg *ConfCmd, sqlChan chan ForwardRollbackSqlOfPrint, wg *sync.WaitGroup) {
	defer wg.Done()
	var (
		rollbackFileName string                   = ""
		tmpFileName      string                   = ""
		oneSqls          string                   = ""
		fhArr            map[string]*os.File      = map[string]*os.File{}
		fhArrBuf         map[string]*bufio.Writer = map[string]*bufio.Writer{}
		FH               *os.File
		bufFH            *bufio.Writer
		err              error
		rollbackFiles    []map[string]string //{"tmp":xx, "rollback":xx}
		lastTrxIndex     uint64              = 0
		trxStr           string              = "commit;\nbegin;\n"
		// trxStrLen int = len(trxStr)
		trxCommitStr string = "commit;\n"
		// trxCommitStrLen int = len(trxCommitStr)
		bytesCntFiles      map[string][][]int = map[string][][]int{} //{"file1":{{8, 0}, {8 , 0}}} {length of bytes, trxIndex}
		lastPrintPos       uint32             = 0
		lastPrintFile      string             = ""
		printBytesInterval uint32             = 1024 * 1024 * 10 //every 10MB print process info
	)
	gLogger.WriteToLogByFieldsNormalOnlyMsg("start thread to write redo/rollback sql into file", logging.INFO)
	for sc := range sqlChan {
		//fmt.Println(sc.sqlInfo)
		if cfg.WorkType == "rollback" {
			tmpFileName = GetForwardRollbackSqlFileName(sc.sqlInfo.schema, sc.sqlInfo.table, cfg.FilePerTable, cfg.OutputDir, true, sc.sqlInfo.binlog, true)
			rollbackFileName = GetForwardRollbackSqlFileName(sc.sqlInfo.schema, sc.sqlInfo.table, cfg.FilePerTable, cfg.OutputDir, true, sc.sqlInfo.binlog, false)

		} else {
			tmpFileName = GetForwardRollbackSqlFileName(sc.sqlInfo.schema, sc.sqlInfo.table, cfg.FilePerTable, cfg.OutputDir, false, sc.sqlInfo.binlog, false)
		}
		if _, ok := fhArr[tmpFileName]; !ok {

			FH, err = os.OpenFile(tmpFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
			gLogger.WriteToLogByFieldsErrorExtramsgExit(err, "Fail to open file "+tmpFileName, logging.ERROR, ehand.ERR_FILE_OPEN)
			bufFH = bufio.NewWriter(FH)
			fhArrBuf[tmpFileName] = bufFH
			fhArr[tmpFileName] = FH
			if cfg.WorkType == "rollback" {
				rollbackFiles = append(rollbackFiles, map[string]string{"tmp": tmpFileName, "rollback": rollbackFileName})
				bytesCntFiles[tmpFileName] = [][]int{}
			}

		}

		if cfg.KeepTrx {
			if sc.sqlInfo.trxIndex != lastTrxIndex {
				if cfg.WorkType == "2sql" {
					fhArrBuf[tmpFileName].WriteString(trxStr)
				}

				/*
					if cfg.WorkType == "rollback" {
						bytesCntFiles[tmpFileName] = append(bytesCntFiles[tmpFileName], []int{trxStrLen, 1})

					}
				*/

			}
		}

		lastTrxIndex = sc.sqlInfo.trxIndex
		oneSqls = GetForwardRollbackContentLineWithExtra(sc, cfg.PrintExtraInfo)
		fhArrBuf[tmpFileName].WriteString(oneSqls)
		if lastPrintFile == "" {
			lastPrintFile = sc.sqlInfo.binlog
		}
		if sc.sqlInfo.binlog != lastPrintFile {
			lastPrintPos = 0
			lastPrintFile = sc.sqlInfo.binlog
			gLogger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("finish processing %s %d", sc.sqlInfo.binlog, sc.sqlInfo.endpos), logging.INFO)
		} else if sc.sqlInfo.endpos-lastPrintPos >= printBytesInterval {
			lastPrintPos = sc.sqlInfo.endpos
			gLogger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("finish processing %s %d", sc.sqlInfo.binlog, sc.sqlInfo.endpos), logging.INFO)
		}

		if cfg.WorkType == "rollback" {
			bytesCntFiles[tmpFileName] = append(bytesCntFiles[tmpFileName], []int{len(oneSqls), int(sc.sqlInfo.trxIndex)})
		}
		/*
			if sc.sqlInfo.trxStatus == C_trxCommit {
				fhArr[tmpFileName].WriteString("commit\n")
			} else if sc.sqlInfo.trxStatus == C_trxRollback {
				fhArr[tmpFileName].WriteString("rollback\n")
			}
		*/

	}
	for fn, bufFH := range fhArrBuf {
		if cfg.KeepTrx && cfg.WorkType == "2sql" {
			bufFH.WriteString(trxCommitStr)
			/*
				if cfg.WorkType == "rollback" {
					bytesCntFiles[fn] = append(bytesCntFiles[fn], []int{trxCommitStrLen, 1})
				}
			*/

		}

		bufFH.Flush()
		fhArr[fn].Close()

	}
	// reverse rollback sql file
	if cfg.WorkType == "rollback" {
		gLogger.WriteToLogByFieldsNormalOnlyMsg("finish writing rollback sql into tmp files, start to revert content order of tmp files", logging.INFO)
		var reWg sync.WaitGroup
		filesChan := make(chan map[string]string, cfg.Threads)
		threadNum := GetMinValue(int(cfg.Threads), len(rollbackFiles))

		for i := 1; i <= threadNum; i++ {
			reWg.Add(1)
			go ReverseFileGo(i, filesChan, bytesCntFiles, cfg.KeepTrx, &reWg)
		}

		for _, tmpArr := range rollbackFiles {
			filesChan <- tmpArr
		}
		close(filesChan)
		reWg.Wait()
		gLogger.WriteToLogByFieldsNormalOnlyMsg("finish reverting content order of tmp files", logging.INFO)
	} else {
		gLogger.WriteToLogByFieldsNormalOnlyMsg("finish writing redo/forward sql into file", logging.INFO)
	}

	gLogger.WriteToLogByFieldsNormalOnlyMsg("exit thread to write redo/rollback sql into file", logging.INFO)

}

func GetForwardRollbackContentLineWithExtra(sq ForwardRollbackSqlOfPrint, ifExtra bool) string {
	if ifExtra {
		return fmt.Sprintf("# datetime=%s database=%s table=%s binlog=%s startpos=%d stoppos=%d\n%s;\n",
			sq.sqlInfo.datetime, sq.sqlInfo.schema, sq.sqlInfo.table, sq.sqlInfo.binlog, sq.sqlInfo.startpos,
			sq.sqlInfo.endpos, strings.Join(sq.sqls, ";\n"))
	} else {

		str := strings.Join(sq.sqls, ";\n") + ";\n"
		//fmt.Println("one sqls", str)
		return str
	}

}

func GetForwardRollbackSqlFileName(schema string, table string, filePerTable bool, outDir string, ifRollback bool, binlog string, ifTmp bool) string {

	_, idx := GetBinlogBasenameAndIndex(binlog)

	if ifRollback {
		if ifTmp {
			if filePerTable {
				return filepath.Join(outDir, fmt.Sprintf(".%s.%s.%s.%d.sql", schema, table, RollbackSqlFileNamePrefix, idx))
			} else {
				return filepath.Join(outDir, fmt.Sprintf(".%s.%d.sql", RollbackSqlFileNamePrefix, idx))
			}

		} else {
			if filePerTable {
				return filepath.Join(outDir, fmt.Sprintf("%s.%s.%s.%d.sql", schema, table, RollbackSqlFileNamePrefix, idx))
			} else {
				return filepath.Join(outDir, fmt.Sprintf("%s.%d.sql", RollbackSqlFileNamePrefix, idx))
			}
		}
	} else {
		if filePerTable {
			return filepath.Join(outDir, fmt.Sprintf("%s.%s.%s.%d.sql", schema, table, ForwardSqlFileNamePrefix, idx))
		} else {
			return filepath.Join(outDir, fmt.Sprintf("%s.%d.sql", ForwardSqlFileNamePrefix, idx))
		}

	}

}

func GenForwardRollbackSqlFromBinEvent(i uint, cfg *ConfCmd, evChan chan MyBinEvent, sqlChan chan ForwardRollbackSqlOfPrint, wg *sync.WaitGroup) {
	defer wg.Done()
	//defer gThreadsFinished.IncreaseFinishedThreadCnt()
	//fmt.Println("enter thread", i)
	var (
		err error
		//var currentIdx uint64
		tbInfo             *TblInfoJson
		db, tb, fulltb     string
		allColNames        []FieldInfo
		colsDef            []SQL.NonAliasColumn
		colsTypeName       []string
		colCnt             int
		sqlArr             []string
		uniqueKeyIdx       []int
		uniqueKey          KeyInfo
		primaryKeyIdx      []int
		ifRollback         bool = false
		ifIgnorePrimary    bool = cfg.IgnorePrimaryKeyForInsert
		currentSqlForPrint ForwardRollbackSqlOfPrint
		posStr             string
		//printStatementSql  bool = false
	)
	gLogger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("start thread %d to generate redo/rollback sql", i), logging.INFO)
	if cfg.WorkType == "rollback" {
		ifRollback = true
	}
	/*
		if cfg.ParseStatementSql && cfg.WorkType == "2sql" {
			printStatementSql = true
		}
	*/

	for ev := range evChan {
		posStr = GetPosStr(ev.MyPos.Name, ev.StartPos, ev.MyPos.Pos)
		if !ev.IfRowsEvent {
			/*
				//only target query can be here, no need to double check
				if !printStatementSql || ev.QuerySql == nil || ev.QuerySql.IsDml() {
					continue
				}
			*/
			currentSqlForPrint = ForwardRollbackSqlOfPrint{sqls: []string{ev.OrgSql},
				sqlInfo: ExtraSqlInfoOfPrint{schema: ev.QuerySql.Tables[0].Database, table: ev.QuerySql.Tables[0].Table,
					binlog: ev.MyPos.Name, startpos: ev.StartPos, endpos: ev.MyPos.Pos,
					datetime: GetDatetimeStr(int64(ev.Timestamp), int64(0), constvar.DATETIME_FORMAT_NOSPACE),
					trxIndex: ev.TrxIndex, trxStatus: ev.TrxStatus}}

		} else {
			db = string(ev.BinEvent.Table.Schema)
			tb = string(ev.BinEvent.Table.Table)
			fulltb = GetAbsTableName(db, tb)
			tbInfo, err = G_TablesColumnsInfo.GetTableInfoJsonOfBinPos(db, tb, ev.MyPos.Name, ev.StartPos, ev.MyPos.Pos)
			if err != nil {
				gLogger.WriteToLogByFieldsErrorExtramsgExit(err, fmt.Sprintf("error to found %s table structure for event %s",
					fulltb, posStr), logging.ERROR, ehand.ERR_BINLOG_EVENT)
				continue
			}
			if tbInfo == nil {
				gLogger.WriteToLogByFieldsExitMsgNoErr(fmt.Sprintf("no suitable table struct found for %s for event %s",
					fulltb, posStr), logging.ERROR, ehand.ERR_BINLOG_EVENT)
			}
			colCnt = len(ev.BinEvent.Rows[0])
			allColNames = GetAllFieldNamesWithDroppedFields(colCnt, tbInfo.Columns)
			colsDef, colsTypeName = GetSqlFieldsEXpressions(colCnt, allColNames, ev.BinEvent.Table)
			colsTypeNameFromMysql := make([]string, len(colsTypeName))
			if len(colsTypeName) > len(tbInfo.Columns) {
				gLogger.WriteToLogByFieldsExitMsgNoErr(fmt.Sprintf("column count %d in binlog > in table structure %d, usually means DDL in the middle, pls generate a suitable table structure conf %s\ntable=%s\nbinlog=%s\ntable structure:\n\t%s\nrow values:\n\t%s",
					len(colsTypeName), len(tbInfo.Columns), cfg.ReadTblDefJsonFile, fulltb, ev.MyPos.String(), spew.Sdump(tbInfo.Columns), spew.Sdump(ev.BinEvent.Rows[0])),
					logging.ERROR, ehand.ERR_ERROR)
			}

			// convert datetime/timestamp type to string
			for ci, colType := range colsTypeName {
				colsTypeNameFromMysql[ci] = tbInfo.Columns[ci].FieldType
				// no need to do this because we donnot parse time/datetime column into go time structure
				/*
					if sliceKits.ContainsString(G_Time_Column_Types, colType) {
						for ri, _ := range ev.BinEvent.Rows {
							if ev.BinEvent.Rows[ri][ci] == nil {
								continue
							}

							//fmt.Println(ev.BinEvent.Rows[ri][0], ev.BinEvent.Rows[ri][ci])
							tv, convertOk := ev.BinEvent.Rows[ri][ci].(time.Time)
							if !convertOk {
								//spew.Dump(ev.BinEvent.Rows[ri][ci])
								tStr, tStrOk := ev.BinEvent.Rows[ri][ci].(string)

								if tStrOk {
									tStrArr := strings.Split(tStr, ".")
									if tStrArr[0] == constvar.DATETIME_ZERO_NO_MS {
										ev.BinEvent.Rows[ri][ci] = constvar.DATETIME_ZERO
									} else {
										gLogger.WriteToLogByFieldsExitMsgNoErr(fmt.Sprintf("fail to convert %s.%s %v to time type %s, convert to string type OK, but value of it is not %s nor %s",
											GetAbsTableName(db, tb), allColNames[ci].FieldName, ev.BinEvent.Rows[ri][ci], posStr,
											constvar.DATETIME_ZERO, constvar.DATETIME_ZERO_NO_MS), logging.ERROR, ehand.ERR_ERROR)
									}
								} else {
									gLogger.WriteToLogByFieldsExitMsgNoErr(fmt.Sprintf("fail to convert %s.%s %v to time type nor string type %s",
										GetAbsTableName(db, tb), allColNames[ci].FieldName, ev.BinEvent.Rows[ri][ci],
										posStr), logging.ERROR, ehand.ERR_ERROR)
								}
							} else {

								if tv.IsZero() || tv.Unix() == 0 {
									//fmt.Println("zero datetime")
									ev.BinEvent.Rows[ri][ci] = constvar.DATETIME_ZERO
								} else {
									tvStr := tv.Format(constvar.DATETIME_FORMAT_FRACTION)
									if tvStr == constvar.DATETIME_ZERO_UNEXPECTED {
										tvStr = constvar.DATETIME_ZERO
									}
									ev.BinEvent.Rows[ri][ci] = tvStr
								}

							}
						}
					} else*/
				if colType == "blob" {
					// text is stored as blob
					if strings.Contains(strings.ToLower(tbInfo.Columns[ci].FieldType), "text") {
						for ri, _ := range ev.BinEvent.Rows {
							if ev.BinEvent.Rows[ri][ci] == nil {
								continue
							}
							txtStr, coOk := ev.BinEvent.Rows[ri][ci].([]byte)
							if !coOk {
								gLogger.WriteToLogByFieldsExitMsgNoErr(fmt.Sprintf("fail to convert %s.%s %v to []byte type %s",
									fulltb, allColNames[ci].FieldName,
									ev.BinEvent.Rows[ri][ci], posStr), logging.ERROR, ehand.ERR_ERROR)

							} else {
								ev.BinEvent.Rows[ri][ci] = string(txtStr)
							}

						}
					}
				}
			}
			uniqueKey = tbInfo.GetOneUniqueKey(cfg.UseUniqueKeyFirst)
			if len(uniqueKey) > 0 {
				uniqueKeyIdx = GetColIndexFromKey(uniqueKey, allColNames)
			} else {
				uniqueKeyIdx = []int{}
			}

			if len(tbInfo.PrimaryKey) > 0 {
				primaryKeyIdx = GetColIndexFromKey(tbInfo.PrimaryKey, allColNames)
			} else {
				primaryKeyIdx = []int{}
				ifIgnorePrimary = false
			}

			if ev.SqlType == "insert" {
				if ifRollback {
					sqlArr = GenDeleteSqlsForOneRowsEventRollbackInsert(posStr, ev.BinEvent, colsDef, uniqueKeyIdx, cfg.FullColumns, cfg.SqlTblPrefixDb)
				} else {
					sqlArr = GenInsertSqlsForOneRowsEvent(posStr, ev.BinEvent, colsDef, cfg.InsertRows, false, cfg.SqlTblPrefixDb, ifIgnorePrimary, primaryKeyIdx)
				}

			} else if ev.SqlType == "delete" {
				if ifRollback {
					sqlArr = GenInsertSqlsForOneRowsEventRollbackDelete(posStr, ev.BinEvent, colsDef, cfg.InsertRows, cfg.SqlTblPrefixDb)
				} else {
					sqlArr = GenDeleteSqlsForOneRowsEvent(posStr, ev.BinEvent, colsDef, uniqueKeyIdx, cfg.FullColumns, false, cfg.SqlTblPrefixDb)
				}
			} else if ev.SqlType == "update" {
				if ifRollback {
					sqlArr = GenUpdateSqlsForOneRowsEvent(posStr, colsTypeNameFromMysql, colsTypeName, ev.BinEvent, colsDef, uniqueKeyIdx, cfg.FullColumns, true, cfg.SqlTblPrefixDb)
				} else {
					sqlArr = GenUpdateSqlsForOneRowsEvent(posStr, colsTypeNameFromMysql, colsTypeName, ev.BinEvent, colsDef, uniqueKeyIdx, cfg.FullColumns, false, cfg.SqlTblPrefixDb)
				}
			} else {
				fmt.Println("unsupported query type %s to generate 2sql|rollback sql, it should one of insert|update|delete. %s", ev.SqlType, ev.MyPos.String())
				continue
			}
			//fmt.Println(sqlArr)
			currentSqlForPrint = ForwardRollbackSqlOfPrint{sqls: sqlArr,
				sqlInfo: ExtraSqlInfoOfPrint{schema: db, table: tb, binlog: ev.MyPos.Name, startpos: ev.StartPos, endpos: ev.MyPos.Pos,
					datetime: GetDatetimeStr(int64(ev.Timestamp), int64(0), constvar.DATETIME_FORMAT_NOSPACE),
					trxIndex: ev.TrxIndex, trxStatus: ev.TrxStatus}}
		}

		for {
			//fmt.Println("in thread", i)
			G_HandlingBinEventIndex.lock.Lock()
			//fmt.Println("handing index:", G_HandlingBinEventIndex.EventIdx, "binevent index:", ev.EventIdx)
			if G_HandlingBinEventIndex.EventIdx == ev.EventIdx {
				sqlChan <- currentSqlForPrint
				G_HandlingBinEventIndex.EventIdx++
				G_HandlingBinEventIndex.lock.Unlock()
				//fmt.Println("handing index == binevent index, break")
				break
			}

			G_HandlingBinEventIndex.lock.Unlock()
			time.Sleep(1 * time.Microsecond)

		}

	}
	//fmt.Println("thread", i, "exits")
	gLogger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("exit thread %d to generate redo/rollback sql", i), logging.INFO)
}
