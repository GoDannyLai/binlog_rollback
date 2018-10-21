package main

import (
	"sync"

	"github.com/siddontang/go-mysql/replication"
)

func main() {
	gLogger.CreateNewRawLogger()
	gConfCmd.IfSetStopParsPoint = false
	gConfCmd.ParseCmdOptions()

	GetTblDefFromDbAndMergeAndDump(gConfCmd)

	if gConfCmd.WorkType != "stats" {
		G_HandlingBinEventIndex = &BinEventHandlingIndx{EventIdx: 1, finished: false}
	}

	eventChan := make(chan MyBinEvent, gConfCmd.Threads*2)
	statChan := make(chan BinEventStats, gConfCmd.Threads*2)
	orgSqlChan := make(chan OrgSqlPrint, gConfCmd.Threads*2)
	sqlChan := make(chan ForwardRollbackSqlOfPrint, gConfCmd.Threads*2)
	var wg, wgGenSql sync.WaitGroup

	// stats file
	statFH, ddlFH, biglongFH := OpenStatsResultFiles(gConfCmd)
	defer statFH.Close()
	defer ddlFH.Close()
	defer biglongFH.Close()
	wg.Add(1)
	go ProcessBinEventStats(statFH, ddlFH, biglongFH, gConfCmd, statChan, &wg)
	if gConfCmd.IfWriteOrgSql {
		wg.Add(1)
		go PrintOrgSqlToFile(gConfCmd.OutputDir, orgSqlChan, &wg)
	}

	if gConfCmd.WorkType != "stats" {
		// write forward or rollback sql to file
		wg.Add(1)
		go PrintExtraInfoForForwardRollbackupSql(gConfCmd, sqlChan, &wg)

		// generate forward or rollback sql from binlog
		//gThreadsFinished.threadsCnt = gConfCmd.Threads
		for i := uint(1); i <= gConfCmd.Threads; i++ {
			wgGenSql.Add(1)
			go GenForwardRollbackSqlFromBinEvent(i, gConfCmd, eventChan, sqlChan, &wgGenSql)
		}

	}

	if gConfCmd.Mode == "repl" {
		ParserAllBinEventsFromRepl(gConfCmd, eventChan, statChan, orgSqlChan)
	} else if gConfCmd.Mode == "file" {
		myParser := BinFileParser{}
		myParser.parser = replication.NewBinlogParser()
		myParser.parser.SetTimestampStringLocation(gBinlogTimeLocation)
		myParser.parser.SetParseTime(false)  // donot parse mysql datetime/time column into go time structure, take it as string
		myParser.parser.SetUseDecimal(false) // sqlbuilder not support decimal type
		myParser.MyParseAllBinlogFiles(gConfCmd, eventChan, statChan, orgSqlChan)
	}

	//fmt.Println(gThreadsFinished.threadsCnt, gThreadsFinished.threadsCnt)
	wgGenSql.Wait()
	close(sqlChan)

	wg.Wait()

}
