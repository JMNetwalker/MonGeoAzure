#Exec queries
Import-Module sqlps -DisableNameChecking -WarningAction SilentlyContinue | Out-Null

#Connectivity details User DB and master DB
$server= @("servername1.database.windows.net", "servername2.database.windows.net")
$databaseDBUser=@("","")
$databaseDBUser[0]=@("DB1", "DB2")
$databaseDBUser[1]=@("DB1")
$databaseDBMaster="master"
$user=@("usernameforserver1","usernameforserver2")
$password=@("passwordforserver1","passwordforserver2")

#Files Name DBUser
$filesDbUser=@("_ResourceDB", "_WaitStats", "_Locks", "_most_time_cpu_execute","_worst_IO_execute","_QDS_longest_average_execution","_QDS_biggest_average_physical_IO_reads","_QDS_Queries_Multiple_Plans", "_QDS_regressed_in_performance")
$fecha=Get-Date -format "yyyyMMddHHmmss"
$folder="E:\Salida\"

#Files Name Master DB
$filesDbMaster=@("_Deadlock","_Conn_Status")

#Queries
$QueriesDBUser = @( "select convert(varchar(20), end_time,120) as end_time,avg_cpu_percent,avg_data_io_percent,avg_log_write_percent,avg_memory_usage_percent,xtp_storage_percent,max_worker_percent,max_session_percent,dtu_limit from sys.dm_db_resource_stats", 
                    "select wait_type, waiting_tasks_count,wait_time_ms,max_wait_time_ms,signal_wait_time_ms from sys.dm_db_wait_stats", 
                    "select conn.session_id as blockerSession,conn2.session_id as BlockedSession,req.wait_time as Waiting_Time_ms,cast((req.wait_time/1000.) as decimal(18,2)) as Waiting_Time_secs,
                      cast((req.wait_time/1000./60.) as decimal(18,2)) as Waiting_Time_mins,t.text as BlockerQuery,t2.text as BlockedQuery, req.wait_type from sys.dm_exec_requests as req
                      inner join sys.dm_exec_connections as conn on req.blocking_session_id=conn.session_id
                      inner join sys.dm_exec_connections as conn2 on req.session_id=conn2.session_id
                      cross apply sys.dm_exec_sql_text(conn.most_recent_sql_handle) as t
                      cross apply sys.dm_exec_sql_text(conn2.most_recent_sql_handle) as t2", 
                    "SELECT TOP 30 total_worker_time, total_elapsed_time,
                              total_worker_time/execution_count AS avg_cpu_cost, execution_count,
                              (SELECT DB_NAME(dbid) + ISNULL('..' + OBJECT_NAME(objectid), '')
                               FROM sys.dm_exec_sql_text([sql_handle])) AS query_database,
                               (SELECT SUBSTRING(est.[text], statement_start_offset/2 + 1,
                                   (CASE WHEN statement_end_offset = -1
                                    THEN LEN(CONVERT(nvarchar(max), est.[text])) * 2
                                    ELSE statement_end_offset
                                    END - statement_start_offset) / 2 )
                             FROM sys.dm_exec_sql_text([sql_handle]) AS est) AS query_text,
                                  total_logical_reads/execution_count AS avg_logical_reads,
                                  total_logical_writes/execution_count AS avg_logical_writes,
                                  last_worker_time, min_worker_time, max_worker_time,
                                  last_elapsed_time, min_elapsed_time, max_elapsed_time,
                                  plan_generation_num, qp.query_plan
                                  FROM sys.dm_exec_query_stats
                                       OUTER APPLY sys.dm_exec_query_plan([plan_handle]) AS qp
                                  WHERE (total_worker_time/execution_count) > 100
                                  ORDER BY total_worker_time DESC",  #Which Queries are taking the most time/cpu to execute
                     "SELECT TOP 30 total_worker_time, total_elapsed_time,
                              total_worker_time/execution_count AS avg_cpu_cost, execution_count,
                              (SELECT DB_NAME(dbid) + ISNULL('..' + OBJECT_NAME(objectid), '')
                               FROM sys.dm_exec_sql_text([sql_handle])) AS query_database,
                               (SELECT SUBSTRING(est.[text], statement_start_offset/2 + 1,
                                   (CASE WHEN statement_end_offset = -1
                                    THEN LEN(CONVERT(nvarchar(max), est.[text])) * 2
                                    ELSE statement_end_offset
                                    END - statement_start_offset) / 2 )
                             FROM sys.dm_exec_sql_text([sql_handle]) AS est) AS query_text,
                                  total_logical_reads/execution_count AS avg_logical_reads,
                                  total_logical_writes/execution_count AS avg_logical_writes,
                                  last_worker_time, min_worker_time, max_worker_time,
                                  last_elapsed_time, min_elapsed_time, max_elapsed_time,
                                  plan_generation_num, qp.query_plan
                                  FROM sys.dm_exec_query_stats
                                       OUTER APPLY sys.dm_exec_query_plan([plan_handle]) AS qp
                                  ORDER BY total_logical_reads DESC", #Worst performing I/O bound queries'
                     "SELECT TOP 30 q.query_id, rs.avg_duration, qt.query_sql_text, qt.query_text_id, p.plan_id,convert(varchar(20), rs.last_execution_time ,120) as last_execution_time 
                              FROM sys.query_store_query_text AS qt 
                              JOIN sys.query_store_query AS q ON qt.query_text_id = q.query_text_id 
                              JOIN sys.query_store_plan AS p ON q.query_id = p.query_id 
                              JOIN sys.query_store_runtime_stats AS rs ON p.plan_id = rs.plan_id
                              WHERE rs.last_execution_time > DATEADD(day, -7, GETUTCDATE())
                              ORDER BY rs.avg_duration DESC", #The number of queries with the longest average execution time within last week
                     "SELECT TOP 30 q.query_id, rs.avg_physical_io_reads, qt.query_sql_text, qt.query_text_id, p.plan_id, rs.runtime_stats_id, 
                              convert(varchar(20), rsi.start_time ,120) AS start_time, 
                              convert(varchar(20), rsi.end_time ,120) as end_time, rs.avg_rowcount, rs.count_executions
                              FROM sys.query_store_query_text AS qt 
                              JOIN sys.query_store_query AS q ON qt.query_text_id = q.query_text_id 
                              JOIN sys.query_store_plan AS p ON q.query_id = p.query_id 
                              JOIN sys.query_store_runtime_stats AS rs ON p.plan_id = rs.plan_id 
                              JOIN sys.query_store_runtime_stats_interval AS rsi ON rsi.runtime_stats_interval_id = rs.runtime_stats_interval_id
                              WHERE rsi.start_time >= DATEADD(day, -7, GETUTCDATE()) 
                              ORDER BY rs.avg_physical_io_reads DESC", #The number of queries that had the biggest average physical IO reads in last week, with corresponding average row count and execution count
                      "WITH Query_MultPlans
                AS
                (
                 SELECT COUNT(*) AS cnt, q.query_id 
                        FROM sys.query_store_query_text AS qt
                        JOIN sys.query_store_query AS q ON qt.query_text_id = q.query_text_id
                        JOIN sys.query_store_plan AS p ON p.query_id = q.query_id
                        GROUP BY q.query_id
                        HAVING COUNT(distinct plan_id) > 1
                )
                SELECT q.query_id, object_name(object_id) AS ContainingObject, query_sql_text,
                       plan_id, p.query_plan AS plan_xml,
                       convert(varchar(20), p.last_compile_start_time ,120) AS last_compile_start_time, 
                       convert(varchar(20), p.last_execution_time ,120) AS last_execution_time
                       FROM Query_MultPlans AS qm
                       JOIN sys.query_store_query AS q ON qm.query_id = q.query_id
                       JOIN sys.query_store_plan AS p ON q.query_id = p.query_id
                       JOIN sys.query_store_query_text qt ON qt.query_text_id = q.query_text_id
                       ORDER BY query_id, plan_id", #Queries with multiple plans
                    "SELECT q.query_id, qt.query_sql_text, qt.query_text_id,  rs1.runtime_stats_id AS runtime_stats_id_1,convert(varchar(20), rsi1.start_time ,120) AS interval_1, 
                      p1.plan_id AS plan_1, 
                      rs1.avg_duration AS avg_duration_1, 
                      rs2.avg_duration AS avg_duration_2,
                      p2.plan_id AS plan_2, 
                      convert(varchar(20), rsi2.start_time ,120) AS interval_2, 
                      rs2.runtime_stats_id AS runtime_stats_id_2
                      FROM sys.query_store_query_text AS qt 
                      JOIN sys.query_store_query AS q ON qt.query_text_id = q.query_text_id 
                      JOIN sys.query_store_plan AS p1 ON q.query_id = p1.query_id 
                      JOIN sys.query_store_runtime_stats AS rs1 ON p1.plan_id = rs1.plan_id 
                      JOIN sys.query_store_runtime_stats_interval AS rsi1 ON rsi1.runtime_stats_interval_id = rs1.runtime_stats_interval_id 
                      JOIN sys.query_store_plan AS p2 ON q.query_id = p2.query_id 
                      JOIN sys.query_store_runtime_stats AS rs2 ON p2.plan_id = rs2.plan_id 
                      JOIN sys.query_store_runtime_stats_interval AS rsi2 ON rsi2.runtime_stats_interval_id = rs2.runtime_stats_interval_id
                      WHERE rsi1.start_time > DATEADD(hour, -48, GETUTCDATE()) 
                      AND rsi2.start_time > rsi1.start_time 
                      AND p1.plan_id <> p2.plan_id
                      AND rs2.avg_duration > 2*rs1.avg_duration
                      ORDER BY q.query_id, rsi1.start_time, rsi2.start_time") #Queries that recently regressed in performance (comparing different point in time looking queries performing 2 times slower)"
                       
 $QueriesDBMaster = @("SELECT event_data,CAST(event_data as XML).value('(/event/@timestamp)[1]', 'varchar(19)') AS timestamp 
                      ,CAST(event_data as XML).value('(/event/data[@name=""error""]/value)[1]', 'INT') AS error
                      ,CAST(event_data as XML).value('(/event/data[@name=""state""]/value)[1]', 'INT') AS state   
                      ,CAST(event_data as XML).value('(/event/data[@name=""is_success""]/value)[1]', 'bit') AS is_success 
                      ,CAST(event_data as XML).value('(/event/data[@name=""database_name""]/value)[1]', 'sysname') AS database_name
                      FROM sys.fn_xe_telemetry_blob_target_read_file('el', null, null, null)
                      where object_name = 'database_xml_deadlock_report'", 
                      "select database_name, convert(varchar(20), start_time,120) as start_time,convert(varchar(20), end_time,120) as end_time, success_count, total_failure_count, connection_failure_count, terminated_connection_count, throttled_connection_count from sys.database_connection_stats" ) 




#Program
try
 {
   for  ($iSrv=0; $iSrv -lt $server.length; $iSrv++) {
    for  ($iDB=0; $iDB -lt $databaseDBUser[$iSrv].length; $iDB++) {
     for  ($i=0; $i -lt $QueriesDBUser.length; $i++) {
        Write-Host -ForegroundColor Green "Starting:" ($server[$iSrv]+"_"+ $databaseDBUser[$iSrv][$iDB]+"_"+$QueriesDBUser[$i])
          Invoke-Sqlcmd -ServerInstance $server[$iSrv] -Database $databaseDBUser[$iSrv][$iDB] -Query $QueriesDBUser[$i] -Username $user[$iSrv] -Password $password[$iSrv] -ConnectionTimeout 60 -QueryTimeout 60 | Select-Object *  | ConvertTo-Json  | Out-File -filePath ($folder+$server[$iSrv]+"_"+$databaseDBUser[$iSrv][$iDB]+"_"+$fecha+$filesDbUser[$i]+".json") -Encoding "UTF8"
        Write-Host -ForegroundColor Green "Completed:" ($server[$iSrv]+"_"+ $databaseDBUser[$iSrv][$iDB]+"_"+$QueriesDBUser[$i])
        }
      }
     }
   for  ($i=0; $i -lt $QueriesDBMaster.length; $i++) {
     for  ($iSrv=0; $iSrv -lt $server.length; $iSrv++) {
        Write-Host -ForegroundColor Green "Starting:" ($server[$iSrv]+"_"+$QueriesDBMaster[$i]  )
          Invoke-Sqlcmd -ServerInstance $server[$iSrv] -Database $databaseDBMaster -Query $QueriesDBMaster[$i] -Username $user[$iSrv] -Password $password[$iSrv] -ConnectionTimeout 60 -QueryTimeout 60 | Select-Object *  | ConvertTo-Json  | Out-File -filePath ($folder+$server[$iSrv]+"_"+$fecha+$filesDbMaster[$i]+".json") -Encoding "UTF8"
        Write-Host -ForegroundColor Green "Completed:" ($server[$iSrv]+"_"+$QueriesDBMaster[$i])
      }  
     }
  }
catch
  {
    Write-Host -ForegroundColor DarkYellow "You're WRONG"
    Write-Host -ForegroundColor Magenta $Error[0].Exception
  }
finally
{
 Write-Host -ForegroundColor Cyan "It's finally over..."
}
