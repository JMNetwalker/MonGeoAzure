# MonGeoAzure

This tool was developed, as an example, for monitoring any geo-replicated database(s) and server(s) in Azure SQL Database. Every time that you executed this PowerShell command you will have a JSON file in the following format: 

    servername.database.windows.net_databasename_YYYYMMDDHHNNSS_title_of_query.json

##How to parametrize this tool?

  - *$server* is an array for the servers that you want to monitor.
  - $databaseDBUser is an array for the user databases that you want to obtain the details.
  - $databaseDBUser[x], you need to specify the user database name per server. Every x represent a server that correspond with $server.
  - $user is an array for the user name for every server that you want to connect.
  - $password is an array for the password for every user name for every server that you want to connect.
  - $filesDbUser is an array with the name of the files that will be generated for every user query that you want to execute, correspond every query that you have configured in $QueriesDBUser.
  - $filesDbMaster is an array with the name of the files that will be generated for every server from the master database, correspond
                  everu query that you have configured in $QueriesDBMaster
  - $QueriesDBUser is an array for every TSQL/query that you want to execute for every user database.
  - $QueriesDBMaster is an array for every TSQL/query that you want to execute in the master database for every server.

##How to analyze the data?

  - Once you have the JSON file, 
  
   SELECT book.* FROM OPENROWSET (BULK '<folder>\servername.database.windows.net_database_YYYYMMDDHHNNSS_ResourceDB.json',  SINGLE_cLOB) as j CROSS APPLY OPENJSON(BulkColumn) WITH( [end_time] datetime , [avg_cpu_percent] [decimal](5, 2), [avg_data_io_percent] [decimal](5, 2) , [avg_log_write_percent] [decimal](5, 2) , [avg_memory_usage_percent] [decimal](5, 2), [xtp_storage_percent] [decimal](5, 2), [max_worker_percent] [decimal](5, 2) , [max_session_percent] [decimal](5, 2) , [dtu_limit] [int] ) AS book
