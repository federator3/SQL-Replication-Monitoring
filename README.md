# SQL-Replication-Monitoring

This repository contains SQL scripts to

    monitor IBM SQL Replication runtime processes
        sqlrep_monitor_capture_EN.sql

How to run the SQL Capture scripts:

    db2 connect to <capture_server>
    db2 set current schema = '<capture_schema>'
    db2 -tvf

All scripts create an easily readable report.
