--#SET TERMINATOR  ;
-- ---------------------------------------------------------------------
-- Q Capture Monitor
-- Report zum schnellen Aufspüren von Unterbrechungen und
-- Ausnahmebedingungen bei der SQL Replication. Queries:
--   100: C-OPE - Apply operational
--   110: C-LAT - Capture latency
--   140: C-REG - Inactive / new registrations
-- ---------------------------------------------------------------------
-- Der Report ist sowohl am Quellsystem periodisch auszuführen (z.B.
-- alle 5 Minuten). Spalte SEVerity zeigt an, ob es sich bei der
-- angezeigten Zeile um eine INFO, eine WARNING oder um einen zu
-- korrigierenden ERROR handelt.
-- ---------------------------------------------------------------------
-- Anpassung vor Inbetriebnahme: (search "-- change")
-- SET CURRENT SCHEMA anpassen und durch
-- verwendete Capture- bzw. Apply-Schema ersetzen
-- ---------------------------------------------------------------------
-- Status: In Erprobung
--
-- ---------------------------------------------------------------------
-- Änderungen / Ergänzungen
--  - 10.10.2020: Initial version
--  - 27.11.2020: latency: monitor interval seconds and not milliseconds
--  - 27.11.2020: added C-REG: inactive or new regstrations
--  - 03.12.2020: added C-OPE Operational State
--  - 09.12.2020: new algorithm to calculate the capture latency
--                a) corrected a bug which previously calculated the
--                   wrong latency when midnight was between monitor
--                   time and current log time
--                b) current log time is only advanced when data has
--                   been captured. To calculate a more realistic
--                   latency in phases with no capturing, the maximum of
--                   current log time and synchtime is used to calculate
--                   the latency
-- ---------------------------------------------------------------------

-- change before execution ---------------------------------------------
-- connect to '<capture_server>';
-- set current schema = '<capture_schema>';
-- change before execution ---------------------------------------------

-- uncomment the following line (CREATE VIEW) when using CREATE VIEW -
-- comment when used as query
-- create view G000.QREP_MONITOR_CAPTURE as


select

-- uncomment the following line (ordercol) when using CREATE VIEW -
-- comment when used as query
-- x.ordercol,

current timestamp as CHECK_TS,
case
  when length(x.program) <= 18
  then substr(x.program, 1 , 18)
  else substr(x.program, 1 , 16) concat '..'
end as PROGRAM,
x.CURRENT_SERVER,
x.MTYP,
x.SEV,
x.MTXT

from

(
-- ---------------------------------------------------------------------
-- Query 100:

--    DE: Komponente: SQL Capture
--    Ausschnitt: Capture operationaler Status
--    EN: Component: SQL Capture
--    Section: Capture operational state

select

100 as ordercol,
'ASNCAP(' concat trim(current schema) concat ')' as program,
current server as CURRENT_SERVER,
'C-OPE' as MTYP,

case when y.MONITOR_TIME <
       y.EXPECTED_TS_LAST_MONITOR_RECORD - 5 seconds
     then 'ERROR'
     else 'INFO'
end as SEV,

case when y.MONITOR_TIME <
       y.EXPECTED_TS_LAST_MONITOR_RECORD - 5 seconds
-- DE
       then 'SQL Capture nicht in Betrieb oder gestoert seit '
          concat trim(VARCHAR(y.MONITOR_TIME)) concat '.'
-- EN
--     then 'SQL Capture down or not operational since '
--        concat trim(VARCHAR(y.MONITOR_TIME)) concat '.'
-- DE
     else 'SQL Capture in Betrieb '
-- EN
--   else 'SQL Capture operational '
end as MTXT

from

(

select
cm.monitor_time,
current timestamp - cp.monitor_interval seconds
  AS EXPECTED_TS_LAST_MONITOR_RECORD

from ibmsnap_capmon cm,
     ibmsnap_capparms cp

-- only the most current rows
where cm.monitor_time = (select max(monitor_time)
                         from ibmsnap_capmon)

) y

UNION

-- Query 110:
--    DE: Komponente: SQL Capture
--    Ausschnitt: Capture Process
--    EN: Component: SQL Capture
--    Section: Capture Process

select
110 as ordercol,
'ASNCAP(' concat trim(current schema) concat ')' as program,
current server as CURRENT_SERVER,
'C-LAT' as MTYP,

case when y.CAPTURE_LATENCY_SEC > 1200
     then 'ERROR'
     when y.CAPTURE_LATENCY_SEC > 300
     then 'WARNING'
     else 'INFO'
end as SEV,
case when y.CAPTURE_LATENCY_SEC > 1200

-- DE
     then 'SQL Capture Latenz > 1200 Sekunden. CAPTURE_LATENCY='
-- EN
--   then 'SQL Capture latency > 1200 seconds. CAPTURE_LATENCY='
          concat
          coalesce(trim(VARCHAR(y.CAPTURE_LATENCY_SEC)) , 'UNKNOWN')
          concat ' s, MEMORY:'
          concat trim(VARCHAR(y.CURRENT_MEMORY_MB))
          concat '/'
          concat varchar(y.memory_limit)
          concat ' MB, TRANS_SPILLED='
          concat trim(VARCHAR(y.TRANS_SPILLED)) concat '.'

     when y.CAPTURE_LATENCY_SEC > 300

-- DE
     then 'SQL Capture Latenz > 300 Sekunden. CAPTURE_LATENCY='
-- EN
--   then 'SQL Capture latency > 300 seconds. CAPTURE_LATENCY='
          concat
          coalesce(trim(VARCHAR(y.CAPTURE_LATENCY_SEC)) , 'UNKNOWN')
          concat ' s, MEMORY:'
          concat trim(VARCHAR(y.CURRENT_MEMORY_MB))
          concat '/'
          concat varchar(y.memory_limit)
          concat ' MB, TRANS_SPILLED='
          concat trim(VARCHAR(y.TRANS_SPILLED)) concat '.'

-- DE
     else 'SQL Capture Latenz ok (< 300 Sekunden). CAPTURE_LATENCY='
-- EN
--   else 'SQL Capture latency ok (< 300 seconds). CAPTURE_LATENCY='
          concat
          coalesce(trim(VARCHAR(y.CAPTURE_LATENCY_SEC)) , 'UNKNOWN')
          concat ' s, MEMORY: '
          concat trim(VARCHAR(y.CURRENT_MEMORY_MB))
          concat '/'
          concat varchar(y.memory_limit)
          concat ' MB, TRANS_SPILLED='
          concat trim(VARCHAR(y.TRANS_SPILLED)) concat '.'
end as MTXT

from

(

select
current timestamp - dec(monitor_interval) seconds
  AS EXPECTED_TS_LAST_MONITOR_RECORD,
cm.monitor_time,
cp.monitor_interval,

-- 09.12.2020: new logic to calculate the capture latency using
-- TIMESTAMPDIFF(2, ...) which calculates the differnce in seconds.
-- Explicitly, TIMESTAMPDIFF(1, ...) - microseconds - was not used
-- to prevent overflows. Instead, the difference in microseconds ist
-- added to the difference in seconds to get a more precise latency
-- value and preventing overflows. Max. difference without overflow:
-- 68 years (due to integer limits)

-- because current log time on some platforms only gets advanced when
-- data was captured, the maximum of current log time and synctime is
-- used to calculate the latency
case
-- '1900-01-01-00.00.00.000000'  when log reader is not yet established
  when cm.current_log_time = '1900-01-01-00.00.00.000000'
    then NULL
  when microsecond(cm.MONITOR_TIME) >=
                    microsecond(max(cm.current_log_time, cm.synchtime))
-- difference in seconds plus difference in microseconds (1 digit only)
    then TIMESTAMPDIFF(2, CHAR(cm.MONITOR_TIME -
                               max(cm.current_log_time, cm.synchtime)))
            + dec((dec(microsecond(cm.MONITOR_TIME)
                - microsecond(max(cm.current_log_time, cm.synchtime)))
                     / 1000000) , 2 , 1)
-- Expl microsecond(cm.MONITOR_TIME) < microsecond(cm.current_log_time)
-- difference in microseconds (1 digit only) negative in this case.
-- Therefore, the negative value is added to the difference in seconds
-- plus 1
  else TIMESTAMPDIFF(2, CHAR(cm.MONITOR_TIME -
                               max(cm.current_log_time, cm.synchtime)))
            + 1 + dec((dec(microsecond(cm.MONITOR_TIME)
                - microsecond(max(cm.current_log_time, cm.synchtime)))
                     / 1000000) , 2 , 1)
end AS CAPTURE_LATENCY_SEC,


-- this logic calculated the wrong difference between the 2 timestamps
-- when midnight was between the 2 timestamps
-- case
--  when cm.current_log_time <> '1900-01-01-00.00.00.000000' then
--   dec(dec(microsecond(cm.MONITOR_TIME
--              - cm.CURRENT_LOG_TIME)) / 1000000
--   + SECOND(cm.MONITOR_TIME - cm.CURRENT_LOG_TIME)
--   + ((MINUTE(cm.MONITOR_TIME - cm.CURRENT_LOG_TIME)*60) )
--   + (HOUR(cm.MONITOR_TIME - cm.CURRENT_LOG_TIME)*3600)
--   + ((DAYS(cm.MONITOR_TIME)
--              - DAYS(cm.CURRENT_LOG_TIME))*86400) , 12 , 1)
-- else null
-- end as CAPTURE_LATENCY_SEC,

dec(dec(cm.CURRENT_MEMORY) / 1024 / 1024, 5 , 0)
  as CURRENT_MEMORY_MB,
cp.memory_limit,
cm.TRANS_SPILLED
from ibmsnap_capmon cm,
     ibmsnap_capparms cp

where cm.monitor_time = (select max(monitor_time)
                         from ibmsnap_capmon)

) y

UNION

-- Query 140:
--    DE: Komponente: SQL Capture
--    Ausschnitt: Alle inaktiven Registrierungen
--    EN: Component: SQL Capture
--    Section: All inactive registrations

select
140 as ordercol,
'ASNCAP(' concat trim(current schema) concat ')' as program,
current server as CURRENT_SERVER,
'C-REG' as MTYP,
case when (y.NUM_ACT = y.NUM_TOTAL) then 'INFO'
     else 'WARNING'
end as SEV,

case when (y.NUM_ACT = y.NUM_TOTAL) then
-- DE
        'Alle ' concat trim(VARCHAR(y.NUM_TOTAL))
        concat ' Registrierungen aktiv. '
-- EN
--      'All ' concat trim(VARCHAR(y.NUM_TOTAL))
--      concat ' registrations are active. '

	else 
-- DE
 	    trim(VARCHAR(y.NUM_INACT + y.NUM_STOPED)) concat ' von '
        concat trim(VARCHAR(y.NUM_TOTAL))
        concat ' Registrierungen inaktiv.'
        concat ' STATE=I: '
        concat trim(VARCHAR(coalesce(y.NUM_INACT , 0)))
        concat ' STATE=S: '
        concat trim(VARCHAR(coalesce(y.NUM_STOPED , 0)))
        concat '.'

-- EN
--      trim(VARCHAR(y.NUM_INACT + y.NUM_STOPED)) concat ' of '
--      concat trim(VARCHAR(y.NUM_TOTAL))
--      concat ' registrations are inactive.'
--      concat ' STATE=I: '
--      concat trim(VARCHAR(coalesce(y.NUM_INACT , 0)))
--      concat ' STATE=S: '
--      concat trim(VARCHAR(coalesce(y.NUM_STOPED , 0)))
--      concat '.'		

end as MTXT


from

(
SELECT
   coalesce(max(DECODE(state, 'A', ct)), 0) AS NUM_ACT,
   coalesce(max(DECODE(state, 'I', ct)), 0) AS NUM_INACT,
   coalesce(max(DECODE(state, 'S', ct)), 0) AS NUM_STOPED,
   coalesce(max(DECODE(state, 'A', ct)), 0)
   + coalesce(max(DECODE(state, 'I', ct)), 0)
   + coalesce(max(DECODE(state, 'S', ct)), 0) as NUM_TOTAL
FROM (
select state, count(*) as ct
       from ibmsnap_register
       group by state
) r
) y


) x

-- comment the following 2 lines (order by / with ur) when
-- using CREATE VIEW - uncomment when used as query
order by x.ordercol
with ur
;

-- set current schema = user;