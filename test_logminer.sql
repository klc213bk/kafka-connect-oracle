--select current_scn  from v$database;
-- current_scn = 7946423650902

 select a.group#, b.member from v$log a inner join v$logfile b on a.group# = b.group#
      where sequence# >= 
      (
        select sequence# from v$log 
        where first_change# <= 7946423650902 and 7946423650902 < next_change#
      )
      order by sequence#, member;
      
execute sys.dbms_logmnr.add_logfile( LogFileName=> '/u02/oradata/ebaouat1/system/redo03a.log', Options=> sys.dbms_logmnr.new);
      
execute SYS.DBMS_LOGMNR.START_LOGMNR(STARTSCN => 7946423650902,  -
    OPTIONS =>  SYS.DBMS_LOGMNR.SKIP_CORRUPTION -
    + SYS.DBMS_LOGMNR.NO_SQL_DELIMITER -
    + SYS.DBMS_LOGMNR.NO_ROWID_IN_STMT -
    + SYS.DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG - 
    + SYS.DBMS_LOGMNR.COMMITTED_DATA_ONLY - 
    + SYS.DBMS_LOGMNR.STRING_LITERALS_IN_STMT);

select *  
FROM  v$logmnr_contents;


