create or replace PROCEDURE LOGMINER_NOARCHIVE_SP2
(
  i_commit_scn NUMBER
  , i_table_whilelist VARCHAR2
  , o_recordset OUT SYS_REFCURSOR
) AS 

  v_commit_scn NUMBER;
  v_min_first_change# NUMBER;
  v_group NUMBER := 0;
  v_index NUMBER := 0;
  
  v_query_str   VARCHAR2(1000);
  
  v_table_whilelist VARCHAR2(1000);
  
  v_now TIMESTAMP;

BEGIN
  DBMS_OUTPUT.PUT_LINE('input commit scn:' || i_commit_scn);
  
  v_table_whilelist := i_table_whilelist;
  
  v_commit_scn := i_commit_scn;

  select min(first_change#) into v_min_first_change# from v$log;
  
  DBMS_OUTPUT.PUT_LINE('v_min_first_change#:' || v_min_first_change# || ', v_commit_scn:' || v_commit_scn);
  
  v_index := 0;
  for rec in (
      select a.group#, b.member from v$log a inner join v$logfile b on a.group# = b.group#
      order by sequence#, member
  ) loop
    if (v_group !=  rec.group#) then  
        v_group := rec.group#;
        v_index := v_index + 1;
        
        if (v_index = 1) then
          sys.dbms_logmnr.add_logfile(
            LogFileName=> rec.member,
            Options=> sys.dbms_logmnr.new);
            DBMS_OUTPUT.PUT_LINE(rec.group# || ', new: ' || rec.member);
        else
          sys.dbms_logmnr.add_logfile(
            LogFileName=> rec.member,
            Options=> sys.dbms_logmnr.addfile);
            DBMS_OUTPUT.PUT_LINE(rec.group# || ', add file: ' || rec.member);
        end if;
    end if;
  end loop;
  
  
  SYS.DBMS_LOGMNR.START_LOGMNR(  
    OPTIONS =>  SYS.DBMS_LOGMNR.SKIP_CORRUPTION
    + SYS.DBMS_LOGMNR.NO_SQL_DELIMITER
    + SYS.DBMS_LOGMNR.NO_ROWID_IN_STMT
    + SYS.DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG
    + SYS.DBMS_LOGMNR.COMMITTED_DATA_ONLY
    + SYS.DBMS_LOGMNR.STRING_LITERALS_IN_STMT);
    
  v_now := systimestamp;
  
  INSERT INTO LOGMNR_CONTENTS_LOG (
  CON_ID,SRC_CON_GUID,SRC_CON_DBID,SRC_CON_UID,SRC_CON_ID,SRC_CON_NAME,CLIENT_ID,
EDITION_NAME,OBJECT_ID,CSCN,SAFE_RESUME_SCN,UNDO_VALUE,REDO_VALUE,STATUS,INFO,CSF,
SSN,RS_ID,SQL_UNDO,SQL_REDO,DATA_OBJD#,DATA_OBJV#,DATA_OBJ#	,DATA_BLK#,ABS_FILE#,UBASQN	,
UBAREC,UBABLK,UBAFIL	,RBABYTE,RBABLK	,RBASQN,SEQUENCE#,THREAD#,SESSION_INFO,SERIAL#	,
SESSION#,AUDIT_SESSIONID,MACHINE_NAME,OS_USERNAME,USERNAME,ROW_ID,TABLE_SPACE,SEG_TYPE_NAME,SEG_TYPE,
TABLE_NAME,SEG_NAME,SEG_OWNER,ROLLBACK,OPERATION_CODE,OPERATION,TX_NAME,PXID,PXIDSQN,PXIDSLT,PXIDUSN,
XID,XIDSQN,XIDSLT,XIDUSN,COMMIT_TIMESTAMP,START_TIMESTAMP,TIMESTAMP,COMMIT_SCN,START_SCN,SCN,
INSERT_TIMESTAMP)
      SELECT CON_ID,SRC_CON_GUID,SRC_CON_DBID,SRC_CON_UID,SRC_CON_ID,SRC_CON_NAME,CLIENT_ID,
EDITION_NAME,OBJECT_ID,CSCN,SAFE_RESUME_SCN,UNDO_VALUE,REDO_VALUE,STATUS,INFO,CSF,
SSN,RS_ID,SQL_UNDO,SQL_REDO,DATA_OBJD#,DATA_OBJV#,DATA_OBJ#	,DATA_BLK#,ABS_FILE#,UBASQN	,
UBAREC,UBABLK,UBAFIL	,RBABYTE,RBABLK	,RBASQN,SEQUENCE#,THREAD#,SESSION_INFO,SERIAL#	,
SESSION#,AUDIT_SESSIONID,MACHINE_NAME,OS_USERNAME,USERNAME,ROW_ID,TABLE_SPACE,SEG_TYPE_NAME,SEG_TYPE,
TABLE_NAME,SEG_NAME,SEG_OWNER,ROLLBACK,OPERATION_CODE,OPERATION,TX_NAME,PXID,PXIDSQN,PXIDSLT,PXIDUSN,
XID,XIDSQN,XIDSLT,XIDUSN,COMMIT_TIMESTAMP,START_TIMESTAMP,TIMESTAMP,COMMIT_SCN,START_SCN,SCN, 
v_now as INSERT_TIMESTAMP
      FROM  v$logmnr_contents b
      WHERE 
        (OPERATION_CODE in (1,2,3) and COMMIT_SCN >= v_commit_scn
          and 
           (seg_owner || '.' || table_name)
           in (select regexp_substr(v_table_whilelist,'[^,]+', 1, level) from dual
           connect by regexp_substr(v_table_whilelist, '[^,]+', 1, level) is not null)
          and 
            b.RS_ID || b.SSN not in (select RS_ID || SSN from LOGMNR_CONTENTS_LOG where COMMIT_SCN >= v_commit_scn) 
        );
        
        
  OPEN o_recordset FOR
    SELECT SCN, COMMIT_SCN, TIMESTAMP, COMMIT_TIMESTAMP
      , OPERATION_CODE, OPERATION, SEG_TYPE_NAME, SEG_OWNER, SEG_NAME, TABLE_NAME ,SQL_REDO, ROW_ID, CSF 
      , RS_ID, SSN, INSERT_TIMESTAMP 
    FROM 
      LOGMNR_CONTENTS_LOG
    WHERE INSERT_TIMESTAMP = v_now;
      
END LOGMINER_NOARCHIVE_SP2;