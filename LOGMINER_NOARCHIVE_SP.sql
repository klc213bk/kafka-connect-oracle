create or replace PROCEDURE LOGMINER_NOARCHIVE_SP
(
  i_scn NUMBER
  , i_commit_scn NUMBER
  , i_table_whilelist VARCHAR2
  , o_current_scn OUT NUMBER
  , o_recordset OUT SYS_REFCURSOR
) AS 

  v_scn NUMBER;
  v_commit_scn NUMBER;
  v_min_first_change# NUMBER;
  v_group NUMBER := 0;
  v_index NUMBER := 0;
  
  v_query_str   VARCHAR2(1000);
  
  v_table_whilelist VARCHAR2(1000);
  
  e_invalid_input_scn EXCEPTION;
BEGIN
  DBMS_OUTPUT.PUT_LINE('input_scn:' || i_scn || ',input commit scn:' || i_commit_scn);
  
  v_table_whilelist := i_table_whilelist;
   
  select current_scn into o_current_scn from v$database;
  DBMS_OUTPUT.PUT_LINE(' sp o_current_scn:' || o_current_scn);
  
  
  if (i_scn is null or i_scn = 0) then
    v_scn := o_current_scn;
    v_commit_scn := o_current_scn;
  elsif (i_commit_scn is null or i_commit_scn = 0) then
    v_scn := i_scn;
    v_commit_scn := o_current_scn;
  else 
    v_scn := i_scn;
    v_commit_scn := i_commit_scn;
  end if;

  DBMS_OUTPUT.PUT_LINE('v_scn:' || v_scn || ', v_commit_scn:' || v_commit_scn);
  
  select min(first_change#) into v_min_first_change# from v$log;
  if v_scn < v_min_first_change# then
    v_scn := v_min_first_change#;
  end if;
  
  v_index := 0;
  for rec in (
      select a.group#, b.member from v$log a inner join v$logfile b on a.group# = b.group#
      where sequence# >= 
      (
        select sequence# from v$log 
        where first_change# <= v_scn and v_scn < next_change#
      )
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
  
  
  SYS.DBMS_LOGMNR.START_LOGMNR(STARTSCN => v_scn,  
    OPTIONS =>  SYS.DBMS_LOGMNR.SKIP_CORRUPTION
    + SYS.DBMS_LOGMNR.NO_SQL_DELIMITER
    + SYS.DBMS_LOGMNR.NO_ROWID_IN_STMT
    + SYS.DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG
    + SYS.DBMS_LOGMNR.COMMITTED_DATA_ONLY
    + SYS.DBMS_LOGMNR.STRING_LITERALS_IN_STMT);
    
    
  OPEN o_recordset FOR
      SELECT THREAD#, SCN, COMMIT_SCN, START_SCN, (XIDUSN||'.'||XIDSLT||'.'||XIDSQN) AS XID, TIMESTAMP, COMMIT_TIMESTAMP
      , OPERATION_CODE, OPERATION, STATUS, SEG_TYPE_NAME, INFO, ROLLBACK, SEG_OWNER, SEG_NAME, TABLE_NAME , USERNAME, SQL_REDO, ROW_ID, CSF 
      , TABLE_SPACE, SESSION_INFO, RS_ID, SSN, RBASQN, RBABLK, SEQUENCE#, TX_NAME 
      FROM  v$logmnr_contents
      WHERE 
        (OPERATION_CODE in (1,2,3) and COMMIT_SCN >= v_commit_scn and COMMIT_SCN < o_current_scn
          and 
           (seg_owner || '.' || table_name)
           in (select regexp_substr(v_table_whilelist,'[^,]+', 1, level) from dual
           connect by regexp_substr(v_table_whilelist, '[^,]+', 1, level) is not null)
        )
        order by scn  
     ;

EXCEPTION   
  WHEN e_invalid_input_scn THEN
    RAISE_APPLICATION_ERROR (-20201, 'scn=' || v_scn || ' ' || 'min scn=' || v_min_first_change#);
  
END LOGMINER_NOARCHIVE_SP;