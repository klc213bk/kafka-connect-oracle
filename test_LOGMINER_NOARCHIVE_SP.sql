SET SERVEROUTPUT ON ;
DECLARE
  l_cursor  SYS_REFCURSOR;
  scn NUMBER;
  cscn NUMBER;
  COMMIT_SCN NUMBER;
  timestamp DATE;
  COMMIT_TIMESTAMP DATE;
  operation_code NUMBER;
  operation VARCHAR2(32);
  seg_owner VARCHAR2(100);
  table_name VARCHAR2(100); 
  sql_redo VARCHAR2(4000);
  row_id  VARCHAR2(18);
  o_current_scn NUMBER;     
BEGIN

  LOGMINER_NOARCHIVE_SP (i_scn    => 2814544,
  i_commit_scn    => 2814544,
  i_table_whilelist => 'PMUSER.T_POLICY_HOLDER,PMUSER.T_INSURED_LIST,PMUSER.T_CONTRACT_BENE,PMUSER.T_ADDRESS',
  o_current_scn => o_current_scn,
              o_recordset => l_cursor);
  
  DBMS_OUTPUT.PUT_LINE('o_current_scn:' || o_current_scn);
  
  LOOP 
    FETCH l_cursor
    INTO  scn, COMMIT_SCN, timestamp, COMMIT_TIMESTAMP
      , operation_code, operation,seg_owner, table_name ,row_id, sql_redo;
    
    EXIT WHEN l_cursor%NOTFOUND;
    
    DBMS_OUTPUT.PUT_LINE(scn || ' | ' || COMMIT_SCN 
    || ' | ' || to_char(timestamp, 'YYYY-MM-DD HH24:MI:SS') || ' | ' || to_char(COMMIT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') || ' | ' || operation_code || ' | ' || operation
    || ' | ' || seg_owner || ' | ' || table_name  || ' | ' || row_id || ' | ' || sql_redo);
  END LOOP;    
  
    CLOSE l_cursor;
    
    /* 
  LOOP 
    FETCH l_cursor
    INTO  scn, cscn, timestamp, COMMIT_TIMESTAMP
      , operation_code, operation,seg_owner, table_name, sql_redo ,row_id;
    
    EXIT WHEN l_cursor%NOTFOUND;
   
   DBMS_OUTPUT.PUT_LINE('cccc');
 
    DBMS_OUTPUT.PUT_LINE( rec.start_scn 
    || ',' || rec.cscn
    || ',' || rec.xid
    || ',' || rec.timestamp
    || ',' || rec.COMMIT_TIMESTAMP
    || ',' || rec.operation_code
    || ',' || rec.operation
    || ',' || rec.seg_owner
    || ',' || rec.table_name
    || ',' || rec.SEQUENCE#
     || ',' || rec.row_id
     || ',' ||rec.sql_redo
        );
   
    
  END LOOP;

  
   */
END;