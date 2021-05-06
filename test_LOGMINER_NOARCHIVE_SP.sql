SET SERVEROUTPUT ON SIZE 1000000
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
       
BEGIN

  LOGMINER_NOARCHIVE_SP (i_scn    => 7940296591377,
  i_table_whilelist => 'LS_EBAO.TEST_T_ADDRESS, TEST_T_CONTACT_BENE, TEST_T_INSURED_LIST, TEST_T_POLICY_HOLDER',
              o_recordset => l_cursor);
  
  LOOP 
    FETCH l_cursor
    INTO  scn, cscn, COMMIT_SCN, timestamp, COMMIT_TIMESTAMP
      , operation_code, operation,seg_owner, table_name ,row_id, sql_redo;
    
    EXIT WHEN l_cursor%NOTFOUND;
    
    DBMS_OUTPUT.PUT_LINE(scn || ' | ' || cscn || ' | ' || COMMIT_SCN 
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