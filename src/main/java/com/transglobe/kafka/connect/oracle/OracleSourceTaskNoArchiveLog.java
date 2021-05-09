package com.transglobe.kafka.connect.oracle;

import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.BEFORE_DATA_ROW_FIELD;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.DATA_ROW_FIELD;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.DOT;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.OPERATION_FIELD;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.SCN_FIELD;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.SEG_OWNER_FIELD;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.SQL_REDO_FIELD;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.TABLE_NAME_FIELD;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.TIMESTAMP_FIELD;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.transglobe.kafka.connect.oracle.models.Data;
import com.transglobe.kafka.connect.oracle.models.DataSchemaStruct;
import oracle.jdbc.OracleTypes;

/**
 *  
 * @author Erdem Cer (erdemcer@gmail.com)
 */

public class OracleSourceTaskNoArchiveLog extends SourceTask {
	static final Logger log = LoggerFactory.getLogger(OracleSourceTaskNoArchiveLog.class);
	private String dbName;  
	private Long streamOffsetScn;
	private Long streamOffsetCommitScn;
	private String streamOffsetRowId;  
	//	private Long streamOffsetCtrl;
	private String topic=null;  
	public OracleSourceConnectorConfigNoArchiveLog config;
	private OracleSourceConnectorUtilsNoArchiveLog utils;
	private static Connection dbConn;
	private boolean closed=false;
	private DataSchemaStruct dataSchemaStruct;

	@Override
	public String version() {
		return VersionUtil.getVersion();
	}

	public static Connection getThreadConnection(){
		return dbConn;
	}

	public static void closeDbConn() throws SQLException{
		dbConn.close();
	}


	@Override
	public void start(Map<String, String> map) {
		log.info(">>>Oracle Kafka Connector is starting...");
		config=new OracleSourceConnectorConfigNoArchiveLog(map);    
		topic=config.getTopic();
		dbName=config.getDbNameAlias();
		//	parseDmlData=config.getParseDmlData();

		try {
			dbConn = new OracleConnection().connect(config);
			utils = new OracleSourceConnectorUtilsNoArchiveLog(dbConn, config);

			Map<String,Object> offset = context.offsetStorageReader().offset(Collections.singletonMap("logminer", dbName));

			if (config.getResetOffset() || offset == null){
				log.info(">>>Resetting offset with new SCN");
				streamOffsetScn = getCurrentScn();
				streamOffsetCommitScn = streamOffsetScn;
				streamOffsetRowId="";     
				log.info(">>>reset");
			} else {
				Object lastRecordedOffset = offset.get("scn");
				Object commitScnPositionObject = offset.get("commitScn");
				Object rowIdPositionObject = offset.get("rowId");        
				streamOffsetScn = (lastRecordedOffset != null) ? Long.parseLong(String.valueOf(lastRecordedOffset)) : 0L;
				streamOffsetCommitScn = (commitScnPositionObject != null) ? Long.parseLong(String.valueOf(commitScnPositionObject)) : 0L;
				streamOffsetRowId = (rowIdPositionObject != null) ? (String) rowIdPositionObject : "";
				log.info(">>>with offset");
			}
			log.info(">>>Offset values={} , scn:{},commitscn:{},rowid:{}",streamOffsetScn,streamOffsetCommitScn,streamOffsetRowId);        

		}catch(SQLException e){
			throw new ConnectException("Error at database tier, Please check : "+e.toString());
		}
	}    

	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		log.info(">>>>>>>>> poll() sleep 2 secs");    
		Thread.sleep(2000);
		
		ArrayList<SourceRecord> records = new ArrayList<>();
		CallableStatement cstmt = null;
		ResultSet resultSet = null;
		try {

			log.info(">>>>>>>>> sstreamOffsetScn={}, treamOffsetCommitScn={}", streamOffsetScn, streamOffsetCommitScn);  

			cstmt = dbConn.prepareCall("{ call LOGMINER_NOARCHIVE_SP(?,?,?,?,?) }");
			cstmt.setLong(1, streamOffsetScn);
			cstmt.setLong(2, streamOffsetCommitScn);
			cstmt.setString(3, config.getTableWhiteList());
			cstmt.registerOutParameter(4, Types.BIGINT);

			// alternative
			//cstmt.registerOutParameter(5, Types.REF_CURSOR);
			cstmt.registerOutParameter(5, OracleTypes.CURSOR);

			cstmt.execute();

			Long currentScn = cstmt.getLong(4);
			log.info(">>>>>>>>> currentScn={}", currentScn);

			resultSet = (ResultSet) cstmt.getObject(5);

			Long scn;
			Long commitScn;
			Date timestamp;
			Date commitTimestamp;
			Integer operationCode;
			String operation;
			String segOwner;
			String tableName;
			String rowId;
			String sqlRedo;
			int count = 0;
			while (resultSet.next()) {
				count++;
				scn = resultSet.getLong("SCN");
				commitScn = resultSet.getLong("COMMIT_SCN");
				timestamp = resultSet.getDate("TIMESTAMP");
				commitTimestamp = resultSet.getDate("COMMIT_TIMESTAMP");
				operationCode = resultSet.getInt("OPERATION_CODE");
				operation = resultSet.getString("OPERATION");
				segOwner = resultSet.getString("SEG_OWNER");
				tableName = resultSet.getString("TABLE_NAME");
				rowId = resultSet.getString("ROW_ID");
				sqlRedo = resultSet.getString("SQL_REDO");

				if (operation.equals("DDL")) {
					commitScn = 0L;
					tableName = "_GENERIC_DDL";
					log.info("+++++++++++++++++++++ DDL ++++++++++++++++++++++++++"); 
				}

				topic = config.getTopic().equals("") ? ("etl"+DOT+segOwner+DOT+tableName).toUpperCase() : config.getTopic();
				topic = topic.toLowerCase();


				dataSchemaStruct = utils.createDataSchema(segOwner, tableName, sqlRedo, operation);

				log.info(">>>>>>>>> dataSchemaStruct={}", dataSchemaStruct);  

				Map<String,String> sourcePartition =  Collections.singletonMap("logminer", dbName);
				Map<String,String> sourceOffset = new HashMap<String,String>();
				sourceOffset.put("scn", scn.toString());
				sourceOffset.put("commitScn", commitScn.toString());
				sourceOffset.put("rowId", rowId);


				Data row = new Data(scn, segOwner, tableName, sqlRedo, new Timestamp(timestamp.getTime()), operation);

				Schema schema = dataSchemaStruct.getDmlRowSchema();
				Struct struct = setValueV2(row, dataSchemaStruct);
//				log.info(">>>>>>>>> sourcePartition={}", sourcePartition);   
//				log.info(">>>>>>>>> sourceOffset={}", sourceOffset); 
//				log.info(">>>>>>>>> topic={}", topic); 
//				log.info(">>>>>>>>> schema={}", schema); 
//				log.info(">>>>>>>>> struct={}", struct); 
				records.add(new SourceRecord(sourcePartition, sourceOffset, topic,  schema, struct));

//				log.info(">>>>>>>>> return records={}", records);   

				streamOffsetScn = scn;			
				if (operation.equals("DDL")) {
					streamOffsetCommitScn = scn;
				} else {
					streamOffsetCommitScn = commitScn;
				}
			}
			log.info(">>>>>>>>> count={}", count);
			if (count == 0) {
				streamOffsetCommitScn = currentScn;
			} else {
				streamOffsetCommitScn++;
				log.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> count={}", count);
			}

			return records;

		} catch (Exception e) {
			log.error(ExceptionUtils.getStackTrace(e));
		}  finally {
			log.info(">>>>>>>>> finally close resultset and statement");
			try {
				if (resultSet != null) resultSet.close();
				if (cstmt != null) cstmt.close();
			} catch (SQLException e) {
				log.error(ExceptionUtils.getStackTrace(e));
			}


		}
		return null;
	}

	@Override
	public void stop() {
		log.info(">>>Stop called for logminer");
		this.closed=true;
		try {            
			log.info(">>>Logminer session cancel");
			//			if (logMinerSelect != null) logMinerSelect.cancel();

			CallableStatement s = dbConn.prepareCall("begin \nSYS.DBMS_LOGMNR.END_LOGMNR; \nend;");
			s.execute();
			s.close();
			if (dbConn!=null){
				log.info("Closing database connection.Last SCN : {}",streamOffsetScn);        
				//				logMinerSelect.close();
				//				logMinerStartStmt.close();        
				dbConn.close();
			}
		} catch (SQLException e) {log.error(e.getMessage());}


	}

	private Struct setValueV2(Data row,DataSchemaStruct dataSchemaStruct) {    
		Struct valueStruct = new Struct(dataSchemaStruct.getDmlRowSchema())
				.put(SCN_FIELD, row.getScn())
				.put(SEG_OWNER_FIELD, row.getSegOwner())
				.put(TABLE_NAME_FIELD, row.getSegName())
				.put(TIMESTAMP_FIELD, row.getTimeStamp())
				.put(SQL_REDO_FIELD, row.getSqlRedo())
				.put(OPERATION_FIELD, row.getOperation())
				.put(DATA_ROW_FIELD, dataSchemaStruct.getDataStruct())
				.put(BEFORE_DATA_ROW_FIELD, dataSchemaStruct.getBeforeDataStruct());
		return valueStruct;

	}  

	private Long getCurrentScn() throws SQLException {
		String sql = "select min(current_scn) CURRENT_SCN from gv$database";
		Statement stmt = dbConn.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		Long currentScn = null;
		while (rs.next()) {
			currentScn = rs.getLong("CURRENT_SCN");
		}
		rs.close();
		stmt.close();

		return currentScn;
	}
}