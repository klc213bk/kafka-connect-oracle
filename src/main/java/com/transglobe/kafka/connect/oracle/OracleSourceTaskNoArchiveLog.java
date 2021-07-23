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

import org.apache.commons.lang3.StringUtils;
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

	private Map<String, Map<String, Object>> csf0Map;
	private Map<String, Map<String, Object>> csf1Map;

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

			if (StringUtils.isBlank(config.getStartScn())) {
				if (config.getResetOffset() || offset == null){
					log.info(">>>Resetting offset with registered or current SCN");
					streamOffsetScn = getCurrentScn();
					streamOffsetCommitScn = streamOffsetScn;
					streamOffsetRowId="";     
				} else {
					Object lastRecordedOffset = offset.get("scn");
					Object commitScnPositionObject = offset.get("commitScn");
					Object rowIdPositionObject = offset.get("rowId");        
					streamOffsetScn = (lastRecordedOffset != null) ? Long.parseLong(String.valueOf(lastRecordedOffset)) : 0L;
					streamOffsetCommitScn = (commitScnPositionObject != null) ? Long.parseLong(String.valueOf(commitScnPositionObject)) : 0L;
					streamOffsetRowId = (rowIdPositionObject != null) ? (String) rowIdPositionObject : "";
					log.info(">>>with offset");
				}
			} else {
				streamOffsetScn = Long.valueOf(config.getStartScn());
				streamOffsetCommitScn = streamOffsetScn;
				streamOffsetRowId="";     
				log.info(">>>with startscn={}", config.getStartScn());

			}
			log.info(">>>Offset scn:{},commitscn:{},rowid:{}",streamOffsetScn,streamOffsetCommitScn,streamOffsetRowId);        

			csf0Map = new HashMap<>();
			csf1Map = new HashMap<>();
		}catch(SQLException e){
			throw new ConnectException("Error at database tier, Please check : "+e.toString());
		}
	}    

	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		//		log.info(">>>>>>>>> poll() sleep 2 secs");    
		Thread.sleep(2000);

		ArrayList<SourceRecord> records = new ArrayList<>();
		CallableStatement cstmt = null;
		ResultSet resultSet = null;
		try {

			//			log.info(">>>>>>>>> sstreamOffsetScn={}, treamOffsetCommitScn={}", streamOffsetScn, streamOffsetCommitScn);  

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
			//			log.info(">>>>>>>>> currentScn={}", currentScn);

			resultSet = (ResultSet) cstmt.getObject(5);

			Long threadNum;
			Long scn;
			Long commitScn;
			Long startScn;
			String xid;
			Date timestamp;
			Date commitTimestamp;
			Integer operationCode;
			String operation;
			Integer status; 
			String segTypeName;
			String info;
			Integer rollback;
			String segOwner;
			String segName;
			String tableName;
			String username;
			String sqlRedo;
			String rowId;
			Integer csf;
			String tableSpace;
			String sessionInfo;
			String rsId;
			Long rbasqn;
			Long rbablk;
			Long sequenceNum;
			String txName;

			//			Map<String, Map<String, Object>> sqlRedoKeepMap = null;

			int count = 0;
			while (resultSet.next()) {
				count++;
				log.info(">>>>>>>>> count={}, streamOffsetScn={}, treamOffsetCommitScn={}", count, streamOffsetScn, streamOffsetCommitScn);  

				Map<String, Object> data = new HashMap<>();
				try {
					threadNum = resultSet.getLong("THREAD#");
					scn = resultSet.getLong("SCN");
					commitScn = resultSet.getLong("COMMIT_SCN");
					startScn = resultSet.getLong("START_SCN");
					xid = resultSet.getString("XID");
					timestamp = resultSet.getDate("TIMESTAMP");
					commitTimestamp = resultSet.getDate("COMMIT_TIMESTAMP");
					operationCode = resultSet.getInt("OPERATION_CODE");
					operation = resultSet.getString("OPERATION");
					status = resultSet.getInt("STATUS");; 
					segTypeName = resultSet.getString("SEG_TYPE_NAME");
					info = resultSet.getString("INFO");
					rollback = resultSet.getInt("ROLLBACK");
					segOwner = resultSet.getString("SEG_OWNER");
					segName = resultSet.getString("SEG_NAME");//
					tableName = resultSet.getString("TABLE_NAME");
					username = resultSet.getString("USERNAME");//
					sqlRedo = resultSet.getString("SQL_REDO");
					rowId = resultSet.getString("ROW_ID");
					csf = resultSet.getInt("CSF");
					tableSpace = resultSet.getString("TABLE_SPACE");//
					sessionInfo = resultSet.getString("SESSION_INFO");//
					rsId = resultSet.getString("RS_ID");//
					rbasqn = resultSet.getLong("RBASQN");//
					rbablk = resultSet.getLong("RBABLK");//
					sequenceNum = resultSet.getLong("SEQUENCE#");//
					txName = resultSet.getString("TX_NAME");//


					if (operation.equals("DDL")) {
						commitScn = 0L;
						tableName = "_GENERIC_DDL";
						log.info("+++++++++++++++++++++ DDL ++++++++++++++++++++++++++"); 
					}


					topic = config.getTopic().equals("") ? getTopicName(config, tableName) : config.getTopic();

					data.put("threadNum", threadNum);
					data.put("scn", scn);
					data.put("commitScn", commitScn);
					data.put("startScn", startScn);
					data.put("xid", xid);
					data.put("timestamp", timestamp);
					data.put("commitTimestamp", commitTimestamp);
					data.put("operationCode", operationCode);
					data.put("operation", operation);
					data.put("status", status);
					data.put("segTypeName", segTypeName);
					data.put("info", info);
					data.put("rollback",rollback);
					data.put("segOwner", segOwner);
					data.put("segName", segName);
					data.put("tableName", tableName);
					data.put("username", username);
					data.put("sqlRedo", sqlRedo);
					data.put("rowId", rowId);
					data.put("csf", csf);
					data.put("tableSpace", tableSpace);
					data.put("sessionInfo", sessionInfo);
					data.put("rsId", rsId);
					data.put("rbasqn", rbasqn);
					data.put("rbablk",rbablk);
					data.put("sequenceNum",sequenceNum);
					data.put("txName", txName); 

					data.put("topic", topic);

					if (!operation.equals("DDL")) {
						String key = scn + "-" + commitScn + "-" + rowId;
						String op = sqlRedo.substring(0, 6);
						boolean withOp = false;
						if(StringUtils.equalsIgnoreCase("INSERT", op)
								|| StringUtils.equalsIgnoreCase("UPDATE", op)
								|| StringUtils.equalsIgnoreCase("DELETE", op)) {
							withOp = true;
						} 
						if (withOp && csf.intValue() == 0) {
							// normal complate withOp cf0
							log.info(">>>>>normal case, logminer data={}",data);
						} else if (withOp && csf.intValue() == 1 ) {
							// check if csf0 exists with key
							if (csf0Map.containsKey(key)) {
								String partialRedo = (String)csf0Map.get(key).get("sqlRedo");
								String thisRedo = (String)data.get("sqlRedo");
								String wholeRedo = thisRedo + partialRedo;
								data.put("sqlRedo", wholeRedo);

								// complete withOp cf1, match case A
								log.info(">>>complete withOp cf1, match case A, partialRedo={}", partialRedo);
								log.info("");
								log.info(">>>>> thisRedo={}",thisRedo);
								log.info("");
								log.info(">>>>> data={}",data);

								csf0Map.remove(key);

							} else {
								csf1Map.put(key, data);

								// uncomplete case B, withOp cf1 
								log.info(">>>uncomplete case B, withOp cf1, data={}", data);
								continue;
							}
						} else if (!withOp && csf.intValue() == 0 ) {
							if (csf1Map.containsKey(key)) {
								String partialRedo = (String)csf1Map.get(key).get("sqlRedo");
								String thisRedo = (String)data.get("sqlRedo");
								String wholeRedo = partialRedo + thisRedo;
								data.put("sqlRedo", wholeRedo);

								// assume csf1 contains Op
								// complete withoutOp csf0, match case B
								log.info(">>>complete withoutOp csf0, match case B, partialRedo={}", partialRedo);
								log.info("");
								log.info(">>>>> thisRedo={}",thisRedo);
								log.info("");
								log.info(">>>>> data={}",data);

								csf1Map.remove(key);
							} else {
								if (csf0Map.containsKey(key)) {
									log.error(">>>csf0Map data={}, resultdata={}", csf0Map.get(key), data);
									throw new Exception("duplicate csf 0");
								} else {
									csf0Map.put(key, data);
									// uncomplete case A, withoutOp cf0 
									log.info(">>>uncomplete case A, withoutOp cf0 data={}", data);
									continue;
								}
							}

						} else if (!withOp && csf.intValue() == 1 ) {
							log.error(">>>csf0Mapdata={}, csf1Mapdata={}, resultdata={}", csf0Map.get(key), csf1Map.get(key), data);
							throw new Exception("Should not go here");
						} else {
							throw new Exception("No such operation");
						}
					}

					sqlRedo = (String)data.get("sqlRedo");
					dataSchemaStruct = utils.createDataSchema(segOwner, tableName, sqlRedo, operation);

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
				} catch (Exception e) {
					log.error(">>>>>>>>> Error:" + ExceptionUtils.getStackTrace(e));
				} 

			}
			if (count == 0) {
				streamOffsetCommitScn = currentScn;
			} else {

				streamOffsetCommitScn++;
			}

			return records;

		} catch (Exception e) {
			log.error("Error:" + ExceptionUtils.getStackTrace(e));

			try {
				if (resultSet != null) resultSet.close();
				if (cstmt != null) cstmt.close();

				log.info(">>> dbConn closed={}", dbConn.isClosed());
				while (dbConn.isClosed()) {
					log.info(">>> try to reconnect");
					dbConn = new OracleConnection().connect(config);
					Thread.sleep(50000);
				}
			} catch (SQLException ex) {
				log.error(ExceptionUtils.getStackTrace(ex));
				throw new InterruptedException(ex.getMessage());
			}
		}  finally {
			try {
				if (resultSet != null) resultSet.close();
				if (cstmt != null) cstmt.close();
			} catch (SQLException e) {
				log.error(ExceptionUtils.getStackTrace(e));
				throw new InterruptedException(e.getMessage());
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

			if (dbConn!=null){
				CallableStatement s = dbConn.prepareCall("begin \nSYS.DBMS_LOGMNR.END_LOGMNR; \nend;");
				s.execute();
				s.close();

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

		Long registerdCurrentScn = getLegalRegisteredCurrentScn();
		if (registerdCurrentScn != null) {
			return registerdCurrentScn;
		} else {
			String sql = "select min(current_scn) CURRENT_SCN from gv$database";
			Statement stmt = null;
			ResultSet rs = null;
			Long currentScn = 0L;
			try {
				stmt = dbConn.createStatement();
				rs = stmt.executeQuery(sql);
				while (rs.next()) {
					currentScn = rs.getLong("CURRENT_SCN");
				}
			} finally {
				if (rs != null) rs.close();
				if (stmt != null) stmt.close();

			}

			return currentScn;
		}


	}
	private String getTopicName(OracleSourceConnectorConfigNoArchiveLog config, String tableName) {
		return config.getTopicPattern().replace("%tablename%", StringUtils.lowerCase(tableName));
	}
	private Long getLegalRegisteredCurrentScn() throws SQLException {
		Statement stmt = null;
		ResultSet rs = null;
		String sql = "";
		Long currentScn = null;
		try {
			sql = "select TIME, CURRENT_SCN from t_streaming_register order by time desc fetch next 1 row only";
			stmt = dbConn.createStatement();
			rs = stmt.executeQuery(sql);
			long time = 0;
			while (rs.next()) {
				time = rs.getLong("TIME");
				currentScn = rs.getLong("CURRENT_SCN");
			}
			if (currentScn != null) {
				if (System.currentTimeMillis() - time > 4*60*60*1000) {
					currentScn = null;
				}
			}

		} finally {
			if (rs != null) rs.close();
			if (stmt != null) stmt.close();
		}

		return currentScn;
	}
}