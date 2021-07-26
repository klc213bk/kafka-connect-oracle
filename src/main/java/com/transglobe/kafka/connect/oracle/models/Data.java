package com.transglobe.kafka.connect.oracle.models;

import java.sql.Timestamp;

/**
 *  
 * @author Erdem Cer (erdemcer@gmail.com)
 */

public class Data{

	private String rsId;
	private long ssn;
    private long scn;
    private long commitScn;
    private String rowId;
    private String segOwner;
    private String segName;
    private String sqlRedo;
    private Timestamp timeStamp;
    private String operation;

    public Data(String rsId, long ssn, long scn, long commitScn, String rowId, String segOwner,String segName,String sqlRedo,Timestamp timeStamp,String operation){
        super();
        this.rsId = rsId;
        this.ssn = ssn;
        this.scn=scn;
        this.commitScn=commitScn;
        this.rowId=rowId;
        this.segOwner=segOwner;
        this.segName=segName;
        this.sqlRedo=sqlRedo;
        this.timeStamp=timeStamp;
        this.operation=operation;
    }


    public String getRsId(){
        return rsId;
    }

    public long getSsn(){
        return ssn;
    }
    public long getScn(){
        return scn;
    }
    public long getCommitScn(){
        return commitScn;
    }
    public String getRowId(){
        return rowId;
    }
    public String getSegOwner(){
        return segOwner;
    }

    public String getSegName(){
        return segName;
    }

    public String getSqlRedo(){
        return sqlRedo;
    }

    public Timestamp getTimeStamp(){
        return timeStamp;
    }

    public String getOperation(){
        return operation;
    }

    public void setRsId(String rsId){
        this.rsId = rsId;
    }

    public void setSsn(long ssn){
        this.ssn=ssn;
    }

    public void setScn(long scn){
        this.scn=scn;
    }
    public void setCommitScn(long commitScn){
        this.commitScn=commitScn;
    }
    public void setRowId(String rowId){
        this.rowId=rowId;
    }
    public void setSegOwner(String segOwner){
        this.segOwner=segOwner;
    }

    public void setSegName(String segName){
        this.segName=segName;
    }

    public void setSqlRedo(String sqlRedo){
        this.sqlRedo=sqlRedo;
    }

    public void setTimeStamp(Timestamp timeStamp){
        this.timeStamp=timeStamp;
    }

    public void setOperation(String operation){
        this.operation=operation;
    }
}