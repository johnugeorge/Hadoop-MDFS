package edu.tamu.lenss.mdfs.models;

import adhoc.aodv.pdu.AODVDataContainer;
import adhoc.etc.IOUtilities;

public class TaskResult extends AODVDataContainer {
	private static final long serialVersionUID = 1L;
	private String taskName;
	private long fileId;
	private int result;
	
	public TaskResult(){
		super(MDFSPacketType.JOB_RESULT, IOUtilities.parseNodeNumber(IOUtilities.getLocalIpAddress()), 255);
		this.setBroadcast(true);
		this.setMaxHop(5);
	}
	
	public TaskResult(String tskName, long id, int res){
		this();
		this.taskName = tskName;
		this.fileId = id;
		this.result = res;
	}
	
	public void setResult(String name, int res){
		this.taskName = name;
		this.result = res;
	}
	
	public String getTaskName(){
		return taskName;
	}
	
	public int getResult(){
		return result;
	}
	
	public long getFileId() {
		return fileId;
	}

	public void setFileId(long fileId) {
		this.fileId = fileId;
	}

	@Override
	public byte[] toByteArray() {
		return super.toByteArray(this);
	}

}
