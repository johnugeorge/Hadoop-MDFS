package edu.tamu.lenss.mdfs.models;

import java.io.Serializable;

/**
 * A Header used in TCPConnection. Currently is used for sending/retrieving file fragment only
 * @author Jay
 */
public class FileFragPacket implements Serializable {
	public static final int REQ_TO_SEND = 0;
	public static final int REQ_TO_RECEIVE = 1;
	
	private static final long serialVersionUID = 1L;
	private String fileName;
	private long createdTime;
	private int fragIndex, reqType;
	private boolean needReply=false, ready=false;
	private boolean fragmented=true;
	
	public FileFragPacket(String fileName, long creationTime, int index, int type){
		this.fileName = fileName;
		this.createdTime = creationTime;
		this.fragIndex = index;
		this.reqType = type;
	}

	public boolean isNeedReply() {
		return needReply;
	}

	public void setNeedReply(boolean needReply) {
		this.needReply = needReply;
	}

	public boolean isReady() {
		return ready;
	}

	public void setReady(boolean readyToReceive) {
		this.ready = readyToReceive;
	}

	
	public String getFileName() {
		return fileName;
	}

	public long getCreatedTime() {
		return createdTime;
	}

	public boolean isFragmented() {
		return fragmented;
	}

	public void setFragmented(boolean fragmented) {
		this.fragmented = fragmented;
	}

	public int getFragIndex() {
		return fragIndex;
	}

	public int getReqType() {
		return reqType;
	}
}
