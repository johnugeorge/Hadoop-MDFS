package edu.tamu.lenss.mdfs.models;

import java.util.List;

import adhoc.aodv.pdu.AODVDataContainer;
import edu.tamu.lenss.mdfs.crypto.FragmentInfo.KeyShareInfo;

public class FileREP extends AODVDataContainer {
	private static final long serialVersionUID = 1L;
	private String fileName;
	private long fileCreatedTime;
	private List<Integer> fileFragIndex;
	private KeyShareInfo keyShare;
	
	public FileREP(String name,long time, int source, int destination){
		super(MDFSPacketType.FILE_REP, source, destination);
		this.setBroadcast(false);
		this.fileName = name;
		this.fileCreatedTime = time;
	}
	
	public List<Integer> getFileFragIndex() {
		return fileFragIndex;
	}

	public void setFileFragIndex(List<Integer> fileFragIndex) {
		this.fileFragIndex = fileFragIndex;
	}

	public String getFileName() {
		return fileName;
	}

	public long getFileCreatedTime() {
		return fileCreatedTime;
	}
	
	public KeyShareInfo getKeyShare() {
		return keyShare;
	}

	public void setKeyShare(KeyShareInfo keyShare) {
		this.keyShare = keyShare;
	}

	@Override
	public byte[] toByteArray() {
		return super.toByteArray(this);
	}

}
