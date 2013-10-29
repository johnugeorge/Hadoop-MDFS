package edu.tamu.lenss.mdfs.models;

import java.util.List;

import adhoc.aodv.pdu.AODVDataContainer;
import adhoc.etc.IOUtilities;

public class FileREQ extends AODVDataContainer {

	private static final long serialVersionUID = 1L;
	private String fileName;
	private long fileCreatedTime;
	private List<Integer> keyFragIndex, fileFragIndex;
	private boolean anyAvailable;
	
	/**
	 * This class will be broadcasted to the MDFSNetwork and query for key and file fragments.
	 * @param name
	 * @param time
	 */
	public FileREQ(String name,long time){
		super(MDFSPacketType.FILE_REQ, IOUtilities.parseNodeNumber(IOUtilities.getLocalIpAddress()), 255);
		this.setBroadcast(true);
		this.setMaxHop(5);
		this.fileName = name;
		this.fileCreatedTime = time;
	}
	
	public List<Integer> getKeyFragIndex() {
		return keyFragIndex;
	}

	public void setKeyFragIndex(List<Integer> keyFragIndex) {
		this.keyFragIndex = keyFragIndex;
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

	
	public boolean isAnyAvailable() {
		return anyAvailable;
	}

	/**
	 * Set this bit if the requester wants Any available fragments of this file.
	 * @param anyAvailable
	 */
	public void setAnyAvailable(boolean anyAvailable) {
		this.anyAvailable = anyAvailable;
	}

	@Override
	public byte[] toByteArray() {
		return super.toByteArray(this);
	}

}
