package edu.tamu.lenss.mdfs.models;

import adhoc.aodv.pdu.AODVDataContainer;
import adhoc.etc.IOUtilities;

public class NewFileUpdate extends AODVDataContainer {

	private static final long serialVersionUID = 1L;
	private MDFSFileInfo fileInfo;

	public NewFileUpdate(MDFSFileInfo file){
		super(MDFSPacketType.NEW_FILE_UPDATE, IOUtilities.parseNodeNumber(IOUtilities.getLocalIpAddress()), 255);
		this.setBroadcast(true);
		this.setMaxHop(5);
		this.fileInfo = file;
	}
	
	public MDFSFileInfo getFileInfo() {
		return fileInfo;
	}

	@Override
	public byte[] toByteArray() {
		return super.toByteArray(this);
	}
}
