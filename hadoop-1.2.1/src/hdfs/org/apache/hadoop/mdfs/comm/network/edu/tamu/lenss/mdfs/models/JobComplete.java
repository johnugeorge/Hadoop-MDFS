package edu.tamu.lenss.mdfs.models;

import adhoc.aodv.pdu.AODVDataContainer;
import adhoc.etc.IOUtilities;

public class JobComplete extends AODVDataContainer {
	private static final long serialVersionUID = 1L;
	
	public JobComplete(){
		super(MDFSPacketType.JOB_COMPLETE, IOUtilities.parseNodeNumber(IOUtilities.getLocalIpAddress()), 255);
		this.setBroadcast(true);
		this.setMaxHop(5);
	}
	
	@Override
	public byte[] toByteArray() {
		return super.toByteArray(this);
	}
}
