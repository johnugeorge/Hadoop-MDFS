package edu.tamu.lenss.mdfs.models;

import adhoc.aodv.pdu.AODVDataContainer;
import adhoc.etc.IOUtilities;

public class TopologyDiscovery extends AODVDataContainer {
	private static final long serialVersionUID = 1L;
	private String message;
	
	public TopologyDiscovery(){
		this("");
	}
	
	/**
	 * Can hold at most 20 characters. 
	 * @param msg
	 */
	public TopologyDiscovery(String msg){
		super(MDFSPacketType.TOPOLOGY_DISCOVERY, IOUtilities.parseNodeNumber(IOUtilities.getLocalIpAddress()), 255);
		this.setBroadcast(true);
		this.setMaxHop(5);
		if(msg.length() >= 20)
			msg = msg.subSequence(0, 20).toString();	// At most 20 characters
		this.message = msg;		
	}
	
	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	@Override
	public byte[] toByteArray() {
		return toByteArray(this);
	}

}
