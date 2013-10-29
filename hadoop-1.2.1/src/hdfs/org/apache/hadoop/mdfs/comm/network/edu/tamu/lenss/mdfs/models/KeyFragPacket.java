package edu.tamu.lenss.mdfs.models;

import adhoc.aodv.pdu.AODVDataContainer;
import edu.tamu.lenss.mdfs.crypto.FragmentInfo.KeyShareInfo;

public class KeyFragPacket extends AODVDataContainer {
	private static final long serialVersionUID = 1L;
	private long createdTime;
	private KeyShareInfo keyShare;

	public KeyFragPacket(int source, int destination, KeyShareInfo key, long fileCreationTime){
		super(MDFSPacketType.KEY_FRAG_PACKET, source, destination);
		this.setBroadcast(false);
		this.createdTime = fileCreationTime;
		this.keyShare = key;
	}
	
	public KeyShareInfo getKeyShareInfo() {
		return keyShare;
	}

	public String getFileName() {
		return keyShare.getFileName();
	}

	public long getCreatedTime() {
		return createdTime;
	}

	public int getFragmentIndex() {
		return keyShare.getIndex();
	}

	@Override
	public byte[] toByteArray() {
		return super.toByteArray(this);
	}

}
