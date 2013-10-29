package adhoc.aodv.pdu;

import java.io.Serializable;

import adhoc.aodv.Constants;

public class UserDataPacket extends Packet implements Serializable{
	private static final long serialVersionUID = 1L;
	//private static final String TAG = UserDataPacket.class.getSimpleName();
	private AODVDataContainer aodvData;	// AODVDataContainer
	private int destAddress;
	private int hopCount=0;
	private byte pduType;
	private int sourceAddress, sourceSeqNum;
	private long packetID;
	
	public UserDataPacket(){
		this.pduType = Constants.USER_DATA_PACKET_PDU;
	}
	
	public UserDataPacket(long packetIdentifier,int sourceAddress, int destinationAddress, AODVDataContainer data){
		this.pduType = Constants.USER_DATA_PACKET_PDU;
		this.packetID = packetIdentifier;
		this.destAddress = destinationAddress;
		this.aodvData = data;
		this.sourceAddress = sourceAddress;
	}
	
	/**
	 * This is a hack used to distinguish new AODVDataContainer and old userData ....bad...
	 * @param type
	 */
	public void setPDUType(byte type){
		this.pduType = type;
	}
	
	public byte getPDUType(){
		return this.pduType;
	}
	
	
	public AODVDataContainer getData(){
		return aodvData;
	}
	
	public int getUserDataType(){
		return aodvData.getPacketType();
	}
	
	public int getSourceNodeAddress(){
		return sourceAddress;
	}
	
	public long getPacketID() {
		return packetID;
	}
	
	public void incrementHopCount(){
		hopCount++;
	}
	
	public int getHopCount(){
		return hopCount;
	}
	
	public void setSourceSequenceNum(int srcNum){
		sourceSeqNum = srcNum;
	}
	
	public int getSourceSeqNum(){
		return sourceSeqNum;
	}
	
	@Override
    public int getDestinationAddress() {
        return destAddress;
    }
	
	@Override
	public byte[] toBytes() {
		return toByteArray(this, UserDataPacket.class);
	}

	@Override
	public String toString(){
		return pduType+";"+sourceAddress+";"+destAddress+";";
	}
	
	
	@Override
	public void parseBytes(byte[] packetData) {
		
		UserDataPacket packet = parseFromByteArray(packetData, UserDataPacket.class);
		if(packet != null){
			this.aodvData = packet.getData();
			this.sourceAddress = packet.getSourceNodeAddress();
			this.destAddress = packet.getDestinationAddress();
			this.packetID = packet.getPacketID();
			this.pduType = Constants.USER_DATA_PACKET_PDU;
		}
	}

}