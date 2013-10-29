package adhoc.aodv.pdu;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import adhoc.aodv.Constants;

public class BroadcastPacket extends Packet implements Serializable {
	private static final long serialVersionUID = 1L;
	private long packetID;
	private int maxHop, hopCount, sourceAddress, sourceSeqNum;
	private ArrayList<Integer> interNodes = new ArrayList<Integer>();
	private AODVDataContainer data;

	public BroadcastPacket(){
		this.packetID = generatePacketID();
		this.pduType = Constants.BROADCAST_PDU;
		
	}
	
	/**
	 * @param src		The ID of the source node
	 * @param srcSeqNum The current sequence number of the source node
	 * @param maxHop	The maximum distance that the packet can be broadcasted
	 * @param inData	The data in this broadcast message
	 */
	public BroadcastPacket(int src, int srcSeqNum, int maxHop, AODVDataContainer inData){
		this.sourceAddress = src;
		this.sourceSeqNum = srcSeqNum;
		this.maxHop = maxHop;
		this.data = inData;
		this.hopCount=1;
		this.packetID = generatePacketID();
		this.pduType = Constants.BROADCAST_PDU;
	}
	
	private long generatePacketID(){
		Random rand = new Random();
		return rand.nextLong();
	}
	
	public long getPacketID() {
		return packetID;
	}
	
	public int getHopCount(){
		return hopCount;
	}
	
	public void incrementHopCount(){
		hopCount++;
	}

	public void setPacketID(long packetID) {
		this.packetID = packetID;
	}

	public int getMaxHop() {
		return maxHop;
	}

	public void setMaxHop(int maxHop) {
		this.maxHop = maxHop;
	}

	public int getSourceAddress() {
		return sourceAddress;
	}

	public void setSourceAddress(int sourceAddress) {
		this.sourceAddress = sourceAddress;
	}

	public AODVDataContainer getData() {
		return data;
	}

	public void setData(AODVDataContainer data) {
		this.data = data;
	}
	
	public int getSourceSeqNum() {
		return sourceSeqNum;
	}

	public void setSourceSeqNum(int sourceSeqNum) {
		this.sourceSeqNum = sourceSeqNum;
	}
	
	public ArrayList<Integer> getInterNodes() {
		return interNodes;
	}

	public void addInterNodes(int interNodeIP) {
		this.interNodes.add(interNodeIP);
	}
	
	/**
	 * @return null if the previous node does not set this field properly
	 */
	public int getLastNode(){
		if(interNodes.isEmpty()){
			return sourceAddress;
		}
		else
			return interNodes.get(interNodes.size()-1);
	}

	@Override
	public byte[] toBytes() {
		return toByteArray(this);
	}

	@Override
	public void parseBytes(byte[] rawPdu){
	}

	@Override
	public int getDestinationAddress() {
		return Constants.BROADCAST_ADDRESS;
	}
	
	/**
	 * 
	 * @param packet
	 * @return null if fails to serialize
	 */
	public static byte[] toByteArray(BroadcastPacket packet){
		ByteArrayOutputStream byteStr = new ByteArrayOutputStream(Constants.UDP_MAX_PACKAGE_SIZE);
		
		try {
			//ObjectOutputStream output = new ObjectOutputStream(byteStr);
			GZIPOutputStream gout = new GZIPOutputStream(byteStr);
			ObjectOutputStream output = new ObjectOutputStream(gout);
			output.writeObject(packet);
			output.flush();
			gout.finish();
			byteStr.flush();
			byte[] byteData = byteStr.toByteArray();
			output.close();
			gout.finish();
			byteStr.close();
			return byteData;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * 
	 * @param packetData
	 * @return null if fails to parse
	 */
	public static BroadcastPacket parseFromByteArray(byte[] packetData){
		BroadcastPacket packet=null;
		try {
			ByteArrayInputStream byteStr = new ByteArrayInputStream(packetData);
			//ObjectInputStream input = new ObjectInputStream(byteStr);
			GZIPInputStream gin = new GZIPInputStream(byteStr);
			ObjectInputStream input = new ObjectInputStream(gin);
			packet = (BroadcastPacket) input.readObject();
			input.close();
			gin.close();
			byteStr.close();
		} catch (OptionalDataException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassCastException e){
			e.printStackTrace();
		}
		return packet;
	}

}
