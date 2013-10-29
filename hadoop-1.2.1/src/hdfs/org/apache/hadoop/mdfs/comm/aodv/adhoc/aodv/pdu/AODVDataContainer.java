package adhoc.aodv.pdu;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.io.Serializable;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * This is an abstract class. The application layer extends this class to create a customized packet sent to 
 * AODV routing protocol
 * @author Jay
 */
public abstract class AODVDataContainer implements Serializable{
	private static final long serialVersionUID = 1L;
	private int appID;	// Optional. An unique ID for the Application using the AODV service
	private int packetType, dest, source, maxHop;
	private long timestamp;
	private boolean broadcast=false;
	
	public AODVDataContainer(){
	}
	
	public AODVDataContainer(int pType){
		this.packetType = pType;
	}
	
	public AODVDataContainer(int pType, int src, int desti){
		this.packetType = pType;
		this.source = src;
		this.dest = desti;
		this.setTimestamp(System.currentTimeMillis());
	}
	
	
	public int getAppID() {
		return appID;
	}

	public void setAppID(int appID) {
		this.appID = appID;
	}

	public int getPacketType() {
		return packetType;
	}

	public void setPacketType(int packetType) {
		this.packetType = packetType;
	}

	public int getDest() {
		return dest;
	}

	public void setDest(int dest) {
		this.dest = dest;
	}

	public int getSource() {
		return source;
	}

	public void setSource(int source) {
		this.source = source;
	}

	public int getMaxHop() {
		return maxHop;
	}

	public void setMaxHop(int maxHop) {
		this.maxHop = maxHop;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public boolean isBroadcast() {
		return broadcast;
	}

	public void setBroadcast(boolean broadcast) {
		this.broadcast = broadcast;
	}

	public <T extends AODVDataContainer> byte[] toByteArray(T type){
		ByteArrayOutputStream byteStr = new ByteArrayOutputStream(8192);
		try {
			GZIPOutputStream gout = new GZIPOutputStream(byteStr);
			ObjectOutputStream output = new ObjectOutputStream(gout);
			//ObjectOutputStream output = new ObjectOutputStream(byteStr);
			output.writeObject(type);
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
	 * This method has to be overrode by children
	 * @return
	 */
	public abstract byte[] toByteArray();
	
	
	/**
	 * Generic Method
	 * @param packetData
	 * @param type
	 * @return
	 */
	public static <T extends AODVDataContainer> T parseFromByteArray(byte[] packetData, Class<T> type){
		T packet=null;
		try {
			ByteArrayInputStream byteStr = new ByteArrayInputStream(packetData);
			GZIPInputStream gin = new GZIPInputStream(byteStr);
			ObjectInputStream input = new ObjectInputStream(gin);
			//ObjectInputStream input = new ObjectInputStream(byteStr);
			packet = type.cast(input.readObject());
			input.close();
			gin.close();
			byteStr.close();
		} catch (OptionalDataException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch(ClassCastException e){
			e.printStackTrace();
		}
		return packet;
	}
}
