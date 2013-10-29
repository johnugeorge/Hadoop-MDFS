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

import adhoc.aodv.Constants;

public abstract class Packet implements Serializable{
	protected static final long serialVersionUID = 1L;
	protected byte pduType;
	
	public Packet(){
		
	}
	
	public Packet(byte type){
		this.pduType = type;
	}
	
	public byte getPduType(){
		return this.pduType;
	}
	
	public void setPduType(byte type){
		this.pduType = type;
	}
	
	public abstract byte[] toBytes();
	
	public abstract void parseBytes(byte[] rawPdu);

	public abstract int getDestinationAddress();
	
	/**
	 * Generic Method
	 * @param packetData
	 * @param type
	 * @return
	 */
	public static <T extends Packet> T parseFromByteArray(byte[] packetData, Class<T> type){
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
		//return Node.getInstance().parseFromByteArray(packetData, type);
	}
	
	public <T extends Packet> byte[] toByteArray(T type, Class<T> typeClass){
		ByteArrayOutputStream byteStr = new ByteArrayOutputStream(Constants.UDP_MAX_PACKAGE_SIZE);
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
			gout.close();
			byteStr.close();
			return byteData;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
}
