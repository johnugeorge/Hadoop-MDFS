package adhoc.aodv.pdu;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import adhoc.aodv.Constants;

public class HelloPacket extends Packet implements Serializable{
	private static final long serialVersionUID = 1L;
	//private static final String TAG = HelloPacket.class.getSimpleName();
	private int sourceAddress;
	private int sourceSeqNr;
	public Set<Double> dummySet = new HashSet<Double>();
	public HelloPacket(){
		this.pduType = Constants.HELLO_PDU;
		for(int i=0; i < 50; i++){		// was 80
			Random rand = new Random();
			dummySet.add(rand.nextDouble());
		}
	}
	
	public HelloPacket(int sourceAddress, int sourceSeqNr){
		this();
		this.pduType = Constants.HELLO_PDU;
		this.sourceAddress = sourceAddress;
		this.sourceSeqNr = sourceSeqNr;
	}
	
	public int getSourceAddress(){
		return sourceAddress;
	}
	
	@Override
	public int getDestinationAddress() {
		return Constants.BROADCAST_ADDRESS;
	}
	
	public int getSourceSeqNr(){
		return sourceSeqNr;
	}

	@Override
	public byte[] toBytes() {
		return toByteArray(this, HelloPacket.class);
	}
	
	@Override
	public String toString(){
		return pduType+";"+sourceAddress+";"+sourceSeqNr;
	}
	
	@Override
	public void parseBytes(byte[] rawPdu) {
		HelloPacket hello = parseFromByteArray(rawPdu, HelloPacket.class);
		this.sourceAddress = hello.getSourceAddress();
		this.sourceSeqNr = hello.getSourceSeqNr();
	}
}
