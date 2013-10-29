package adhoc.aodv.pdu;

import java.util.ArrayList;

import adhoc.aodv.Constants;

public class RERR extends AodvPDU {
	private static final long serialVersionUID = 1L;
	private int unreachableNodeAddress;
	private int unreachableNodeSequenceNumber;
	private ArrayList<Integer> destAddresses = new ArrayList<Integer>();

	
	
	public RERR(){
		this.pduType = Constants.RERR_PDU;
	}
	
	/**
	 * 
	 * @param unreachableNodeAddress
	 * @param unreachableNodeSequenceNumber
	 * @param destinationAddresses
	 */
    public RERR(int unreachableNodeAddress ,int unreachableNodeSequenceNumber, ArrayList<Integer> destinationAddresses) {
    	this.unreachableNodeAddress = unreachableNodeAddress;
    	this.unreachableNodeSequenceNumber = unreachableNodeSequenceNumber;
    	pduType = Constants.RERR_PDU;
        destAddresses = destinationAddresses;
        destAddress = -1;
    }

	/**
	 * Constructor of a route error message  
	 * @param
	 * @param
	 * @param destinationAddress the node which hopefully will receive this PDU packet
	 */
    public RERR(int unreachableNodeAddress ,int unreachableNodeSequenceNumber, int destinationAddress){
    	this.unreachableNodeAddress = unreachableNodeAddress;
    	this.unreachableNodeSequenceNumber = unreachableNodeSequenceNumber;
    	pduType = Constants.RERR_PDU;
        destAddress = destinationAddress;
    }
    
	public int getUnreachableNodeAddress(){
		return unreachableNodeAddress;
	}
	
	public int getUnreachableNodeSequenceNumber(){
		return unreachableNodeSequenceNumber;
	}
	
	public ArrayList<Integer> getAllDestAddresses(){
		return destAddresses;
	}
	
	@Override
	public byte[] toBytes() {
		return toByteArray(this, RERR.class);
	}
	
	@Override
	public String toString() {
		return Byte.toString(pduType)+";"+unreachableNodeAddress+";"+unreachableNodeSequenceNumber;
	}
	
	@Override
	public void parseBytes(byte[] rawPdu){
		
		RERR rerr = parseFromByteArray(rawPdu, RERR.class);
		this.unreachableNodeAddress = rerr.getUnreachableNodeAddress();
		this.unreachableNodeSequenceNumber = rerr.getDestinationSequenceNumber();
	}
}
