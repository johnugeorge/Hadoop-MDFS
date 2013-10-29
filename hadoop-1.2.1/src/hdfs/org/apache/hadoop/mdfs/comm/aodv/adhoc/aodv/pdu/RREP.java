package adhoc.aodv.pdu;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import adhoc.aodv.Constants;

public class RREP extends AodvPDU {
	private static final long serialVersionUID = 1L;
	private int hopCount = 0;
    private int srcSeqNum;
    
    private HashMap<Integer, RREPData> destSet = new HashMap<Integer, RREPData>(); // <SourceAddress, SrcSeqNum>
    
    public RREP(){
    	this.pduType = Constants.RREP_PDU;
    }
    
    /**
     * 
     * @param sourceAddress 		The node that request for the route
     * @param destinationAddress	The destination node that the source is looking for 
     * @param sourceSequenceNumber	The sequence number of the source
     * @param destinationSequenceNumber	The sequence number of the destination
     * @param hopCount				The hop-count to the destination
     */
    public RREP(	int sourceAddress,
    				int destinationAddress,
    				int sourceSequenceNumber,
    				int destinationSequenceNumber,
    				int hopCount){
    	
    	super(sourceAddress,destinationAddress,destinationSequenceNumber);
    	pduType = Constants.RREP_PDU;
    	srcSeqNum = sourceSequenceNumber;
    	this.hopCount = hopCount;
    	destSet.put(sourceAddress, new RREPData(destinationAddress, destinationSequenceNumber, hopCount));
    }
    
    public RREP(	int sourceAddress,
    				int destinationAddress,
    				int sourceSequenceNumber,
    				int destinationSequenceNumber) {
    	
    	super(sourceAddress,destinationAddress,destinationSequenceNumber);
    	pduType = Constants.RREP_PDU;
    	srcSeqNum = sourceSequenceNumber;
    	destSet.put(sourceAddress, new RREPData(destinationAddress, destinationSequenceNumber, hopCount));
    }
	
	public int getHopCount(){
		return hopCount;
	}
	
	public void setHopCount(int hop){
		hopCount = hop;
	}
	
	public void incrementHopCount(){
		 Iterator<Entry<Integer, RREPData>> iter =  destSet.entrySet().iterator();
		 while(iter.hasNext()){
			 iter.next().getValue().incrementHopCount();
		 }
		hopCount++;
	}
	
	public int getDestinationSequenceNumber(){
		return destSeqNum;
	}
	
	public void setDestSequenceNumber(int destSeq){
		destSeqNum = destSeq;
	}
	
	public int getSourceSequenceNumber(){
		return srcSeqNum;
	}
	
	public void setSourceSequenceNumber(int srcSeq){
		srcSeqNum = srcSeq;
	}
	
	public void addDestination(int dest, int destSeq, int destHopCount){
		destSet.put(dest, new RREPData(dest, destSeq, destHopCount));
	}
	
	public void removeDestinatioin(int dest){
		destSet.remove(dest);
	}
	
	public HashMap<Integer, RREPData> getAllDestinations(){
		return destSet;
	}

	@Override
	public byte[] toBytes() {
		return toByteArray(this, RREP.class);
	}
	
	@Override
	public String toString() {
		return super.toString()+srcSeqNum+";"+hopCount;
	}
	
	@Override
	public void parseBytes(byte[] rawPdu) {
		RREP rrep = parseFromByteArray(rawPdu, RREP.class);
		this.srcAddress = rrep.getSourceAddress();
		this.destAddress = rrep.getDestinationAddress();
		this.srcSeqNum = rrep.getSourceSequenceNumber();
		this.destSeqNum = rrep.getDestinationSequenceNumber();
		this.hopCount = rrep.getHopCount();
		this.destSet = rrep.getAllDestinations();
		
	}
	
	
	public static final class RREPData implements Serializable{
		private static final long serialVersionUID = 1L;
		public int destinationAdd, destinationSeqNum, destHopCount;
		
		public RREPData(int add, int seq, int hop){
			this.destHopCount = hop;
			this.destinationAdd = add;
			this.destinationSeqNum = seq;
		}
		
		public void incrementHopCount(){
			destHopCount++;
		}
	}
}
