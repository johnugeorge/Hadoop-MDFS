package adhoc.aodv.pdu;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import adhoc.aodv.Constants;


public class RREQ extends AodvPDU {
	private static final long serialVersionUID = 1L;
	//private static final String TAG = RREQ.class.getSimpleName();
	private int srcSeqNum;
    private int hopCount = 0;
    private int broadcastID;
    private HashMap<Integer, Integer> destSet = new HashMap<Integer, Integer>(); // <SourceAddress, SrcSeqNum>
    
    
    public RREQ(){
    	this.pduType = Constants.RREQ_PDU;
    }
    
    /**
     * Constructor for creating a route request PDU
     * @param sourceNodeAddress the originators node address
     * @param destinationNodeAddress the address of the desired node
     * @param sourceSequenceNumber originators sequence number
     * @param destinationSequenceNumber should be set to the last known sequence number of the destination
     * @param broadcastId along with the source address this number uniquely identifies this route request PDU
     */
    public RREQ(int sourceNodeAddress, int destinationNodeAddress, int sourceSequenceNumber, int destinationSequenceNumber, int broadcastId) {
		super(sourceNodeAddress, destinationNodeAddress, destinationSequenceNumber);
    	this.pduType = Constants.RREQ_PDU;
        this.srcSeqNum = sourceSequenceNumber;
        this.destSet.put(destinationNodeAddress, destinationSequenceNumber);
        this.broadcastID = broadcastId;
    }

	public int getBroadcastId(){
		return broadcastID;
	}

	public int getSourceSequenceNumber(){
		return srcSeqNum;
	}
	
	public void setDestSeqNum(int destinationSequenceNumber){
		destSeqNum = destinationSequenceNumber;
	}

	public int getHopCount(){
		return hopCount;
	}
	
	public void incrementHopCount(){
		hopCount++;
	}
	
	public void addDestinations(int dest, int destSeqNum){
		destSet.put(dest, destSeqNum);
	}
	
	public void removeDestinations(int dest){
		destSet.remove(dest);
	}
	
	public HashMap<Integer, Integer> getAllDestinations(){
		return destSet;
	}
	
	public void setAllDestinations(HashMap<Integer, Integer> dests){
		destSet = dests;
	}

	@Override
	public byte[] toBytes() {
		return this.toByteArray(this, RREQ.class);
		//return this.toString().getBytes();
	}
	
	@Override
	public String toString(){
		return super.toString()+srcSeqNum+";"+hopCount+";"+broadcastID;
	}
	
	/**
	 * The input rawPdu includes the header from the parent class AODVPdu
	 */
	@Override
	public void parseBytes(byte[] rawPdu) {
		// Need to parse out the parent data before continue
		// Parent pduType+";"+srcAddress+";"+destAddress+";"+destSeqNum+";";
		// Basically Just need "pduType" at front so that Receiver can recognize the packet type
		
		RREQ rreq = parseFromByteArray(rawPdu, RREQ.class);
		this.destAddress = rreq.getDestinationAddress();
		this.destSeqNum = rreq.getDestinationSequenceNumber();
		this.srcAddress = rreq.getSourceAddress();
		this.srcSeqNum = rreq.getSourceSequenceNumber();
        this.destSet = rreq.getAllDestinations();
        this.broadcastID = rreq.getBroadcastId();
        this.hopCount = rreq.getHopCount();
	}
}
