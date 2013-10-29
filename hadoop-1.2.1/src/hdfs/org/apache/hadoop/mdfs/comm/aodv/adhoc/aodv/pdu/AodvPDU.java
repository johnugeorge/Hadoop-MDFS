package adhoc.aodv.pdu;

import java.io.Serializable;



public abstract class AodvPDU extends Packet implements Serializable{
	private static final long serialVersionUID = 1L;
    protected int srcAddress, destAddress;
    protected int destSeqNum;
    
    public AodvPDU(){
    	super();
    }
    
    public AodvPDU(int sourceAddress, int destinationAddess, int destinationSequenceNumber){
    	srcAddress = sourceAddress;
    	destAddress = destinationAddess;
    	destSeqNum = destinationSequenceNumber;
    }
    
    public int getSourceAddress() {
        return srcAddress;
    }
    
    public void setSourceAddress(int src){
    	this.srcAddress = src;
    }

    @Override
    public int getDestinationAddress() {
        return destAddress;
    }
    
    public void setDestinationAddress(int dest){
    	this.destAddress = dest;
    }
    
    @Override
    public String toString(){
    	return Byte.toString(pduType)+";"+srcAddress+";"+destAddress+";"+destSeqNum+";";
    }

    public int getDestinationSequenceNumber() {
        return destSeqNum;
    }
    
   
}
