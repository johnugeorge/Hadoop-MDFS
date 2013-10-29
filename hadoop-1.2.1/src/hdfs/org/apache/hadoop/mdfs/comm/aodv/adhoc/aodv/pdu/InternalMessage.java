package adhoc.aodv.pdu;

import adhoc.etc.Debug;

public class InternalMessage extends AodvPDU{

	
	private static final long serialVersionUID = 1L;

	public InternalMessage(byte pduType, int destinationAddress){
		this.pduType = pduType;
		this.destAddress = destinationAddress;
	}
	
	
	@Override
	public void parseBytes(byte[] rawPdu) {
		Debug.print("DO NOT USE");
		
	}

	@Override
	public byte[] toBytes() {
		Debug.print("DO NOT USE");
		return null;
	}

}
