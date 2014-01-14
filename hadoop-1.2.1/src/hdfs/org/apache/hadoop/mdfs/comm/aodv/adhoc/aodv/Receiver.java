package adhoc.aodv;

import java.net.BindException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import adhoc.aodv.exception.AodvException;
import adhoc.aodv.exception.NoSuchRouteException;
import adhoc.aodv.exception.RouteNotValidException;
import adhoc.aodv.pdu.BroadcastPacket;
import adhoc.aodv.pdu.HelloPacket;
import adhoc.aodv.pdu.Packet;
import adhoc.aodv.pdu.RERR;
import adhoc.aodv.pdu.RREP;
import adhoc.aodv.pdu.RREP.RREPData;
import adhoc.aodv.pdu.RREQ;
import adhoc.aodv.pdu.UserDataPacket;
import adhoc.aodv.routes.ForwardRouteEntry;
//import adhoc.etc.AndroidDataLogger;
//import adhoc.etc.AndroidDataLogger.LogFileInfo.LogFileName;
import adhoc.etc.Logger;
import adhoc.udp.UdpReceiver;

public class Receiver implements Runnable {
	private Sender sender;
	private Queue<Message> receivedMessages;
	private RouteTableManager routeTableManager;
	private UdpReceiver udpReceiver;
	private int nodeAddress;
	private Thread receiverThread;
	//private AndroidDataLogger dataLogger;
	private static final String TAG = Receiver.class.getSimpleName();

    /**
     */
    private Node parent;
	private volatile boolean keepRunning = true;

	public Receiver(Sender sender, int nodeAddress, Node parent, RouteTableManager routeTableManager) throws SocketException, UnknownHostException, BindException {
		this.parent = parent;
		this.nodeAddress = nodeAddress;
		this.sender = sender;
		receivedMessages = new ConcurrentLinkedQueue<Message>();
		this.routeTableManager = routeTableManager;
		udpReceiver = new UdpReceiver(this, nodeAddress);
		//dataLogger = parent.getDatalogger();
	}

	public void startThread(){
		keepRunning = true;
		udpReceiver.startThread();
		receiverThread = new Thread(this, Receiver.class.getSimpleName());
		receiverThread.start();
	}
	
	/**
	 * Stops the receiver thread.
	 */
	public void stopThread() {
		keepRunning = false;
		udpReceiver.stopThread();
		receiverThread.interrupt();
	}
	
	public void run() {
		while (keepRunning) {
			synchronized (receivedMessages) {
				while (receivedMessages.isEmpty()) {
					try {
						receivedMessages.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}

			Message msg = receivedMessages.poll(); 
			if(msg == null || msg.senderNodeAddress == nodeAddress)
				continue;
			
			StringBuilder str = new StringBuilder();
			str.append(System.currentTimeMillis() + ", ");
			str.append(msg.getType() + ", " + nodeAddress + ", " + msg.getSender());
			str.append("\n");
			//dataLogger.appendSensorData(LogFileName.PACKET_RECEIVED, str.toString());
			//System.out.println(" MEssage Recieved in Receiver "+str);
			switch (msg.getType()) {
			case Constants.HELLO_PDU:
				HelloPacket hello = HelloPacket.parseFromByteArray(msg.data, HelloPacket.class);
				helloMessageReceived(hello);
				break;
			case Constants.BROADCAST_PDU:
				BroadcastPacket brdPkt = BroadcastPacket.parseFromByteArray(msg.data);
				broadcastPacketReceived(brdPkt);
				break;
			/*case Constants.RREQ_PDU:
				RREQ rreq = RREQ.parseFromByteArray(msg.data, RREQ.class);
				routeRequestReceived(rreq, msg.senderNodeAddress);
				break;
			case Constants.RREP_PDU:
				RREP rrep = RREP.parseFromByteArray(msg.data, RREP.class);
				routeReplyReceived(rrep, msg.senderNodeAddress);
				break;
			case Constants.RERR_PDU:
				RERR rerr = RERR.parseFromByteArray(msg.data, RERR.class);
				routeErrorRecived(rerr);
				break;*/
			case Constants.USER_DATA_PACKET_PDU:
				UserDataPacket userDataPacket = UserDataPacket.parseFromByteArray(msg.data, UserDataPacket.class);
				userDataPacketReceived(userDataPacket, msg.senderNodeAddress);
				break;
			default:
				// The received message is not in the domain of protocol messages
				Logger.w(TAG, "Unrecognized Packet Type");
				break;
			}
		}
	}

	/**
	 * Method used by the lower network layer to queue messages for later processing
	 * 
	 * @param senderNodeAddress Is the address of the node that sent a message
	 * @param msg is an array of bytes which contains the sent data
	 */
	public void addMessage(int senderNodeAddress, byte[] msg) {
		receivedMessages.add(new Message(senderNodeAddress, msg));
		synchronized (receivedMessages) {
			receivedMessages.notify();
		}
	}

	private Map<Integer, Long> helloMap = new HashMap<Integer, Long>();
	/**
	 * Handles a HelloHeader, when such a message is received from a neighbor <br>
	 * Need to receiver 2 consecutive packets in order to update the neighbor list
	 * @param hello is the HelloHeader message received
	 */
	private void helloMessageReceived(HelloPacket hello) {
		int src = hello.getSourceAddress();
		long time = System.currentTimeMillis();
		if(!helloMap.containsKey(src)){
			helloMap.put(src, time);
			return;	// First packet received
		}
		
		/*if(time - helloMap.get(src) < 1.5*Constants.BROADCAST_INTERVAL){
			try {
				routeTableManager.setValid(hello.getSourceAddress(), hello.getSourceSeqNr(), 1);
			} catch (NoSuchRouteException e) {
				routeTableManager.createForwardRouteEntry(	hello.getSourceAddress(), hello.getSourceAddress(),
															hello.getSourceSeqNr(),	1,	true);
				e.printStackTrace();
			}
		}*/
		helloMap.put(src, time);
	}
	
	
	private void broadcastPacketReceived(BroadcastPacket packet){
		//Logger.v(TAG, "Receive BroadcastID: " + packet.getPacketID());
		if(sender.isBroadcastIDReceived(packet.getPacketID()) ){
			// Do nothing
		}
		else{			
			// Update the ForwardRouteTable. Only use the packet with shortest delay. The later incoming packet from shorter hop-counts
			// may be ignored
			//updateRouteByIncomingPacket(packet.getSourceAddress(), packet.getHopCount(), packet.getSourceSeqNum(), packet.getLastNode());
			
			sender.cacheBroadcastID(packet.getPacketID());
			// if this packet is not broadcasted anymore, it won't be added to the broadcast queue in sender. To avoid processing
			// multiple duplicate broadcast packet, we add it to sender broadcast queue manually here.
			if(packet.getHopCount() < packet.getMaxHop()){
				// Increment the hop-count and rebroadcast
				packet.incrementHopCount();
				packet.addInterNodes(nodeAddress);
				sender.queuePacket(packet);
				Logger.i(TAG, "Received Broadcast packet is queued to send");
			}
			// Handle the message on this node
			parent.notifyAboutDataReceived(packet.getSourceAddress(), packet.getData());
		}
	}

	/**
	 * Handles the incoming RREP messages
	 * RREQTable is only cleaned up by the Timer in RouteTableManager, not the RREP message
	 * @param rrep is the message received
	 * @param senderNodeAddress the address of the sender (One of the neighbors)
	 */
	/*private void routeReplyReceived(RREP rrep, int senderNodeAddress) {
		//rrepRoutePrecursorAddress is a local int used to hold the next-hop address from the forward route 
		int rrepRoutePrecursorAddress = -1;
		
		//Create route to previous node with unknown seqNum (neighbor)
		if(routeTableManager.createForwardRouteEntry(senderNodeAddress,	senderNodeAddress,
													Constants.UNKNOWN_SEQUENCE_NUMBER, 1, true)){
			Logger.v(TAG, "RREP is received and route to: "+senderNodeAddress+" is created with destSeq: "+Constants.UNKNOWN_SEQUENCE_NUMBER);
		}
		rrep.incrementHopCount();

		// Source Address is the node that originally seeks for the route. The RREP will be sent back to this source
		if (rrep.getSourceAddress() != nodeAddress) {
			//forward the RREP
			sender.queuePDUmessage(rrep);
		
			// handle the first part of the route (reverse route) - from this node to the one which originated the RREQ
			try {
				//add the sender node to precursors list of the reverse route
				ForwardRouteEntry reverseRoute = routeTableManager.getForwardRouteEntry(rrep.getSourceAddress());
				reverseRoute.addPrecursorAddress(senderNodeAddress);
				rrepRoutePrecursorAddress = reverseRoute.getNextHop();
			} catch (AodvException e) {
				//no reverse route is currently known so the RREP may not reach the originator of the RREQ
				Logger.e(TAG, e.toString());
			}
		}
		
		// handle the second part of the route - from this node to the destination address in the RREP
		Iterator<Entry<Integer, RREPData>> iter = rrep.getAllDestinations().entrySet().iterator();
		while(iter.hasNext()){
			Entry<Integer, RREPData> dest = iter.next();
			RREPData destData = dest.getValue();
			try {
				ForwardRouteEntry oldRoute = routeTableManager.getForwardRouteEntry(destData.destinationAdd);
				if(rrepRoutePrecursorAddress != -1){
					oldRoute.addPrecursorAddress(rrepRoutePrecursorAddress);
				}
				//see if the RREP contains updates (better seqNum or hopCountNum) to the old route
				routeTableManager.updateForwardRouteEntry(oldRoute,
						new ForwardRouteEntry(	destData.destinationAdd, senderNodeAddress,
												destData.destHopCount,	destData.destinationSeqNum,
												oldRoute.getPrecursors()));
			} catch (NoSuchRouteException e) {
				// This node doesn't have a route to the destination. Add it to the routing table
				e.printStackTrace();
				ArrayList<Integer> precursorNode = new ArrayList<Integer>();
				if(rrepRoutePrecursorAddress != -1){
					precursorNode.add(rrepRoutePrecursorAddress);
				}
				routeTableManager.createForwardRouteEntry(	destData.destinationAdd, senderNodeAddress, 
															destData.destinationSeqNum,	destData.destHopCount,
															precursorNode, true);
			} catch (RouteNotValidException e) {
				// Route to the destination existed, but is not valid anymore
				try {
					//update the previously known route with the better route contained in the RREP
					routeTableManager.setValid(destData.destinationAdd, destData.destinationSeqNum);
					if(rrepRoutePrecursorAddress != -1){
						routeTableManager.getForwardRouteEntry(destData.destinationAdd).addPrecursorAddress(rrepRoutePrecursorAddress);
					}
				}catch (AodvException e1) {
					 Logger.e(TAG, e.toString());
				}
			}
		}
	}*/

	/**
	 * Handles a RREQ message when received
	 * 
	 * @param rreq the RREQ message that were received
	 * @param senderNodeAddress the node (a neighbor) which sent this message
	 */
/*	private void routeRequestReceived(RREQ rreq, int senderNodeAddress) {
		if (routeTableManager.routeRequestExists(rreq.getSourceAddress(), rreq.getBroadcastId())) {
			return;
		}
		
		//Create route to previous node with unknown seqNum (neighbor)
		if(routeTableManager.createForwardRouteEntry(senderNodeAddress,	senderNodeAddress,
													Constants.UNKNOWN_SEQUENCE_NUMBER, 1, true)){
			Logger.v(TAG, "RREQ is received from: "+senderNodeAddress+" and route where created with destSeq: "+Constants.UNKNOWN_SEQUENCE_NUMBER);
		}
		
		// Increments the hopCount and Adds the RREQ to the table
		rreq.incrementHopCount();
		routeTableManager.createRouteRequestEntry(rreq, true);

		updateRouteByIncomingPacket(rreq.getSourceAddress(), rreq.getHopCount(), rreq.getSourceSequenceNumber(), senderNodeAddress);
		
		searchRREQRoutes(rreq);
	}*/
	
	/**
	 * Search for the routes in the RREQ and return a RREP with all the known routes
	 * @param rreq
	 * @return
	 */
	/*private RREP searchRREQRoutes(RREQ rreq){
		RREP rrep = new RREP();
		ArrayList<Integer> toBeRemoved = new ArrayList<Integer>(); 
		Iterator<Entry<Integer, Integer>> iter = rreq.getAllDestinations().entrySet().iterator();
		while(iter.hasNext()){
			Entry<Integer, Integer> dest = iter.next();
			//check if this node is the destination
			if (dest.getKey() == nodeAddress) {
				if(parent.getNextSequenceNumber(parent.getCurrentSequenceNumber()) == dest.getValue()){
					parent.getNextSequenceNumber();
				}
				rrep.addDestination(nodeAddress, parent.getCurrentSequenceNumber(), 0);
				
				// Not necessary
				rrep.setSourceAddress(rreq.getSourceAddress());
				rrep.setDestinationAddress(nodeAddress);
				rrep.setSourceSequenceNumber(rreq.getSourceSequenceNumber());
				rrep.setDestSequenceNumber(parent.getCurrentSequenceNumber());
				
				toBeRemoved.add(nodeAddress);
			}
			else{
				//this node is not the destination of the RREQ so we need to check if we have the requested route
				ForwardRouteEntry entry;
				try {
					entry = routeTableManager.getForwardRouteEntry(dest.getKey());
					// If a valid route exists with a seqNum that is greater or equal to the RREQ, then send a RREP
					if (isIncomingSeqNrBetter(entry.getDestinationSequenceNumber(), dest.getValue())) {
						rrep.addDestination(entry.getDestinationAddress(), entry.getDestinationSequenceNumber(), entry.getHopCount());
						
						// Not necessary
						rrep.setSourceAddress(rreq.getSourceAddress());
						rrep.setDestinationAddress(entry.getDestinationAddress());
						rrep.setSourceSequenceNumber(rreq.getSourceSequenceNumber());
						rrep.setDestSequenceNumber(entry.getDestinationSequenceNumber());
						rrep.setHopCount(entry.getHopCount());
						
						toBeRemoved.add(dest.getKey());
						
						// Gratuitous RREP for the destination Node
						RREP gRrep = new RREP(	entry.getDestinationAddress(),
												rreq.getSourceAddress(),
												entry.getDestinationSequenceNumber(),
												rreq.getSourceSequenceNumber(),
												rreq.getHopCount()	);
						sender.queuePDUmessage(gRrep);
					}
				} catch (NoSuchRouteException e) {
					//this node is an intermediate node, but do not know a route to the desired destination
					e.printStackTrace();
				} catch (RouteNotValidException e) {
					//this node know a route but it is not active any longer.
					e.printStackTrace();
					try {
						int maxSeqNum = getMaximumSeqNum(routeTableManager.getLastKnownDestSeqNum(dest.getKey()),
															dest.getValue()	);
						rreq.setDestSeqNum(maxSeqNum);
					} catch (NoSuchRouteException e1) {
						//table route were deleted by the timer
						e.printStackTrace();
					}
				}
			}
		}
		
		for(Integer i : toBeRemoved){
			Logger.v(TAG, "Revmoe " + i + " from RREQ"); 
			rreq.removeDestinations(i);
		}
		
		// Send RREP or rebroadcast RREQ packets
		if (rrep != null && !rrep.getAllDestinations().isEmpty()) {
			sender.queuePDUmessage(rrep);
		} 
		
		if(!rreq.getAllDestinations().isEmpty()){
			Logger.v(TAG, "Still has " + rreq.getAllDestinations().size() + " elements");
			sender.queuePDUmessage(rreq);
		}
		
		return rrep;
	}*/

	/**
	 * 
	 * @param source		The source of this packet
	 * @param hopFromSrc	The hop-count to the source
	 * @param srcSeqNum		Sequence Number of the source node
	 * @param preNode		The node that forward this packet to me
	 */
	/*private void updateRouteByIncomingPacket(int source, int hopFromSrc, int srcSeqNum, int preNode){
		//a reverse route may already exists, so we need to compare route info value to know what to update
		try {
			// Throw an exception if the source address is unavailable. Bad methodology...
			ForwardRouteEntry oldRoute = routeTableManager.getForwardRouteEntry(source);
			
			if(isIncomingRouteInfoBetter(	srcSeqNum,
											oldRoute.getDestinationSequenceNumber(),
											hopFromSrc,
											oldRoute.getHopCount())){
				
				//remove the old entry and then replace with new information
				routeTableManager.updateForwardRouteEntry(oldRoute,
						new ForwardRouteEntry(	source,	preNode, hopFromSrc, srcSeqNum,
												oldRoute.getPrecursors()));
			}
		} catch (NoSuchRouteException e) {
			// Creates a reverse route for the RREP that may be received later on
			routeTableManager.createForwardRouteEntry(	source,	preNode, srcSeqNum,
														hopFromSrc, true);
			// The previous stored route is invalid now. Just validate it again
		} catch (RouteNotValidException e) {
			try {
				routeTableManager.setValid(source, srcSeqNum);
			} catch (NoSuchRouteException e1) {
				routeTableManager.createForwardRouteEntry(	source,	preNode, srcSeqNum,	hopFromSrc, true);
			}
		}
	}*/
	
	/**
	 * Handles a RERR message when received
	 * 
	 * @param rerrMsg is the received error message
	 */
/*	private void routeErrorRecived(RERR rerrMsg) {
		Logger.v(TAG, "RERR received, unreachableNode: "+rerrMsg.getUnreachableNodeAddress());
		try {
			ForwardRouteEntry entry = routeTableManager.getForwardRouteEntry(
															rerrMsg.getUnreachableNodeAddress());

			//only send a RERR if the message contains a seqNum that is greater or equal to the entry known in the table
			if (isIncomingSeqNrBetter(rerrMsg.getUnreachableNodeSequenceNumber(), entry.getDestinationSequenceNumber()))
			{
				RERR rerr = new RERR(	rerrMsg.getUnreachableNodeAddress(),
										rerrMsg.getUnreachableNodeSequenceNumber(), 
										entry.getPrecursors()	);
				sender.queuePDUmessage(rerr);
				routeTableManager.setInvalid(rerrMsg.getUnreachableNodeAddress(), rerrMsg.getUnreachableNodeSequenceNumber());
			}
		} catch (AodvException e) {
			//no route is known so we do not have to react on the error message
		}
	}*/

	/**
	 * Handles a userDataPacket when received
	 * @param userData is the received packet
	 * @param senderNodeAddress the originator of the message
	 */
	private void userDataPacketReceived(UserDataPacket userData, int senderAddress) {
		// Broadcast is unlikely to happen here. UserDataPacket is used on one to one communication. BroadcastPacket
		// is specifically designed to carry broadcast data
		userData.incrementHopCount();
		if (userData.getDestinationAddress() == nodeAddress 
				|| userData.getDestinationAddress() == Constants.BROADCAST_ADDRESS	) {
			parent.notifyAboutDataReceived(userData.getSourceNodeAddress(), userData);
			
		} else {
			//sender.queueUserMessageToForward(userData);
		}
		
		// Update the route to the UserDataPacket Source
		/*updateRouteByIncomingPacket(userData.getSourceNodeAddress(), userData.getHopCount(), 
				userData.getSourceSeqNum(), senderAddress);*/
	}
	
	/**
	 * Computes the maximum of the two sequence numbers, such that the possibility of rollover is taken into account
	 * @param firstSeqNum the first of the given sequence numbers which to compare
	 * @param secondSeqNum the second of the given sequence numbers which to compare
	 * @return returns the maximum sequence number
	 */
	/*public static int getMaximumSeqNum(int firstSeqNum, int secondSeqNum){
		if(isIncomingSeqNrBetter(firstSeqNum, secondSeqNum)){
			return firstSeqNum;
		} else {
			return secondSeqNum;
		}
	}*/
	
	/**
	 * Used to compare sequence numbers
	 * @param incomingSeqNum the sequence number contained in a received AODV PDU message
	 * @param currentSeqNum the sequence number contained in a known forward route
	 * @return returns true if incomingSeqNr is greater or equal to currentSeqNr
	 */
	/*private static boolean isIncomingSeqNrBetter(int incomingSeqNum, int currentSeqNum) {
		return isIncomingRouteInfoBetter(incomingSeqNum, currentSeqNum, 0, 1);
	}*/
	
	/**
	 * Used to compare sequence numbers and hop count
	 * @param incommingSeqNum the sequence number contained in a received AODV PDU message
	 * @param currentSeqNum the sequence number contained in a known forward route
	 * @return returns true if incomingSeqNum > currentSeqNum OR incomingSeqNum == currentSeqNum AND incomingHopCount < currentHopCount  
	 */
	/*protected static boolean isIncomingRouteInfoBetter(int incomingSeqNum, int currentSeqNum, int incomingHopCount, int currentHopCount) {
		if (Math.abs(incomingSeqNum - currentSeqNum) > Constants.SEQUENCE_NUMBER_INTERVAL) {

			if ((incomingSeqNum % Constants.SEQUENCE_NUMBER_INTERVAL) >= (currentSeqNum % Constants.SEQUENCE_NUMBER_INTERVAL)) {
				if ((incomingSeqNum % Constants.SEQUENCE_NUMBER_INTERVAL) == (currentSeqNum % Constants.SEQUENCE_NUMBER_INTERVAL)
						&& incomingHopCount > currentHopCount) {
					return false;
				}
				return true;
			} else {
				//the node have an older route so it should not be used
				return false;
			}
		} 
		else {
			if (incomingSeqNum >= currentSeqNum) {
				if (incomingSeqNum == currentSeqNum && incomingHopCount > currentHopCount) {
					return false;
				}
				return true;
			} 
			else {
				return false;
			}
		}
	}*/

	/**
	 * @author Jay A class to contain the received data from a lower network layer (UDP). Objects
	 *         of this type is stored in a receiving queue for later processing
	 * 
	 */
	private class Message {
		private int senderNodeAddress;
		private byte[] data;

		public Message(int senderNodeAddress, byte[] data) {
			this.senderNodeAddress = senderNodeAddress;
			this.data = data;
		}
		
		public int getSender(){
			return senderNodeAddress;
		}

		public byte getType() {
			Packet p = Packet.parseFromByteArray(data, Packet.class);
			if(p != null){
				//Logger.v(TAG, "Receive Type " + p.getPduType());
				return p.getPduType();
			}
			else
				return -1;
		}
	}
}
