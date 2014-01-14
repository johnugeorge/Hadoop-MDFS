package adhoc.aodv;

import java.io.IOException;
import java.net.BindException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import adhoc.aodv.exception.AodvException;
import adhoc.aodv.exception.DataExceedsMaxSizeException;
import adhoc.aodv.exception.InvalidNodeAddressException;
import adhoc.aodv.pdu.AodvPDU;
import adhoc.aodv.pdu.BroadcastPacket;
import adhoc.aodv.pdu.HelloPacket;
import adhoc.aodv.pdu.Packet;
import adhoc.aodv.pdu.UserDataPacket;
//import adhoc.etc.AndroidDataLogger;
//import adhoc.etc.AndroidDataLogger.LogFileInfo.LogFileName;
import adhoc.etc.Logger;
import adhoc.udp.UdpSender;

public class Sender implements Runnable{
	private Node parent;
    private int nodeAddress;
    private ScheduledThreadPoolExecutor broadcastExecutor = new ScheduledThreadPoolExecutor(1);
    
    private Queue<Packet> pduMessages;
    private Queue<Long> broadcastMsgIDs;	// Hack... Can only cache so many cached broadcasted packets
    private Queue<UserDataPacket> userMessagesToForward;
    private Queue<UserDataPacket> userMessagesFromNode;
    
    private final Object queueLock = new Integer(0);
    //private RouteTableManager routeTableManager;
    private UdpSender udpSender;
    private boolean isRREQsent = false;
    private volatile boolean keepRunning = true;
    private Thread senderThread;
    //private AndroidDataLogger dataLogger;
    private static final String TAG=Sender.class.getSimpleName();
    
    public Sender(Node parent,int nodeAddress, RouteTableManager routeTableManager) throws SocketException, UnknownHostException, BindException {
    	this.parent = parent;
        this.nodeAddress = nodeAddress;
		udpSender = new UdpSender();
        pduMessages = new ConcurrentLinkedQueue<Packet>();	//  protocol messages
        broadcastMsgIDs = new LinkedBlockingQueue<Long>(Constants.BROADCAST_CACHE_SIZE);
        userMessagesToForward = new ConcurrentLinkedQueue<UserDataPacket>();
        userMessagesFromNode = new ConcurrentLinkedQueue<UserDataPacket>();
        //this.routeTableManager = routeTableManager;
        //dataLogger = parent.getDatalogger();
        
    }
    
    public void startThread(){
    	keepRunning = true;
	//startHelloBroadcast();
    	senderThread = new Thread(this, Sender.class.getSimpleName());
    	senderThread.start();
    }
    
    public void stopThread(){
    	keepRunning = false;
    	broadcastExecutor.shutdown();
    	udpSender.closeSoket();
    	senderThread.interrupt();
    }
    
    /**
     * This Thread is responsible checking the message queue
     */
    public void run(){
    	while(keepRunning){
        	synchronized(queueLock){
    			while(isAllEmpty()){
    				try {
						queueLock.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
    			}
    		}
    		
        	/*
        	 * First handle user data messages that is sent from this node. Keep sending until userMessagesFromNode 
        	 * queue is empty
        	 */
        	//if(!isRREQsent){
	    		UserDataPacket userData = userMessagesFromNode.peek(); 
	    		while(userData != null){
	    			StringBuilder str = new StringBuilder();
	    			str.append(System.currentTimeMillis() + ", ");
	    			str.append("userdata, " + userData.getUserDataType() + ", ");
	    			str.append(parent.getNodeId() + ", " + userData.getDestinationAddress() + ", ");
	    			str.append(userData.getHopCount());
	    			str.append("\n\n\n\n");
	    			//System.out.println(" UserData Packet "+str);
	    			//dataLogger.appendSensorData(LogFileName.PACKET_SENT, str.toString());
	    			
	    			try{
	    				// Fail to send the user data due to non-existing route.
						if(!sendUserDataPacket(userData)){
							isRREQsent = true;
							//do not process any user messages before the head is sent
							break;
						} else {
							parent.notifyAboutDataSentSucces((int)userData.getPacketID());
						}
					} catch (DataExceedsMaxSizeException e) {
						parent.notifyAboutSizeLimitExceeded((int)userData.getPacketID());
						e.printStackTrace();
	    			} catch (InvalidNodeAddressException e) {
	    				parent.notifyAboutInvalidAddressGiven((int)userData.getPacketID());
	    				e.printStackTrace();
					}
	    			//it is expected that the queue still has the same userDataHeader object as head
		    		userMessagesFromNode.poll();				    		
	    			userData = userMessagesFromNode.peek();
	    		}
        	//}
        	
        	/*
        	 * Handles user data messages (received from other nodes) that are to be forwarded 
        	 */
    		userData = userMessagesToForward.peek();
    		while(userData != null){
    			StringBuilder str = new StringBuilder();
    			str.append(System.currentTimeMillis() + ", ");
    			str.append("userdata, " + userData.getUserDataType() + ", ");
    			str.append(parent.getNodeId() + ", " + userData.getDestinationAddress() + ", ");
    			str.append(userData.getHopCount());
    			str.append("\n");
    			//dataLogger.appendSensorData(LogFileName.PACKET_FORWARD, str.toString());
    			
    			try{
    				// Hack. Not sure how RREQ Error is handled. isRREQsent is not set back to false sometimes and block the entire user queue
		    		if(!sendUserDataPacket(userData)){
		    			isRREQsent = true; // Needed?
		    			//break;
		    		}
		    		else{
		    			isRREQsent = false;
		    		}
    			} catch (InvalidNodeAddressException e) {
    				e.printStackTrace();
    				e.printStackTrace();
				} catch (DataExceedsMaxSizeException e) {
					e.printStackTrace();
				}
    			//it is expected that the queue still has the same userDataHeader object as head
    			userMessagesToForward.poll();
    			userData = userMessagesToForward.peek();
    		}
    		
    		/*
    		 * Handle protocol messages 
    		 */
    		Packet packet = pduMessages.poll();
    		while(packet != null){
    			StringBuilder str = new StringBuilder();
	    		str.append(System.currentTimeMillis() + ", ");
	    		str.append(packet.getPduType() + ", " + packet.getPduType() + ", " );
	    		str.append(parent.getNodeId() + ", " + packet.getDestinationAddress());
	    		str.append("\n");
	    		//dataLogger.appendSensorData(LogFileName.PACKET_SENT, str.toString());
	    		
    			if(packet instanceof AodvPDU){
    				/*AodvPDU pdu = (AodvPDU)packet;
    				try {
						handleAodvPDU(pdu);
					} catch (InvalidNodeAddressException e) {
						e.printStackTrace();
					}*/ 
    			} else if(packet instanceof HelloPacket){
    				broacastPacket(packet);
    			} else if(packet instanceof BroadcastPacket){
    				BroadcastPacket brdPkt = (BroadcastPacket) packet;
    				cacheBroadcastID(brdPkt.getPacketID());
    				broacastPacket(packet);
    				
    			} else {
    				Logger.e(TAG, "Sender queue contains an unknown message Packet PDU!");
    			}
	    		packet = pduMessages.poll();
    		}
    	}    	
    }
    
    /**
     * Check if all the queue are empty
     * @return
     */
    private boolean isAllEmpty(){
    	return pduMessages.isEmpty() && userMessagesToForward.isEmpty() && (isRREQsent || userMessagesFromNode.isEmpty());
    }
    
    /**
     * Handle the internal message
     * @param pdu
     * @throws InvalidNodeAddressException
     * @throws DataExceedsMaxSizeException
     */
    /*private void handleAodvPDU(AodvPDU pdu) throws InvalidNodeAddressException{
		switch (pdu.getPduType()) {
			case Constants.RREQ_PDU:
				broacastPacket(pdu);
				if(pdu.getSourceAddress() == nodeAddress){
					try {
						routeTableManager.setRouteRequestTimer(	((RREQ)pdu).getSourceAddress(),
																((RREQ)pdu).getBroadcastId()	);
					} catch (NoSuchRouteException e) {
						e.printStackTrace();
					}
				}
				break;
				
			case Constants.RREP_PDU:						
				if(!sendAodvPacket(pdu,pdu.getSourceAddress())){
					Logger.e(TAG, "Did not have a forward route for sending back the RREP message to: "+pdu.getSourceAddress()+" the requested destination is: "+pdu.getDestinationAddress());
				}
				break;
				
			case Constants.RERR_PDU:
				RERR rerr = (RERR)pdu;
				for(int nodeAddress: rerr.getAllDestAddresses()){
					if(!sendAodvPacket(new RERR(	rerr.getUnreachableNodeAddress(),
											rerr.getUnreachableNodeSequenceNumber(),
											nodeAddress	), nodeAddress)){
						Logger.e(TAG, "Did not have a forward route for sending the RERR message!!");
					}
				}
				break;
				
			case Constants.RREQ_FAILURE_PDU:
				cleanUserDataPacketsFromNode(pdu.getDestinationAddress());
				isRREQsent = false;
				synchronized (queueLock) {
					queueLock.notify();
				}
				break;
				
			case Constants.FORWARD_ROUTE_CREATED:
				UserDataPacket userPacket = userMessagesFromNode.peek();
				if(userPacket != null && pdu.getDestinationAddress() == userPacket.getDestinationAddress()){
					isRREQsent = false;
					synchronized (queueLock) {
						queueLock.notify();
					}
				}
				break;
				
			default:
				Logger.e(TAG, "Sender queue contained an unknown message AODV PDU!");
				break;
		}
    }*/
    /**
     * @param packet is the message which are to be broadcasted to the neighboring nodes
     * @throws SizeLimitExceededException 
     */
	private boolean broacastPacket(Packet packet) {
		try {
			return udpSender.sendPacket(Constants.BROADCAST_ADDRESS, packet.toBytes());
		} catch (IOException e) {
			e.printStackTrace();
		} catch (DataExceedsMaxSizeException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/*
	 * Implement a round-robin queue
	 */
	protected void cacheBroadcastID(long id){
		if(broadcastMsgIDs.size() >= Constants.BROADCAST_CACHE_SIZE){
			broadcastMsgIDs.poll();
		}
		broadcastMsgIDs.add(id);
	}
	
	
	private boolean sendUserDataPacket(UserDataPacket packet) throws DataExceedsMaxSizeException, InvalidNodeAddressException{
		if(	packet.getDestinationAddress() != Constants.BROADCAST_ADDRESS
			&& packet.getDestinationAddress() >= Constants.MIN_VALID_NODE_ADDRESS
			&& packet.getDestinationAddress() <= Constants.MAX_VALID_NODE_ADDRESS){
			
			if(packet.getDestinationAddress() == nodeAddress){
				throw new InvalidNodeAddressException("It is not allowed to send to our own address: "+nodeAddress);
			}
			
			
			try {
				return udpSender.sendPacket(packet.getDestinationAddress(), packet.toBytes());
			} catch (IOException e) {
				e.printStackTrace();
				return false;
			}
			/*try {
				// Throw an AODVException if there is no such route in the routing table... Bad methodology
				int nextHop = routeTableManager.getForwardRouteEntry(packet.getDestinationAddress()).getNextHop();
				try {
					return udpSender.sendPacket(nextHop, packet.toBytes());
				} catch (IOException e) {
					e.printStackTrace();
					return false;
				}
				
			}
			catch (AodvException e) {	// Can be either NoSuchRouteException or RouteNotValidException
				e.printStackTrace();
				try {
					// Throw an AODVException if there was no such route in the routing table... Bad methodology
					int lastKnownDestSeqNum = routeTableManager.getLastKnownDestSeqNum(packet.getDestinationAddress());
					if(packet.getSourceNodeAddress() == nodeAddress){
						// Create a new RREQ
						if(!createNewRREQ(packet.getDestinationAddress(), lastKnownDestSeqNum, false)){
							Logger.e(TAG, "Sender: Failed to add new RREQ entry to the request table. Src: "+nodeAddress+" broadID: "+parent.getCurrentBroadcastID());
							return false;
						}
					} else {
						queuePDUmessage(new RERR(	packet.getDestinationAddress(), 
													lastKnownDestSeqNum,
													packet.getSourceNodeAddress()	)	);
						cleanUserDataPacketsToForward(packet.getDestinationAddress());
					}
				} catch (NoSuchRouteException e1) {
					e.printStackTrace();
					if(packet.getSourceNodeAddress() == nodeAddress){
						// Search for an totally unknown destination
						createNewRREQ(packet.getDestinationAddress(), Constants.UNKNOWN_SEQUENCE_NUMBER, false);
					} else {
						queuePDUmessage(new RERR(	packet.getDestinationAddress(), 
													Constants.UNKNOWN_SEQUENCE_NUMBER,
													packet.getSourceNodeAddress()	)	);
						cleanUserDataPacketsToForward(packet.getDestinationAddress());
					}
				}
				return false;
			}*/
		} else if( packet.getDestinationAddress() == Constants.BROADCAST_ADDRESS){
			return broacastPacket(packet);
		} else {
			 throw new InvalidNodeAddressException("Sender: got request to send a user packet which had  an invalid node address: "+packet.getDestinationAddress());
		}
	}
    
	/**
	 * Note: this method is able to send messages to itself if necessary. Note: DO NOT USE FOR BROADCASTING
	 * @param destinationNodeAddress should not be exchanged as the 'nextHopAddress'. DestinationNodeAddress is the final place for this packet to reach
	 * @param packet is the message to be sent
	 * @return false if no route to the desired destination is currently known.
	 * @throws InvalidNodeAddressException 
	 * @throws SizeLimitExceededException 
	 */
    /*private boolean sendAodvPacket(AodvPDU packet, int destinationNodeAddress) {
    	if(destinationNodeAddress >= Constants.MIN_VALID_NODE_ADDRESS 
    			&& destinationNodeAddress <= Constants.MAX_VALID_NODE_ADDRESS){
			try {
				int nextHop = routeTableManager.getForwardRouteEntry(destinationNodeAddress).getNextHop();
		    		return udpSender.sendPacket(nextHop, packet.toBytes());
				return udpSender.sendPacket(destinationNodeAddress, packet.toBytes());
			} catch (IOException e) {
				Logger.e(TAG, "IOExeption when trying to send a packet to: "+destinationNodeAddress);
			} catch (AodvException e){
				Logger.e(TAG, "There is no route to: "+destinationNodeAddress);
			}
    	} else {
    		Logger.e(TAG, "Try to send an AODV packet but the destination address is out valid range");
    	}
    	return false;
    }*/
    
    /**
     * 
     * Creates and queues a new RREQ
     * @param destinationNodeAddress is the destination that you want to discover a route to
     * @param lastKnownDestSeqNum
     * @param setTimer is set to false if the timer should not start count down the entry's time
     * @return returns true if the route were created and added successfully.
     */
    /*public boolean createNewRREQ(int destinationNodeAddress, int lastKnownDestSeqNum, boolean setTimer){
    	RREQ rreq = new RREQ(nodeAddress,
				destinationNodeAddress,
				parent.getNextSequenceNumber(),
				lastKnownDestSeqNum,
				parent.getNextBroadcastID()	);
    	if(routeTableManager.createRouteRequestEntry(rreq,setTimer)){
    		queuePDUmessage(rreq);
    		return true;
    	}
    	return false;
    }*/
    
    /**
     * 
     * @param destSet <DestinationAddress, DestinationSequenceNumber>
     * @param setTimer
     * @return
     */
    /*public boolean createNewRREQ(HashMap<Integer, Integer> destSet, boolean setTimer){
    	if(destSet.isEmpty())
    		return false;
    	
    	// Randomly put one destination in the global variable. This is not going to be used anyway
    	Entry<Integer, Integer> oneEntry = destSet.entrySet().iterator().next();
    	
    	RREQ rreq = new RREQ(nodeAddress, oneEntry.getKey(), parent.getNextSequenceNumber(), 
    			oneEntry.getValue(), parent.getNextBroadcastID());
    	rreq.setAllDestinations(destSet);
    	
    	if(routeTableManager.createRouteRequestEntry(rreq,setTimer)){
    		queuePDUmessage(rreq);
    		return true;
    	}
    	return false;
    }*/
    
    /**
     * Method for queuing protocol messages for sending
     * @param aodvPDU is the Protocol Data Unit to be queued. 
     */
    protected void queuePDUmessage(AodvPDU aodvPDU){
    	pduMessages.add(aodvPDU);
    	synchronized (queueLock) {
    		queueLock.notify();	
		}
    }
    
    protected void queuePacket(Packet packet){
    	pduMessages.add(packet);
    	synchronized (queueLock) {
    		queueLock.notify();
		}
    }
    

    /*protected void queueUserMessageToForward(UserDataPacket userData){
    	userMessagesToForward.add(userData);
    	synchronized (queueLock) {
			queueLock.notify();
		}
    }*/
    
    protected void queueUserMessageFromNode(UserDataPacket userPacket){
    	userMessagesFromNode.add(userPacket);
    	//Logger.v(TAG, "New User Message is queued. Type: " + userPacket.getData().getPacketType());
    	synchronized (queueLock) {
    		queueLock.notify();
		}
    }
    
    public boolean isBroadcastIDReceived(long id){
    	return broadcastMsgIDs.contains(id);
    }
    
    /*private void cleanUserDataPacketsToForward(int destinationAddress){
    	synchronized (userMessagesToForward) {
			for(UserDataPacket msg: userMessagesToForward){
				if(msg.getDestinationAddress() == destinationAddress){
					userMessagesToForward.remove(msg);
				}
			}
		}
    }*/
    
    
    /**
     * Removes every message from the user packet queue that matches the given destination
     * @param destinationAddress the destination which to look for
     */
    /*private void cleanUserDataPacketsFromNode(int destinationAddress){
    	synchronized (userMessagesFromNode) {
			for(UserDataPacket msg: userMessagesFromNode){
				if(msg.getDestinationAddress() == destinationAddress){
					userMessagesFromNode.remove(msg);
				}
			}
		}
    }*/
    
    private void startHelloBroadcast(){
    	broadcastExecutor.scheduleAtFixedRate(new Runnable(){
    		@Override
    		public void run() {
    			queuePacket(new HelloPacket(nodeAddress,parent.getCurrentSequenceNumber()));
    		}
    	}, 0, Constants.BROADCAST_INTERVAL, TimeUnit.MILLISECONDS);
    }
}
