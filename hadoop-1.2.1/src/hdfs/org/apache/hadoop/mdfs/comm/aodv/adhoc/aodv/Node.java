package adhoc.aodv;

import java.net.BindException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Observable;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import adhoc.aodv.exception.InvalidNodeAddressException;
import adhoc.aodv.pdu.AODVDataContainer;
import adhoc.aodv.pdu.AodvPDU;
import adhoc.aodv.pdu.BroadcastPacket;
import adhoc.aodv.pdu.UserDataPacket;
//import adhoc.etc.AndroidDataLogger;
import adhoc.etc.IOUtilities;
import adhoc.etc.Logger;
import adhoc.etc.MyTextUtils;
import adhoc.tcp.TCPConnection;



/**
 * <pre>Note - Any observer should implement their update methods in the following way:
public void update(Observable o, Object arg) {
	MessageToObserver msg = (MessageToObserver)arg;
	int userPacketID, destination, type = msg.getMessageType();
	
	switch (type) {
	case ObserverConst.ROUTE_ESTABLISHMENT_FAILURE:
		//Note: any messages that had same destination has been removed from sending 
		int unreachableDestinationAddrerss  = (Integer)msg.getContainedData();
		...
		break;
	case ObserverConst.DATA_RECEIVED:
		byte[] data = (byte[])msg.getContainedData();
		int senderAddress = (Integer)((PacketToObserver)msg).getSenderNodeAddress();
		...
		break;
	case ObserverConst.DATA_SENT_SUCCESS:
		userPacketID = (Integer)msg.getContainedData();
		...
		break;
	case ObserverConst.INVALID_DESTINATION_ADDRESS:
		userPacketID = (Integer)msg.getContainedData();
		...
		break;
	case ObserverConst.DATA_SIZE_EXCEEDES_MAX:
		userPacketID = (Integer)msg.getContainedData();
		...
		break;
	case ObserverConst.ROUTE_INVALID:
		destination  = (Integer)msg.getContainedData();
		...
		break;
	case ObserverConst.ROUTE_CREATED:
		destination = (Integer)msg.getContainedData();
		...
		break;
	default:
		break;
	}
}
 * </pre>
 * @author Rabie
 *
 */
public class Node extends Observable {
	private int nodeAddress;
	private int nodeSequenceNumber = Constants.FIRST_SEQUENCE_NUMBER;
	private int nodeBroadcastID = Constants.FIRST_BROADCAST_ID;
    private Sender sender;
    private Receiver receiver;
    private RouteTableManager routeTableManager;
    private Object sequenceNumberLock = 0;
    private Queue<MessageToObserver> messagesForObservers;
    private volatile boolean keepRunning = true;
    private static final String TAG = Node.class.getSimpleName();
    private static Node instance = null;
    private MessageQueueChecker checker;
    //private AndroidDataLogger dataLogger;

	/**
	 * Creates an instance of the Node class
	 * @param nodeAddress
	 * @throws InvalidNodeAddressException Is thrown if the given node address is outside of the valid interval of node addresses
	 * @throws SocketException is cast if the node failed to instantiate port connections to the ad-hoc network
	 * @throws UnknownHostException
	 * @throws BindException this exception is thrown if network interface already is connected to a another network 
	 */
    private Node(int nodeAddress) {
    	if(nodeAddress > Constants.MAX_VALID_NODE_ADDRESS 
    			|| nodeAddress < Constants.MIN_VALID_NODE_ADDRESS){
    		//given address is out of the valid range
    		Logger.e(TAG, "Invalid IP");
    		return;
    	}
    	this.nodeAddress = nodeAddress;
    	routeTableManager = new RouteTableManager(nodeAddress, this);
    	messagesForObservers = new ConcurrentLinkedQueue<MessageToObserver>();
    	TCPConnection.getInstance().setRouteTableManager(routeTableManager);
    	/*dataLogger = new AndroidDataLogger();
    	AndroidDataLogger.createRequiredFiles();
    	dataLogger.init();*/
        
    	try {
			sender = new Sender(this, nodeAddress, routeTableManager);
			receiver = new Receiver(sender, nodeAddress, this, routeTableManager);
		} catch (BindException e) {
			Logger.e(TAG, e.toString());
		} catch (SocketException e) {
			Logger.e(TAG, e.toString());
		} catch (UnknownHostException e) {
			Logger.e(TAG, e.toString());
		}
    	
    }
    
    public static synchronized Node getInstance() {
		if (instance == null) {
			String localIp = IOUtilities.getLocalIpAddress();
			if(!MyTextUtils.isEmpty(localIp)){
				int nodeId = IOUtilities.parseNodeNumber(localIp);
		    	instance = new Node(nodeId);
			}
			else{
				Logger.e(TAG, "IP is not available. Fail to initiate");
			}
		}
		return instance;
	}
    
    public int getNodeId(){
    	return nodeAddress;
    }
    
    /*public AndroidDataLogger getDatalogger(){
    	return dataLogger;
    }*/
    
    /**
     * Starts executing the AODV routing protocol 
     * @throws UnknownHostException 
     * @throws SocketException 
     * @throws BindException 
     */
    public void startThread(){
    	keepRunning = true;
    	routeTableManager.startTimerThread();
		sender.startThread();
		receiver.startThread();
		checker = new MessageQueueChecker();
		checker.start();
    	Logger.v(TAG, "All library threads are running");
    }
    
    /**
     * Stops the AODV protocol. 
     * 
     * Note: using this method tells the running threads to terminate. 
     * This means that it does not insure that any remaining userpackets is sent before termination.
     * Such behavior can be achieved by monitoring the notifications by registering as an observer.
     */
    public void stopThread(){
    	keepRunning = false;
    	receiver.stopThread();
    	sender.stopThread();
    	routeTableManager.stopTimerThread();
    	checker.interrupt();
    	TCPConnection.stopAllTCP();
    	instance=null;
    	Logger.v(TAG, "All library threads are stopped");
    }

    /**
     * Method to be used by the application layer to send data to a single destination node or all neighboring nodes (broadcast).
     */
    /*public void sendData(int packetIdentifier, int destinationAddress, byte[] data){
    	sender.queueUserMessageFromNode(new UserDataPacket(packetIdentifier,destinationAddress, data, nodeAddress));
    }*/
    
    public void sendAODVDataContainer(AODVDataContainer packet){
    	if(packet.isBroadcast()){
    		BroadcastPacket pkt = new BroadcastPacket(packet.getSource(), getNextSequenceNumber(),
    				packet.getMaxHop(), packet);
    		sender.queuePacket(pkt);
    		//sender.queuePacket(pkt);	// Try 2 times...
    		//sender.queuePacket(pkt);	// Try 3 times...
    	}
    	else{
    		//AODV_DATA_CONTAINER
    		UserDataPacket pkt = new UserDataPacket(packet.getTimestamp(), packet.getSource(), packet.getDest(), packet); 
    		pkt.setSourceSequenceNum(getNextSequenceNumber());
    		sender.queueUserMessageFromNode(pkt);
    	}
    }
	
    /**
     * Method for getting the current sequence number for this node
     * @return an integer value of the current sequence number
     */
	protected int getCurrentSequenceNumber(){
		return nodeSequenceNumber;
	}
	
	/**
	 * Increments the given number but does NOT set this number as the nodes sequence number
	 * @param number is the number which to increment
	 */
	protected int getNextSequenceNumber(int number){
		if((number >= Constants.MAX_SEQUENCE_NUMBER || number < Constants.FIRST_SEQUENCE_NUMBER)){
			return Constants.FIRST_SEQUENCE_NUMBER;
		} else {
			return number++;
		}
	}
	
	
	/**
	 * Increments and set the sequence number before returning the new value. 
	 * @return returns the next sequence number
	 */
	protected int getNextSequenceNumber(){
		synchronized (sequenceNumberLock) {
			if(nodeSequenceNumber == Constants.UNKNOWN_SEQUENCE_NUMBER
					|| nodeSequenceNumber == Constants.MAX_SEQUENCE_NUMBER	){
				
				nodeSequenceNumber = Constants.FIRST_SEQUENCE_NUMBER;
			}
			else{
				nodeSequenceNumber++;	
			}
			return nodeSequenceNumber;
		}
	}

	/**
	 * Increments the broadcast ID 
	 * @return returns the incremented broadcast ID
	 */	
	protected int getNextBroadcastID() {
		synchronized (sequenceNumberLock) {
			if(nodeBroadcastID == Constants.MAX_BROADCAST_ID){
				nodeBroadcastID = Constants.FIRST_BROADCAST_ID;
			} else {
				nodeBroadcastID++;
			}
			return nodeBroadcastID;			
		}
	}
	
	/**
	 * Only used for debugging
	 * @return returns the current broadcast ID of this node
	 */
	protected int getCurrentBroadcastID(){
		return nodeBroadcastID;
	}
	
	/**
	 * Notifies the application layer about 
	 * @param senderNodeAddess the source node which sent a message
	 * @param data the actual data which the application message contained
	 */
	protected void notifyAboutDataReceived(int senderNodeAddess, UserDataPacket packet) {
		//System.out.print("Received data type "+packet.getData().getPacketType()+" from Node "+senderNodeAddess);
		messagesForObservers.add(new AODVDataToObserver(ObserverConst.AODV_DATA_CONTAINER, packet.getData()));
		wakeNotifierThread();
	}
	
	/**
	 * Notifies the application layer  
	 * @param senderNodeAddess the source node which sent a message
	 * @param data the actual data which the application message contained
	 * @author Jay
	 */
	protected void notifyAboutDataReceived(int senderNodeAddess, AODVDataContainer data) {	
		 messagesForObservers.add(new AODVDataToObserver(ObserverConst.AODV_DATA_CONTAINER, data));
		 //Logger.i(TAG, "Packet type " + data.getPacketType() + " is added to messagesForObservers");
		 wakeNotifierThread();
	}
	
	/**
	 * Notifies the observer(s) about the route establishment failure for a destination
	 * @param nodeAddress is the unreachable destination
	 */
	protected void notifyAboutRouteEstablishmentFailure(int faliedToReachAddress) {
		messagesForObservers.add(new ValueToObserver(faliedToReachAddress, ObserverConst.ROUTE_ESTABLISHMENT_FAILURE));
		wakeNotifierThread();
	}
	
	/**
	 * Notifies the observer(s) that a packet is sent successfully from this node.
	 * NOTE: This does not guarantee that the packet also is received at the destination node
	 * @param packetIdentifier the ID of a packet which the above layer can recognize
	 */
	protected void notifyAboutDataSentSucces(int packetIdentifier){
		messagesForObservers.add(new ValueToObserver(packetIdentifier, ObserverConst.DATA_SENT_SUCCESS));
		wakeNotifierThread();
	}
	
	/**
	 * Notifies the observer(s) that an invalid destination address where detected for a user packet to be sent
	 * @param packetIdentifier an integer that identifies the user packet with bad destination address 
	 */
	protected void notifyAboutInvalidAddressGiven(int packetIdentifier){
		messagesForObservers.add(new ValueToObserver(packetIdentifier, ObserverConst.INVALID_DESTINATION_ADDRESS));
		wakeNotifierThread();
	}
	
	protected void notifyAboutSizeLimitExceeded(int packetIdentifier){
		messagesForObservers.add(new ValueToObserver(packetIdentifier, ObserverConst.DATA_SIZE_EXCEEDES_MAX));
		wakeNotifierThread();
	}
	
	protected void notifyAboutRouteToDestIsInvalid(int destinationAddress){
		messagesForObservers.add(new ValueToObserver(destinationAddress, ObserverConst.ROUTE_INVALID));
		wakeNotifierThread();
	}
	
	protected void notifyAboutNewNodeReachable(int destinationAddress){
		messagesForObservers.add(new ValueToObserver(destinationAddress, ObserverConst.ROUTE_CREATED));
		wakeNotifierThread();
	}
	
	private void wakeNotifierThread(){
		synchronized (messagesForObservers) {
			messagesForObservers.notify();
		}
	}
	/**
	 * This interface defines the a structure for an observer to retrieve a message from the observable
	 * @author rabie
	 *
	 */
	public interface MessageToObserver{
		
		/**
		 * 
		 * @return returns the type of this message as an Integer
		 */
		public int getMessageType();
		
		/**
		 * This method is used to retrieve the data that the observable wants to notify about
		 * @return returns the object that is contained
		 */
		public Object getContainedData();
		
	}
	
	/**
	 * This Object is soly used to wrap the AODVDataContainer and send to the application layer 
	 * @author Jay
	 */
	public static class AODVDataToObserver implements MessageToObserver{
		private AODVDataContainer data;
		private int type;
		
		public AODVDataToObserver(int t, AODVDataContainer d){
			this.type = t;
			this.data = d;
		}
		
		@Override
		public int getMessageType() {
			return type;
		}
		@Override
		public Object getContainedData() {
			return data;
		}
		
	}
	
	public class ValueToObserver implements MessageToObserver{
		private Integer value;
		private int type;
		
		public ValueToObserver(int value, int msgType) {
			this.value = new Integer(value);
			type = msgType;
		}
		@Override
		public Object getContainedData() {
			return value;
		}

		@Override
		public int getMessageType() {
			return type;
		}
		
	}
	
	/**
	 * This class presents a received package from another node, to the application layer
	 * @author Rabie
	 *
	 */
	public class PacketToObserver implements MessageToObserver{
		private byte[] data;
		private int senderNodeAddress;
		private int type;
		
		public PacketToObserver(int senderNodeAddress, byte[] data, int msgType) {
			type = msgType;
			this.data = data;
			this.senderNodeAddress = senderNodeAddress;
		}
		
		/**
		 * A method to retrieve the senders address of this data
		 * @return returns an integer value representing the unique address of the sending node
		 */
		public int getSenderNodeAddress(){
			return senderNodeAddress;
		}

		/**
		 * A method to retrieve the data sent
		 * @return returns a byte array containing the data which 
		 * where sent by another node with this node as destination
		 */
		@Override
		public Object getContainedData() {
			return data;
		}

		@Override
		public int getMessageType() {
			return type;
		}
	}

	protected void queuePDUmessage(AodvPDU pdu) {
		sender.queuePDUmessage(pdu);
	}


	/**
	 * This private class is only used to check the mesageForObsers queue
	 * @author Jay
	 */
	private class MessageQueueChecker extends Thread{
		private MessageQueueChecker(){
			super("MessageQueueChecker");
		}
		
		@Override
		public void run() {
			while(keepRunning){
				try{
					synchronized (messagesForObservers) {
						while(messagesForObservers.isEmpty()){
							messagesForObservers.wait();
						}
					}
					setChanged();
					MessageToObserver tmp=messagesForObservers.poll();
					notifyObservers(tmp);
					//Logger.i(TAG, "Packet  is sent to Observer" + tmp.getMessageType());
				}catch (InterruptedException e) {
					// thread stopped
				}
			}
		}
	}
	
	
	/*
	 * My Test Functions
	 */
	public RouteTableManager getRouteManager(){
		return routeTableManager;
	}
	
	/*public boolean sendRREQ(int destID){
		return sender.createNewRREQ(destID, Constants.UNKNOWN_SEQUENCE_NUMBER, false);
	}
	
	public boolean sendRREQ(HashMap<Integer, Integer> destSet){
		return sender.createNewRREQ(destSet, false);
	}*/
}
