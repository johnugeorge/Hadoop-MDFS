package adhoc.aodv;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;

import adhoc.aodv.exception.NoSuchRouteException;
import adhoc.aodv.exception.RouteNotValidException;
import adhoc.aodv.pdu.InternalMessage;
import adhoc.aodv.pdu.RREQ;
import adhoc.aodv.routes.ForwardRouteEntry;
import adhoc.aodv.routes.ForwardRouteTable;
import adhoc.aodv.routes.RouteEntry;
import adhoc.aodv.routes.RouteRequestEntry;
import adhoc.aodv.routes.RouteRequestTable;
import adhoc.etc.MyTextUtils;

public class RouteTableManager {

	private volatile boolean keepRunning = true;
	private ForwardRouteTable forwardRouteTable;
	private RouteRequestTable routeRequestTable;
	private final Object tableLocks = new Integer(0);
	//private TimeoutNotifier timeoutNotifier;
	private int nodeAddress;
	private Node parent;
	
	private HashSet<Integer> blockingSet = new HashSet<Integer>();

	public RouteTableManager(int nodeAddress, Node parent) {
		this.nodeAddress = nodeAddress;
		this.parent = parent;
		forwardRouteTable = new ForwardRouteTable();
		routeRequestTable = new RouteRequestTable();
		//timeoutNotifier = new TimeoutNotifier();
	}

	public void startTimerThread(){
		keepRunning = true;
		//timeoutNotifier = new TimeoutNotifier();
		//timeoutNotifier.start();	
	}
	
	public void stopTimerThread() {
		keepRunning = false;
		//timeoutNotifier.stopThread();
	}

	
	/**
	 * Creates an entry and adds it to the appropriate table
	 * @param rreq The RREQ entry to be added
	 * @param setTimer is set to false if the timer should not start count down the entry's time
	 * @return returns true if the route were created and added successfully.
	 */
	protected boolean createRouteRequestEntry(RREQ rreq, boolean setTimer) {
		Iterator<Entry<Integer, Integer>> iter = rreq.getAllDestinations().entrySet().iterator();
		
		Entry<Integer, Integer> dest;
		while(iter.hasNext()){
			dest = iter.next(); 
			RouteRequestEntry entry;
			try {
				entry = new RouteRequestEntry( rreq.getBroadcastId(),
												rreq.getSourceAddress(), dest.getValue(),
												rreq.getHopCount(),	dest.getKey());
			} catch (RouteNotValidException e) {
				return false;
			}
			
			if (routeRequestTable.addRouteRequestEntry(entry, setTimer)) {
				if(setTimer){
					//notify the timer since the RREQ table (the sorted list) isn't empty at this point
					synchronized (tableLocks) {
						tableLocks.notify();
					}
				}
				return true;
			}
		}
		
		/*RouteRequestEntry entry;
		try {
			entry = new RouteRequestEntry(	rreq.getBroadcastId(),
											rreq.getSourceAddress(),
											rreq.getDestinationSequenceNumber(),
											rreq.getHopCount(), 
											rreq.getDestinationAddress());
		} catch (RouteNotValidException e) {
			return false;
		}
		
		if (routeRequestTable.addRouteRequestEntry(entry, setTimer)) {
			if(setTimer){
				//notify the timer since the RREQ table (the sorted list) isn't empty at this point
				synchronized (tableLocks) {
					tableLocks.notify();
				}
			}
			return true;
		}*/
		return false;
	}

	/**
	 * Creates an entry and adds it to the appropriate table
	 * @param destinationNodeAddress the destination address which this node will have a route for
	 * @param nextHopAddress is the neighbor address which to forward to if the destination should be reached 
	 * @param destinationSequenceNumber is the sequence number of the destination
	 * @param hopCount the number of intermediate node which will participate to forward a possible package for the destination
	 * @return returns true if the route were created and added successfully.
	 */
	protected boolean createForwardRouteEntry(int destinationNodeAddress, int nextHopAddress, int destinationSequenceNumber,
			int hopCount, boolean notifyObserver) {
		return createForwardRouteEntry(destinationNodeAddress, nextHopAddress, destinationSequenceNumber, hopCount, new ArrayList<Integer>(), notifyObserver);
	}

	/**
	 * Creates an entry and adds it to the appropriate table
	 * @param destinationNodeAddress the destination address which this node will have a route for
	 * @param nextHopAddress is the neighbor address which to forward to if the destination should be reached 
	 * @param destinationSequenceNumber is the sequence number of the destination
	 * @param hopCount the number of intermediate node which will participate to forward a possible package for the destination
	 * @param precursorNodes a list of node addresses which has used this route to forward packages
	 * @return returns true if the route were created and added successfully.
	 */
	protected boolean createForwardRouteEntry(int destinationNodeAddress, int nextHopAddress,
						int destinationSequenceNumber, int hopCount, ArrayList<Integer> precursorNodes, boolean notifyObserver) {
		ForwardRouteEntry forwardRouteEntry;
		try {
			forwardRouteEntry = new ForwardRouteEntry(	destinationNodeAddress,	nextHopAddress,
														hopCount, destinationSequenceNumber, 
														precursorNodes	);
		} catch (RouteNotValidException e) {
			return false;
		}
		if (forwardRouteTable.addForwardRouteEntry(forwardRouteEntry)) {
			synchronized (tableLocks) {
				tableLocks.notify();
			}
			if(notifyObserver){
				parent.notifyAboutNewNodeReachable(destinationNodeAddress);
			}
			parent.queuePDUmessage(new InternalMessage(	Constants.FORWARD_ROUTE_CREATED, destinationNodeAddress)	);
			return true;
		}
		return false;
	}

	protected boolean routeRequestExists(int sourceAddress, int broadcastID) {
		return routeRequestTable.routeRequestEntryExists(sourceAddress, broadcastID);
	}

	/**
	 * method used to check the forward route table if a valid entry exist with a freshness that is as least as required
	 * @param destinationAddress the destination address of the node which a route is will be looked at
	 * @param destinationSequenceNumber specify any freshness requirement
	 * @return returns true if such a valid forward route exist with the seq number or higher 
	 */
	protected boolean validForwardRouteExists(int destinationAddress, int destinationSequenceNumber) {
		RouteEntry forwardRoute;
		try {
			forwardRoute = (ForwardRouteEntry) forwardRouteTable.getForwardRouteEntry(destinationAddress);
		} catch (NoSuchRouteException e) {
			return false;
		} catch (RouteNotValidException e) {
			return false;
		}

		if (forwardRoute.getDestinationSequenceNumber() >= destinationSequenceNumber) {
			return true;
		}
		return false;
	}

	/**
	 * 
	 * @param sourceAddress
	 * @param broadcastID
	 * @param removeEntry
	 * @return returns a RouteRequestEntry if any where found
	 * @throws NoSuchRouteException a NoSuchRouteException is cast in the event of an unsuccessful search
	 */
	protected RouteRequestEntry getRouteRequestEntry(int sourceAddress, int broadcastID, boolean removeEntry)
			throws NoSuchRouteException {

		return (RouteRequestEntry) routeRequestTable.getRouteRequestEntry(sourceAddress, broadcastID, removeEntry);
	}

	/**
	 * 
	 * @param destinationAddress
	 * @return ForwardRouteEntry if exists and valid
	 * @throws NoSuchRouteException
	 * @throws RouteNotValidException
	 */
	public ForwardRouteEntry getForwardRouteEntry(int destinationAddress) throws NoSuchRouteException, RouteNotValidException {
		return forwardRouteTable.getForwardRouteEntry(destinationAddress);
	}
	
	

	/*protected void updateForwardRouteEntry(ForwardRouteEntry oldEntry, ForwardRouteEntry newEntry) throws NoSuchRouteException{
		if (Receiver.isIncomingRouteInfoBetter(	newEntry.getDestinationSequenceNumber(), oldEntry.getDestinationSequenceNumber(), 
												newEntry.getHopCount(),	oldEntry.getHopCount())) {
			if(forwardRouteTable.updateForwardRouteEntry(newEntry)){
				synchronized (tableLocks) {
					tableLocks.notify();
				}
			}
		}
	}*/
	
	public boolean removeForwardRouteEntry(int destinationAddress) {
		return forwardRouteTable.removeEntry(destinationAddress);
	}

	protected int getLastKnownDestSeqNum(int destinationAddress) throws NoSuchRouteException {
		return forwardRouteTable.getLastKnownDestSeqNumber(destinationAddress);
	}
	
	protected ArrayList<Integer> getPrecursors(int destinaitonAdrress){
		return forwardRouteTable.getPrecursors(destinaitonAdrress);
	}

	/**
	 * Makes a forward route valid, updates its sequence number if necessary and resets the AliveTimeLeft
	 * @param destinationAddress used to determine which forward route to set valid
	 * @param newDestinationSeqNumber this destSeqNum is only set in the entry if it is greater that the existing destSeqNum
	 * @throws NoSuchRouteException thrown if no table information is known about the destination
	 */
	/*protected void setValid(int destinationAddress, int newDestinationSeqNumber) throws NoSuchRouteException {
		forwardRouteTable.setValid(destinationAddress, newDestinationSeqNumber,true);
	}*/
	
	/**
	 * Added by Jay. This method allows we also update the hop count
	 * @param destinationAddress
	 * @param newDestinationSeqNumber
	 * @param hopCount
	 * @throws NoSuchRouteException
	 */
	/*protected void setValid(int destinationAddress, int newDestinationSeqNumber, int hopCount) throws NoSuchRouteException {
		forwardRouteTable.setValid(destinationAddress, newDestinationSeqNumber, hopCount, true);
	}*/
	
	/*protected void setInvalid(int destinationAddress, int newDestinationSeqNumber) throws NoSuchRouteException {
		forwardRouteTable.setValid(destinationAddress,newDestinationSeqNumber,false);
	}*/

	/**
	 * 
	 * @param destinationAddress
	 * @return
	 * @throws NoSuchRouteException thrown if no table information is known about the destination
	 * @throws RouteNotValidException thrown if a route were found, but is marked as invalid
	 */
	protected int getHopCount(int destinationAddress) throws NoSuchRouteException, RouteNotValidException {
		return ((ForwardRouteEntry) forwardRouteTable.getForwardRouteEntry(destinationAddress)).getHopCount();
	}

	/**
	 * resets the time left to live of the RREQ entry
	 * 
	 * @param sourceAddress
	 * @param brodcastID
	 * @throws NoSuchRouteException thrown if no table information is known about the destination
	 */
	protected void setRouteRequestTimer(int sourceAddress, int broadcastID) throws NoSuchRouteException {
		routeRequestTable.setRouteRequestTimer(sourceAddress, broadcastID);
		//wake the timer thread since a RREQ should be monitored
		synchronized (tableLocks) {
			tableLocks.notify();
		}
	}

	/**
	 * This timer is responsible for cleaning up the RREQTables and ForwardRouteTable. 
	 * RREQTable is only cleaned up by Timer here, not by RREP 
	 * @author Jay
	 */
	/*private class TimeoutNotifier extends Thread {
		public TimeoutNotifier() {
			super("TimeoutNotifier");
		}
		
		private RouteRequestEntry route;
		private ForwardRouteEntry froute;
		private long time;
		public void run() {
			while (keepRunning) {
				try {
					synchronized (tableLocks) {
						while (routeRequestTable.isEmpty() && forwardRouteTable.isEmpty()) {
							tableLocks.wait();
						}
					}
					time = getMinimumTime();
					if(time > 0)
						sleep(time);
										
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
	
				// Route Request clean up
				RouteEntry routeEnt = routeRequestTable.getNextRouteToExpire();
				if(routeEnt != null){
					route = (RouteRequestEntry)routeEnt;
					while (route != null && route.getFinalAliveTime() <= System.currentTimeMillis()) {
						routeRequestTable.removeEntry(route.getSourceAddress(), route.getBroadcastID());
						//Debug.print(route.toString());
						if (route.getSourceAddress() == nodeAddress) {
							if (!validForwardRouteExists(route.getDestinationAddress(), route.getDestinationSequenceNumber())) {
								if (route.resend()) {
									//create a new RREQ message to broadcast
									RREQ newReq = new RREQ(nodeAddress,	route.getDestinationAddress(),
															parent.getCurrentSequenceNumber(),
															route.getDestinationSequenceNumber(),
															parent.getNextBroadcastID());
									//update the RREQ entry
									route.setBroadcastID(newReq.getBroadcastId());
									//reinsert the entry with no timer
									routeRequestTable.addRouteRequestEntry(route, false);
									//let the sender broadcast the RREQ
									parent.queuePDUmessage(newReq);
								} else {
									// all RREQ retires is used. Notify the application layer
									parent.queuePDUmessage(new InternalMessage(Constants.RREQ_FAILURE_PDU, route.getDestinationAddress()));
									parent.notifyAboutRouteEstablishmentFailure(route.getDestinationAddress());
								}
							}
						}
						route = (RouteRequestEntry) routeRequestTable.getNextRouteToExpire();
					}
				}

				// Forward Route Cleanup
				routeEnt = forwardRouteTable.getNextRouteToExpire();
				if(routeEnt != null){
					froute = (ForwardRouteEntry)routeEnt;
					while (froute != null && froute.getFinalAliveTime() <= System.currentTimeMillis()) {
						
						//is froute a neighbour?
						if (froute.getHopCount() == 1 && froute.isValid()) {
							try {
								setInvalid(froute.getDestinationAddress(), froute.getDestinationSequenceNumber());
							} catch (NoSuchRouteException e) {
								e.printStackTrace();
							}
							parent.notifyAboutRouteToDestIsInvalid(froute.getDestinationAddress());
							
							// Remove all messages that needed to be sent to this broken destination
							for(RERR rerr : forwardRouteTable.findBrokenRoutes(froute.getDestinationAddress())){
								parent.queuePDUmessage(rerr);
							}
						}
						else if (froute.isValid()) {
							// Gratitude time for longer path. It takes longer to create. A Hacky fix!
							// The idea is to give longer path longer alive time
							if(froute.getFinalAliveTime() + (froute.getHopCount()-1)*Constants.ROUTE_ALIVETIME*2 <= System.currentTimeMillis()){
								try {
									forwardRouteTable.setValid(froute.getDestinationAddress(), froute.getDestinationSequenceNumber(), false);
								} catch (NoSuchRouteException e) {
									e.printStackTrace();
								}
								parent.notifyAboutRouteToDestIsInvalid(froute.getDestinationAddress());
							}
						} 
						else {
							forwardRouteTable.removeEntry(froute.getDestinationAddress());
						}
						froute = (ForwardRouteEntry) forwardRouteTable.getNextRouteToExpire();
					}
				}
			}
		}

		*//**
		 * Get the minimum time the thread can sleep. Return Constants.ROUTE_ALIVETIME if no route in the routing table
		 * @return
		 *//*
		private long getMinimumTime() {
			long a = Constants.ROUTE_ALIVETIME, b = Constants.ROUTE_ALIVETIME;	// Default waiting value
			long curTime = System.currentTimeMillis();
			RouteEntry routeEntry = routeRequestTable.getNextRouteToExpire();
			if(routeEntry != null)
				a = routeRequestTable.getNextRouteToExpire().getFinalAliveTime()-curTime;
			
			routeEntry = forwardRouteTable.getNextRouteToExpire();
			if(routeEntry != null)
				b = forwardRouteTable.getNextRouteToExpire().getFinalAliveTime()-curTime;
			
			long t = (a < b ? a : b);
			return (t > 0 ? t : 0);
		}

		public void stopThread() {
			this.interrupt();
		}
	}*/
	
	/**
	 * Get the ForwardRouteEntry of the destination. Send a RREQ if the route is not available and wait for waitTime.
	 * This is a blocking call!
	 * @param destinationAddress
	 * @param sendRREQ
	 * @return	null if the route can't be figured out within the waitTime
	 * @throws NoSuchRouteException
	 * @throws RouteNotValidException
	 */
	/*public ForwardRouteEntry getForwardRouteEntry(int destinationAddress, long waitTime)  {
		try {
			return forwardRouteTable.getForwardRouteEntry(destinationAddress);
		} 
		catch (AodvException e) {
			if(waitTime > 0){
				if(parent.sendRREQ(destinationAddress)){
					try {
						Thread.sleep(waitTime);
					} catch (InterruptedException e1) {
						return null;
					}
					return forwardRouteTable.getOneForwardRouteEntry(destinationAddress);
				}
			}
		} 
		return null;
	}*/
	
	/*
	 * Testing purpose
	 */
	public String getForwardTableSummary(){
		return forwardRouteTable.summary();
	}
	
	public String getRequestTableSummary(){
		return routeRequestTable.summary();
	}
	
	public void setBlockingIpSet(String blockingIps){
		if(MyTextUtils.isEmpty(blockingIps)){
			blockingSet.clear();
			return;
		}
		
		String[] ips = blockingIps.split(",");
		if(ips.length < 1){
			ips = new String[1];
			ips[0]=blockingIps;
		}
		blockingSet.clear();
		for(int i=0; i<ips.length; i++){
			blockingSet.add(Integer.parseInt(ips[i]));
		}
	}
	public HashSet<Integer> getBlockingIpSet(){
		return blockingSet;
	}
	
	public ArrayList<ForwardRouteEntry> getNeighbors() {
		return forwardRouteTable.getNeighbors(1, true);
	}
	
}