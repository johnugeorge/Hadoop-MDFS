package edu.tamu.lenss.mdfs.comm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import adhoc.aodv.Node;
import adhoc.etc.IOUtilities;
import edu.tamu.lenss.mdfs.Constants;
import edu.tamu.lenss.mdfs.models.NodeInfo;
import edu.tamu.lenss.mdfs.models.TopologyDiscovery;
import edu.tamu.lenss.mdfs.utils.JCountDownTimer;


/**
 * This class handle send topologyDisocvery request and wait for the responses.
 * @author Jay
 */
public class TopologyHandler {
	private volatile boolean waitingReply = false;
	//private List<NodeInfo> nodeInfo = new ArrayList<NodeInfo>();
	private Map<Integer, NodeInfo> nodeInfoMap = new HashMap<Integer, NodeInfo>();
	private Map<Integer, NodeInfo> cachedMap = new HashMap<Integer, NodeInfo>();
	private JCountDownTimer timer;
	private long last_discover_time;
	//private int retries = Constants.TOPOLOGY_DISCOVERY_MAX_RETRIES;
	private LinkedBlockingQueue<TopologyListener> listenerQueue = new LinkedBlockingQueue<TopologyListener>();
	
	public TopologyHandler(TopologyListener lis){
		this();
		listenerQueue.add(lis);
	}
	
	public TopologyHandler(){
		this.timer = new JCountDownTimer(Constants.TOPOLOGY_DISCOVERY_TIMEOUT, Constants.TOPOLOGY_DISCOVERY_TIMEOUT) {
		     public void onTick(long millisUntilFinished) {
		     }

		     public void onFinish() {
		    	 if(waitingReply){
		    		 //if(nodeInfo.isEmpty())
		    		 if(nodeInfoMap.isEmpty())
		    			 onErrorCallBack("Timeout");
		    		 else{
		    			 // Need to be removed
		    			 NodeInfo curNode = new NodeInfo(Node.getInstance().getNodeId(), Node.getInstance().getNodeId(), 
		 						60, 1, 100000);
		 				 /*nodeInfo.add(curNode);	// Add my information
		    			 onCompleteCallBack(nodeInfo);*/
		    			 nodeInfoMap.put(curNode.getSource(), curNode); // Add my information
		    			 onCompleteCallBack(nodeInfoMap);
		    			 last_discover_time = System.currentTimeMillis();
		    		 }
		    	 }
		    	 waitingReply = false;
		     }
		  };
	}
	
	public void broadcastRequest(TopologyListener lis){
			
		listenerQueue.add(lis);
		// Use the cache data
		if(!cachedMap.isEmpty() && ( System.currentTimeMillis()-last_discover_time < Constants.TOPOLOGY_CACHE_EXPIRY_TIME)){
			System.out.println(" Using Cached Map for topology discovery of nodes size "+ cachedMap.size());
			onCompleteCallBack(cachedMap);
			return;
		}

		if(System.currentTimeMillis()-last_discover_time < Constants.TOPOLOGY_REBROADCAST_THRESHOLD){
			//onCompleteCallBack(nodeInfo);
			onCompleteCallBack(nodeInfoMap);
			return;
		}
		TopologyDiscovery top = new TopologyDiscovery("Broadcast Message from " + IOUtilities.getLocalIpAddress());
		ServiceHelper.getInstance().getMyNode().sendAODVDataContainer(top);
		waitingReply = true;
		//nodeInfo.clear();
		nodeInfoMap.clear();
		timer.start();
	}
	
	protected void receiveNewPacket(NodeInfo info){		
		//nodeInfo.add(info);
		nodeInfoMap.put(info.getSource(),info);
		timer.cancel();	// Reset the timer
		timer.start();
	}
	
	
	private void onErrorCallBack(String msg){
		/*if(retries >0){
			System.out.println(" Number of retries left for topology discovery "+retries);
			retries--;
			broadcastRequest(listenerQueue.poll());
		}
		else{
			retries= Constants.TOPOLOGY_DISCOVERY_MAX_RETRIES;//reset retries
		}*/
		timer.cancel();
		while(!listenerQueue.isEmpty()){
			listenerQueue.poll().onError(msg);
		}
	}
	
	//private void onCompleteCallBack(List<NodeInfo> topList){
	private void onCompleteCallBack(Map<Integer, NodeInfo> topMap){
		/*while(!listenerQueue.isEmpty()){
			listenerQueue.poll().onComplete(topList);
		}*/
		//nodeInfo.clear();
		//retries= Constants.TOPOLOGY_DISCOVERY_MAX_RETRIES;//reset retries
		cachedMap.putAll(topMap);
		timer.cancel();
		List<NodeInfo> topList = new ArrayList<NodeInfo>(topMap.values());
		while(!listenerQueue.isEmpty()){
			listenerQueue.poll().onComplete(topList);
		}
		nodeInfoMap.clear();
	}
	
	public interface TopologyListener{
		public void onError(String msg);
		public void onComplete(List<NodeInfo> topList);
	}
}
