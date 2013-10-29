package edu.tamu.lenss.mdfs.models;

import java.util.ArrayList;
import java.util.List;

import adhoc.aodv.pdu.AODVDataContainer;

public class NodeInfo extends AODVDataContainer {
	private static final long serialVersionUID = 1L;
	private int batteryLevel, rank;
	private int timeSinceInaccessible;
	private float failureProbability;
	private List<Integer> neighborsList = new ArrayList<Integer>();
	
	public NodeInfo(){
		super(MDFSPacketType.NODE_INFO);
		this.setBroadcast(false);
	}
	
	public NodeInfo(int source, int destination){
		super(MDFSPacketType.NODE_INFO, source, destination);
		this.setBroadcast(false);
	}
	
	public NodeInfo(int source, int destination, int battery, int ran, int timeSinceInaccessible){
		super(MDFSPacketType.NODE_INFO, source, destination);
		this.batteryLevel = battery;
		this.rank = ran;
		this.timeSinceInaccessible = timeSinceInaccessible;
		this.setBroadcast(false);
	}
	
	
	public int getBatteryLevel() {
		return batteryLevel;
	}

	public void setBatteryLevel(int batteryLevel) {
		this.batteryLevel = batteryLevel;
	}

	public int getRank() {
		return rank;
	}

	public void setRank(int rank) {
		this.rank = rank;
	}

	public long getTimeSinceInaccessible() {
		return timeSinceInaccessible;
	}

	public void setTimeSinceInaccessible(int timeSinceInaccessible) {
		this.timeSinceInaccessible = timeSinceInaccessible;
	}

	public List<Integer> getNeighborsList() {
		return neighborsList;
	}

	public void setNeighborsList(ArrayList<Integer> neighborsList) {
		this.neighborsList = neighborsList;
	}
	
	public void addNeighbor(int neighbor){
		this.neighborsList.add(neighbor);
	}
	
	

	public double getFailureProbability() {
		// Need to be removed
		return failureProbability;
		//return Math.random();
	}

	public void setFailureProbability(float failureProbability) {
		this.failureProbability = failureProbability;
	}
	
	@Override
	public byte[] toByteArray() {
		return super.toByteArray(this);
	}

}
