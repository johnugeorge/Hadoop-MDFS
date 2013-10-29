package edu.tamu.lenss.mdfs.models;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import adhoc.aodv.pdu.AODVDataContainer;
import adhoc.etc.IOUtilities;

public class TaskSchedule extends AODVDataContainer {
	private static final long serialVersionUID = 1L;
	private Map<Integer, List<MDFSFileInfo>> schedule = new HashMap<Integer, List<MDFSFileInfo>>();
	//private SparseArray<List<MDFSFileInfo>> schedule = new SparseArray<List<MDFSFileInfo>>(); 
	
	public TaskSchedule(){
		super(MDFSPacketType.JOB_SCHEDULE, IOUtilities.parseNodeNumber(IOUtilities.getLocalIpAddress()), 255);
		this.setBroadcast(true);
		this.setMaxHop(5);
	}
	
	public void insertTask(int nodeId, MDFSFileInfo file){
		if(schedule.containsKey(nodeId)){
			schedule.get(nodeId).add(file);
		}
		else{
			List<MDFSFileInfo> list = new ArrayList<MDFSFileInfo>();
			list.add(file);
			schedule.put(nodeId, list);
		}
	}
	
	public List<MDFSFileInfo> getTaskList(int nodeId){
		if(schedule.containsKey(nodeId)) {
			return schedule.get(nodeId);
		}
		return null;
	}
	
	public Map<Integer, List<MDFSFileInfo>> getScheduleMap(){
		return schedule;
	}
	
	@Override
	public byte[] toByteArray() {
		return super.toByteArray(this);
	}

}
