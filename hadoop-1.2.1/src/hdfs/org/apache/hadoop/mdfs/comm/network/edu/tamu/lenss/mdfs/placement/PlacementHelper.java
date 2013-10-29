package edu.tamu.lenss.mdfs.placement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import adhoc.etc.MyPair;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import edu.tamu.lenss.mdfs.models.NodeInfo;

public class PlacementHelper {
	private int k1Val, k2Val, n1Val, n2Val;
	private int networkSize;
	private int samplesCount = 250;
	private boolean[][] adjMatrix;
	private HashSet<NodeInfo> nodes;
	// bidirectional map from original node id to the virtual node id
	// <Virtual Index, Real IP Id>
	private HashBiMap<Integer, Integer> map;	
	
	// Store the final result
	private List<Integer> keyStorages, fileStorages;
	private HashMap<Integer, List<Integer>> keySourceLocations, fileSourceLocations;
	
	// Computatioin time spent
	private double mc_start, mc_stop, ilp_start, ilp_stop;
	
	
	
	public PlacementHelper(HashSet<NodeInfo> info, int n1, int k1, int n2, int k2){
		this.nodes = info;
		this.n1Val = n1;
		this.k1Val = k1;
		this.n2Val = n2;
		this.k2Val = k2;
		this.networkSize = nodes.size();
		buildMap();
	}
	
	private void buildMap(){
		map = HashBiMap.create(networkSize);
		adjMatrix = new boolean[networkSize][networkSize];
		Iterator<NodeInfo> iter = nodes.iterator();
		int idx = 0;
		NodeInfo node;
		while(iter.hasNext()){
			node = iter.next();
			if(!map.containsValue(node.getSource())){	// Sometimes a node reply twice...why?
				map.put(idx, node.getSource());
				idx++;
			}
		}
		
		// Build the adjacent matrix
		BiMap<Integer, Integer> inverseMap = map.inverse();
		iter = nodes.iterator();
		Integer nodeNum, neighNum;
		while(iter.hasNext()){
			node = iter.next();
			List<Integer> neighbors = node.getNeighborsList();
			nodeNum = inverseMap.get(node.getSource());
			if( nodeNum == null)
				continue;
			for(Integer n : neighbors){
				neighNum = inverseMap.get(n);
				// Some nodes may still have the previous neighbors that is no longer existing. Need to take care of
				if( neighNum == null)
					continue;
				
				if(nodeNum >= networkSize
						|| neighNum >= networkSize){
					continue;	// Crapy way to avoid index out of bound
				}
				adjMatrix[nodeNum][neighNum]=true;
				adjMatrix[neighNum][nodeNum]=true;
			}
		}
	}
	
	/**
	 * Blocking call
	 */
	public void findOptimalLocations(){
		findFileLocations();
		findKeyLocations();	//Temporary disabled.
	}
	
	/**
	 * Blocking call
	 */
	public void findKeyLocations(){
		MyPair<HashMap<Integer, List<Integer>>, ArrayList<Integer>> pair = searchPlacement(n1Val, k1Val);
		
		// Inverse the index mapping. Covert the virtual index back to the real IP index
		keySourceLocations = new HashMap<Integer, List<Integer>>();
		Iterator<Entry<Integer, List<Integer>>> iter = pair.first.entrySet().iterator();
		while(iter.hasNext()){
			Entry<Integer, List<Integer>> entry = iter.next();
			List<Integer> neighbors = entry.getValue();
			for(int i=0; i<neighbors.size(); i++){
				neighbors.set(i, map.get(neighbors.get(i)));
			}
			keySourceLocations.put(entry.getKey(), neighbors);
		}
		
		keyStorages = pair.second;
		for(int i=0; i<keyStorages.size(); i++){
			keyStorages.set(i, map.get(keyStorages.get(i)));
		}
	}
	
	/**
	 * Blocking call
	 */
	public void findFileLocations(){
		MyPair<HashMap<Integer, List<Integer>>, ArrayList<Integer>> pair = searchPlacement(n2Val, k2Val);

		// Inverse the index mapping. Covert the virtual index back to the real IP index
		fileSourceLocations = new HashMap<Integer, List<Integer>>();
		Iterator<Entry<Integer, List<Integer>>> iter = pair.first.entrySet().iterator();
		while(iter.hasNext()){
			Entry<Integer, List<Integer>> entry = iter.next();
			List<Integer> neighbors = entry.getValue();
			for(int i=0; i<neighbors.size(); i++){
				neighbors.set(i, map.get(neighbors.get(i)));
			}
			fileSourceLocations.put(entry.getKey(), neighbors);
		}
		
		fileStorages = pair.second;
		for(int i=0; i<fileStorages.size(); i++){
			fileStorages.set(i, map.get(fileStorages.get(i)));
		}
	}
	
	private MCSimulation sim;
	private MyPair<HashMap<Integer, List<Integer>> ,ArrayList<Integer>> searchPlacement(int n, int k){
		//double[] failProb = {0.2,0.7,0.5,0.3,0.1};
		double[] failProb = new double[networkSize];
		BiMap<Integer, Integer> inverseMap = map.inverse();
		for(NodeInfo info:nodes){
			failProb[inverseMap.get(info.getSource())] = info.getFailureProbability();
		}
		
		if( sim == null){
			samplesCount = 20*networkSize;	// Temporary	
			sim = new MCSimulation(adjMatrix, failProb, samplesCount);
			mc_start=System.currentTimeMillis();
			sim.simulate();
			mc_stop = System.currentTimeMillis();
		}
		
		FacilityLocation location = new FacilityLocation(sim.getExpDistMatrix(),n,k );
		ilp_start=System.currentTimeMillis();
		location.solve();
		ilp_stop=System.currentTimeMillis();
		location.getStorageNodes();
		location.getFragemntSources();
		return new MyPair<HashMap<Integer, List<Integer>>, ArrayList<Integer>>(location.getFragemntSources(),location.getStorageNodes());
	}

	public int getSamplesCount() {
		return samplesCount;
	}

	public void setSamplesCount(int samplesCount) {
		this.samplesCount = samplesCount;
		this.sim = null; // Reset the MCSimulation object
	}

	public List<Integer> getKeyStorages() {
		return keyStorages;
	}

	public List<Integer> getFileStorages() {
		return fileStorages;
	}

	public HashMap<Integer, List<Integer>> getKeySourceLocations() {
		return keySourceLocations;
	}

	public HashMap<Integer, List<Integer>> getFileSourceLocations() {
		return fileSourceLocations;
	}
	
	public double getMCSimTime(){
		return (mc_stop-mc_start);
	}
	
	public double getILPTime(){
		return (ilp_stop-ilp_start);
	}
	
}
