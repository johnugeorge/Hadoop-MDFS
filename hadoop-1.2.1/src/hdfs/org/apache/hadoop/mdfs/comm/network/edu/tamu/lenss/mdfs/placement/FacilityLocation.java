package edu.tamu.lenss.mdfs.placement;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.sat4j.pb.OptToPBSATAdapter;
import org.sat4j.pb.PseudoOptDecorator;
import org.sat4j.pb.SolverFactory;
import org.sat4j.pb.tools.DependencyHelper;
import org.sat4j.pb.tools.WeightedObject;
import org.sat4j.specs.ContradictionException;
import org.sat4j.specs.IVec;
import org.sat4j.specs.TimeoutException;

import adhoc.etc.Logger;
import edu.tamu.lenss.mdfs.placement.LPItem.VarType;

public class FacilityLocation {
	private static final String TAG = FacilityLocation.class.getSimpleName();
	private OptToPBSATAdapter optimizer;
	private DependencyHelper<LPItem, String> helper;
	private static final int DEFAULT_TIMEOUT = 30;
	
	private int N; // number of nodes
	private int nVal;
	private int kVal;
	private double[][] distMatrix;
	
	List<LPItem> lpItems = new ArrayList<LPItem>();
	
	public FacilityLocation(double[][] distanceMatrix, int nVal, int kVal){
		optimizer = new OptToPBSATAdapter(new PseudoOptDecorator(SolverFactory.newCuttingPlanes()));
		optimizer.setTimeout(DEFAULT_TIMEOUT);
		this.distMatrix = distanceMatrix;
		this.nVal = nVal;
		this.kVal = kVal;
		this.N = distanceMatrix.length;
		
		helper = new DependencyHelper<LPItem, String>(optimizer, false);
	}
	
	private ArrayList<Integer> locations = new ArrayList<Integer>();	// Store the storage nodes
	private HashMap<Integer, List<Integer>> fragmentSource = new HashMap<Integer, List<Integer>> (); // Store potential sources of each node 
	public List<LPItem> solve() {
		buildItems();
		buildMatrix();
		
		// Set Objective Function
		@SuppressWarnings("unchecked")
		WeightedObject<LPItem>[] objects = new WeightedObject[N*N+N];	
		int idx = 0;
		for(LPItem item:lpItems){
			objects[idx] = WeightedObject.newWO(item, matrixObj[idx]);
			idx++;
		}
		helper.setObjectiveFunction(objects);
		
		// Set Constraints Function
		for(int row=0; row<N*N+N+2; row++){	// Loop through each row
			@SuppressWarnings("unchecked")
			WeightedObject<LPItem>[] constraint = new WeightedObject[N*N+N];
			idx = 0;
			for(LPItem item:lpItems){		// loop through each item(column)
				constraint[idx] = WeightedObject.newWO(item, matrixA[row][idx]);
				idx++;
			}
			try {
				if(row <= N*N+N)
					helper.atLeast(Integer.toString(row), BigInteger.valueOf(matrixB[row]), constraint);
				else
					helper.atMost(Integer.toString(row), BigInteger.valueOf(matrixB[row]), constraint);
			} catch (ContradictionException e) {
				Logger.e(TAG, e.toString());
				e.printStackTrace();
				return null;
			}
		}
		
		/*
		 * Solve the LP problem.
		 * Return the solution to List<LPItem> result.
		 * Store the fragment locations and potential source of each nodes to variables
		 * 
		 * */
		List<LPItem> result=null;
		try {
			if(helper.hasASolution()){
				result = new ArrayList<LPItem>();
				IVec<LPItem> sol = helper.getSolution();
				
				for(Iterator<LPItem> it = sol.iterator(); it.hasNext();){
					LPItem tmp = it.next();
					result.add(tmp);
					if(tmp.getType()==VarType.storage){
						if(fragmentSource.containsKey(tmp.getNodeIndex())){
							fragmentSource.get(tmp.getNodeIndex()).add(tmp.getStorageIndex());
						}
						else{
							fragmentSource.put(tmp.getNodeIndex(), new ArrayList<Integer>());
							fragmentSource.get(tmp.getNodeIndex()).add(tmp.getStorageIndex());
						}
						//Logger.v(TAG, "Node " + tmp.getNodeIndex() + ", Storage " + tmp.getStorageIndex());
					}
					else if(tmp.getType()==VarType.node){
						
						locations.add(tmp.getStorageIndex());
						//Logger.v(TAG, "Storage Node: " + tmp.getStorageIndex());
					}
				}
			}
			else{
				Logger.w(TAG, "No Solution!");
			}
				
		} catch (TimeoutException e) {
			Logger.e(TAG, e.toString());
			return null;
		}
		return result;
	}
	
	/**
	 * The value is available after {@link solve} is complete
	 * @return
	 */
	public ArrayList<Integer> getStorageNodes(){
		return locations;
	}
	
	/**
	 * The value is available after {@link solve} is complete
	 * @return
	 */
	public HashMap<Integer, List<Integer>> getFragemntSources(){
		return fragmentSource;
	}
	
	
	/**
	 * Build the variable X matrix. Totally N*N+N variables. <Br>
	 * The first N*N variables are "storages-Location" variables, the last
	 * N variables are "Node-Location" variables. 
	 */
	private void buildItems(){
		for(int i=0; i<N+1;i++){
			for(int j=0; j<N; j++){
				LPItem item = new LPItem(VarType.storage);
				if(i<N)
					item = new LPItem(VarType.storage);
				else
					item = new LPItem(VarType.node);
				item.setNodeIndex(i);
				item.setStorageIndex(j);
				lpItems.add(item);
			}
		}
	}
	
	private int[][] matrixA;	
	private int[] matrixB;
	private int[] matrixObj;
	/**
	 * Solve LP Problem:
	 * minimize fX
	 * subject  AX <= B
	 * 
	 * A has N*N+N+2 rows and N*N+N columns <br>
	 * The first N rows is the constraint that each node has at least k storage nodes <br>
	 * The next N^2 rows is the constraint that only nodes chosen as storage node can serve as fragment source of others <br>
	 * The next 2 rows is the constraint that there are total n storage nodes. <br>
	 * Because there is no equality constraint, we use both >= and <= constraints to achieve a equality constraint 
	 */
	private void buildMatrix(){
		matrixA = new int[N*N+N+2][N*N+N];	
		matrixB = new int[N*N+N+2];
		matrixObj = new int[N*N+N];
		// Loop through every rows of Matrix A
		for(int row=0; row < N*N+N+2; row++){
			for(int col = 0; col < N*N+N; col++){
				// constraint 3
				if(row < N){
					if(col >= row*N && col < row*N+N )
						matrixA[row][col] = 1;
				}
				// constraint 4
				else if(row < N*N+N){
					int idx1 = row-N;
					int idx2 = idx1%N+N*N;
					matrixA[row][idx1]=-1;
					matrixA[row][idx2]=1;
				}
				// constraint 2. equality constraint
				else if(row < N*N+N+2){
					if(col >= N*N)
						matrixA[row][col]=1;
				}
			}
		}
		
		/*
		 * Build Matrix B. Loop through each element
		 */
		for(int row = 0; row < N*N+N+2; row++ ){
			if(row < N)
				matrixB[row]=kVal;
			else if(row < N*N+N)
				matrixB[row]=0;
			else if (row < N*N+N+2)
				matrixB[row]=nVal;
		}
		
		// Objective function. 1-d distance matrix
		for(int row=0; row<N; row++){
			for(int col=0; col<N; col++ ){
				/*
				 * Because the API can only take integer input, we scale up the double value and convert to an integer
				 * The objective value will also be be scaled up, but the placement result won't change
				 */
				matrixObj[row*N+col]= (int)Math.round(10000*distMatrix[row][col]);
			}
		}
		for(int i=N*N; i<N*N+N; i++){
			matrixObj[i]=0;
		}
	}
	
	public void setSolverTimout(int timeInSecs){
		optimizer.setTimeout(timeInSecs);
	}
	
	public int getnVal() {
		return nVal;
	}

	public void setnVal(int nVal) {
		this.nVal = nVal;
	}

	public int getkVal() {
		return kVal;
	}

	public void setkVal(int kVal) {
		this.kVal = kVal;
	}

	public double[][] getDistMatrix() {
		return distMatrix;
	}
	
	
}
