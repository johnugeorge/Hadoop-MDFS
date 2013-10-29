package edu.tamu.lenss.mdfs.placement;

import java.text.DecimalFormat;
import java.util.Random;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.uncommons.maths.random.BinomialGenerator;

import adhoc.etc.Logger;


public class MCSimulation {
	private static final String TAG = MCSimulation.class.getSimpleName();
	private boolean[][] adjacencyMatrix;
	private double[][] expDistMatrix;
	private double[] failureProb;	// Failure probability
	private int sampleNum;
	
	
	
	public MCSimulation(){
	}
	
	public MCSimulation(boolean[][] adjMat, double[] failProb){
		this(adjMat, failProb, 0);
	}
	
	public MCSimulation(boolean[][] adjMat, double[] failProb, int samples){
		this.adjacencyMatrix = adjMat;
		this.failureProb = failProb;
		this.sampleNum = samples;
	}
	
	/**
	 * Blocking call. Run monte carolo simulation. This takes a while...
	 */
	public void simulate(){
		BinomialGenerator generator;
		int nodeCount = failureProb.length;
		if(sampleNum == 0){
			//sampleNum = (int)(0.5*Math.pow(2, nodeCount)); 
			sampleNum = 200;
		}
		boolean[][][] adjMatrixSamples = new boolean[sampleNum][nodeCount][nodeCount];
		boolean[][] nodeOfSample = new boolean[sampleNum][nodeCount];	// Whether the node exist or not of each sample
		for(int i=0; i < sampleNum; i++){
			adjMatrixSamples[i]=arrayCopy(adjacencyMatrix);
		}
		
		Random rand = new Random(System.currentTimeMillis());
		double checkPoint;		// In case BinomialGenerator(1, checkPoint, rand) fails
		for(int n=0; n<nodeCount; n++){	// Loop through all nodes
			checkPoint = 1.0-failureProb[n];
			if(checkPoint <= 0 || checkPoint >= 1){
				if(Math.abs(checkPoint-0) < Math.abs(checkPoint-1))
					checkPoint = 0.01;
				else
					checkPoint = 0.99;
			}
			generator = new BinomialGenerator(1, checkPoint, rand);	
			for(int s=0; s<sampleNum; s++){	// Loop through all sample graph
				if(generator.nextValue()==0){
					// Make the row and column all zeros
					for(int j=0; j<nodeCount; j++){
						adjMatrixSamples[s][n][j]=false;
						adjMatrixSamples[s][j][n]=false;
					}
					nodeOfSample[s][n]=false;
				}
				else
					nodeOfSample[s][n]=true;
			}
		}
		
		// find expected all pair shortest distance
		RealMatrix sumDist = new Array2DRowRealMatrix(nodeCount,nodeCount);
		BFS bfs = new BFS(adjacencyMatrix);
		int[][] sampleDistMat;
		for(int s=0; s<sampleNum; s++){
			bfs.setAdjMatrix(adjMatrixSamples[s]);
			sampleDistMat = bfs.allPairBFS();
			// If the node is removed, the distance to itself becomes infinity (size+1)
			for(int c=0; c<nodeCount; c++){
				if(!nodeOfSample[s][c]){
					sampleDistMat[c][c]=nodeCount+1;
				}
			}
			sumDist.setSubMatrix(sumDist.add(new Array2DRowRealMatrix(toDoubleArray(sampleDistMat))).getData(), 
					0, 0);
			//bfs.allPairBFS();
		}
		sumDist=sumDist.scalarMultiply((double)1/sampleNum);
		expDistMatrix = sumDist.getData();
		Logger.v(TAG, "Finish MC Simulation");
	}
	
	/**
	 * Covert an integer array to a double array
	 * @param array
	 * @return
	 */
	public static double[][] toDoubleArray(int[][] array){
		int rowLen = array.length;
		int colLen = array[0].length;
		double[][] dbArray = new double[rowLen][colLen];
		for(int i=0; i<rowLen; i++){
			for(int j=0; j<colLen; j++){
				dbArray[i][j] = (double)array[i][j];
			}
		}
		return dbArray;
	}
	/**
	 * Copy a 2D boolean array
	 * @param array
	 * @return
	 */
	public static boolean[][] arrayCopy(boolean[][] array){
		int rowLen = array.length;
		int colLen = array[0].length;
		boolean[][] copy = new boolean[rowLen][colLen];
		for(int i=0; i<rowLen; i++) {
			System.arraycopy(array[i], 0, copy[i], 0, colLen);
		}
		return copy;
	}

	public int getSampleNum() {
		return sampleNum;
	}

	public void setSampleNum(int sampleNum) {
		this.sampleNum = sampleNum;
	}

	public boolean[][] getAdjacencyMatrix() {
		return adjacencyMatrix;
	}

	public double[][] getExpDistMatrix() {
		return expDistMatrix;
	}

	public double[] getFailureProb() {
		return failureProb;
	}
	
	public String distanceMatrixString(){
		StringBuilder builder = new StringBuilder("");
		int N = expDistMatrix.length;
		DecimalFormat df = new DecimalFormat("#.##");
		for(int i=0; i<N; i++){
			for(int j=0; j<N; j++){
				builder.append(df.format(expDistMatrix[i][j])+ " \t");
			}
			builder.append("\n");
		}
		return builder.toString();
	}
	
}
