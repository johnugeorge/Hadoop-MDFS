package org.apache.hadoop.mdfs.protocol;

import java.util.LinkedList;
import java.io.File;

import java.util.concurrent.locks.*;

import edu.tamu.lenss.mdfs.Constants;
import edu.tamu.lenss.mdfs.MDFSDirectory;
import edu.tamu.lenss.mdfs.MDFSFileCreator;
import edu.tamu.lenss.mdfs.MDFSFileCreator.MDFSFileCreatorListener;
import edu.tamu.lenss.mdfs.MDFSFileRetriever;
import edu.tamu.lenss.mdfs.MDFSFileRetriever.FileRetrieverListener;
import edu.tamu.lenss.mdfs.comm.ServiceHelper;
import edu.tamu.lenss.mdfs.models.MDFSFileInfo;


class BlockOperation{
	public String blockToOperate;
	public String operation;

	BlockOperation(String blockToOperate,String operation){
		this.blockToOperate = blockToOperate;
		this.operation = operation;
	}
}

class ListOfBlocksOperation{
	private LinkedList<BlockOperation> blocksOperation;
	boolean valueSet =false;

	ListOfBlocksOperation(){
		blocksOperation = new LinkedList<BlockOperation>();
	}

	synchronized void addElem(BlockOperation blockOps){
		System.out.println("Adding Elem"+blockOps.blockToOperate + " Operation "+ blockOps.operation);
		blocksOperation.add(blockOps);
		System.out.println("Size "+blocksOperation.size());
		notify();

	}

	synchronized BlockOperation  getElem(){
		BlockOperation blockOps =blocksOperation.poll();
		try{
			if(blockOps  == null){
				wait();
				blockOps = blocksOperation.poll();
			}
			System.out.println("Removing Elem"+blockOps.blockToOperate + " Operation "+ blockOps.operation);
		}  catch(InterruptedException e) {
			System.out.println("InterruptedException caught");
		}
		System.out.println("Size "+blocksOperation.size());
		return blockOps;
	}

	synchronized void addToMaxOneElemList(BlockOperation blockOps){
		if(valueSet){
			try{
				wait();
			} catch(InterruptedException e) {
				System.out.println("InterruptedException caught");
			}
		}
		blocksOperation.add(blockOps);
		valueSet=true;
		System.out.println("Adding Elem"+blockOps.blockToOperate + " Operation "+ blockOps.operation);
		notify();

	}

	synchronized BlockOperation removeFromMaxOneElemList(){
		if(!valueSet){
			try{
				wait();
			} catch(InterruptedException e) {
				System.out.println("InterruptedException caught");
			}
		}
		BlockOperation blockOps = blocksOperation.poll();
		valueSet=false;
		System.out.println("Removing Elem"+blockOps.blockToOperate + " Operation "+ blockOps.operation);
		notify();
		return blockOps;

               
	}	

}

class MDFSCommunicator implements Runnable{
	private ListOfBlocksOperation ll; 
	Thread t ;
	static Lock lock = new ReentrantLock();
	static  Condition fileComplete  = lock.newCondition(); 
	static boolean isComplete=false;
	static boolean isSuccess=true;
	static final int MAXRETRY = 5;


	MDFSCommunicator(ListOfBlocksOperation list,boolean newThread){
		ll =list;
		if(newThread){
			t= new Thread(this,"MDFS Communication Thread");
			System.out.println(" MDFS Communication Thread Started ");
			t.start();
		}
	}

	public void run() {
		System.out.println(" MDFS Communication Thread Run ");
		while(true){
			//BlockOperation blockOps =ll.getElem();
			BlockOperation blockOps =ll.removeFromMaxOneElemList();
			String fileName=blockOps.blockToOperate;
			File fileBlock= new File(fileName);
			if(blockOps.operation == "CREATE"){
				createFile(fileBlock);
			}
			else if(blockOps.operation == "RETRIEVE"){

			}
			else{
				System.out.println(" Unknown Operation "+blockOps.operation);
				continue;
			}

			lock.lock();
			try{
				if(isComplete==false){

					fileComplete.await();
				}
				else{
					System.out.println(" isComplete is already true.");
				}
			}
			catch (InterruptedException e) {

				System.out.println(" Error:Interrupted Exception ");

			}
			finally {
				lock.unlock();
			}

		}	
	}


	public boolean sendBlockOperation(BlockOperation blockOps) {
		System.out.println(" Send Block Operations to Network");
		System.out.println("Block Operation Elem "+blockOps.blockToOperate + " Operation "+ blockOps.operation);
		int maxRetry = MAXRETRY;
		isSuccess=true;
		//BlockOperation blockOps =ll.getElem();
		while(maxRetry > 0){
			String fileName=blockOps.blockToOperate;
			File fileBlock= new File(fileName);
			if(blockOps.operation == "CREATE"){
				createFile(fileBlock);
			}
			else if(blockOps.operation == "RETRIEVE"){
				retrieveFile(fileName);
			}
			else{
				System.out.println(" Unknown Operation "+blockOps.operation);
				return false;
			}

			lock.lock();
			try{
				if(isComplete==false){

					fileComplete.await();
				}
				else{
					System.out.println(" isComplete is already true.");
				}
			}
			catch (InterruptedException e) {

				System.out.println(" Error:Interrupted Exception ");

			}
			finally {
				lock.unlock();
			}
			if(isSuccess){
				maxRetry=MAXRETRY;
				break;
			}
			else{
				maxRetry--;
				System.out.println(" Earlier execution for file "+blockOps.blockToOperate + " Operation "+ blockOps.operation+" is unsuccessful");
				System.out.println(" No of retries left is "+maxRetry);
			}
		}
		if(maxRetry == 0)
			return false;
		else
			return true;
	}

	public static void createFile(File file){
		isComplete=false;
		MDFSFileCreator creator = new MDFSFileCreator(file, Constants.KEY_CODING_RATIO, Constants.FILE_CODING_RATIO, fileCreatorListener);
		creator.setDeleteFileWhenComplete(true);
		creator.start();
	}

	private static MDFSFileCreatorListener fileCreatorListener = new MDFSFileCreatorListener(){
		@Override
		public void statusUpdate(String status) {
			System.out.println("Creation status update: " + status);
		}

		@Override
		public void onError(String error) {
			System.err.println("Creation Error:     " + error);
			lock.lock();
			isSuccess =false;
			try{
				if(isComplete== false ){
					isComplete=true;
					fileComplete.signal();
				}else{
					System.out.println(" isComplete is already true.");
				}
			}
			finally {
				lock.unlock();
			}
			System.out.println("File Creation Error. ");
		}

		@Override
		public void onComplete() {
			lock.lock();
			isSuccess =true;
			try{
				if(isComplete== false ){
					isComplete=true;
					fileComplete.signal();
				}else{
					System.out.println(" isComplete is already true.");
				}
			}
			finally {
				lock.unlock();
			}
			System.out.println("File Creation Complete. ");
		}

	};


	public static void retrieveFile(String fileName){
		isComplete=false;
		System.out.println(" Retrieve file "+fileName);
		MDFSFileRetriever retriever = new MDFSFileRetriever(fileName, fileName.hashCode());
		retriever.setListener(fileRetrieverListener);
		retriever.start();
	}





	private static FileRetrieverListener fileRetrieverListener = new FileRetrieverListener(){
		@Override
		public void statusUpdate(String status) {
			System.out.println("Retrieval status update: " + status);
		}

		@Override
		public void onError(String error) {
			System.err.println("Retrieval Error:     " + error);
			lock.lock();
			isSuccess =false;
			try{
				if(isComplete== false ){
					isComplete=true;
					fileComplete.signal();
				}else{
					System.out.println(" isComplete is already true.");
				}
			}
			finally {
				lock.unlock();
			}
			System.out.println("File Retrieval Error. ");
		}

		@Override
		public void onComplete(File decryptedFile, MDFSFileInfo fileInfo) {
			lock.lock();
			isSuccess =true;
			try{
				if(isComplete== false ){
					isComplete=true;
					fileComplete.signal();
				}else{
					System.out.println(" isComplete is already true.");
				}
			}
			finally {
				lock.unlock();
			}
			System.out.println("File Retrieval Complete. ");
		}

	};



}	


