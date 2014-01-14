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

import org.apache.commons.logging.*;


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
	public static final Log LOG = LogFactory.getLog(ListOfBlocksOperation.class);


	ListOfBlocksOperation(){
		blocksOperation = new LinkedList<BlockOperation>();
	}

	synchronized void addElem(BlockOperation blockOps){
		LOG.info("Adding Elem"+blockOps.blockToOperate + " Operation "+ blockOps.operation);
		blocksOperation.add(blockOps);
		LOG.info("Size "+blocksOperation.size());
		notify();

	}

	synchronized BlockOperation  getElem(){
		BlockOperation blockOps =blocksOperation.poll();
		try{
			if(blockOps  == null){
				wait();
				blockOps = blocksOperation.poll();
			}
			LOG.info("Removing Elem"+blockOps.blockToOperate + " Operation "+ blockOps.operation);
		}  catch(InterruptedException e) {
			System.out.println("InterruptedException caught");
		}
		LOG.info("Size "+blocksOperation.size());
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
		LOG.info("Adding Elem"+blockOps.blockToOperate + " Operation "+ blockOps.operation);
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
		LOG.info("Removing Elem"+blockOps.blockToOperate + " Operation "+ blockOps.operation);
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
	static String requested_file=""; 
	public static final Log LOG = LogFactory.getLog(MDFSCommunicator.class);


	MDFSCommunicator(ListOfBlocksOperation list,boolean newThread){
		ll =list;
		if(newThread){
			t= new Thread(this,"MDFS Communication Thread");
			LOG.info(" MDFS Communication Thread Started ");
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
		LOG.info("Send block Operation to Network: Elem "+blockOps.blockToOperate + " Operation "+ blockOps.operation);
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
					LOG.error("sendBlockOperation:: isComplete is already true.");
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
				LOG.info(" Earlier execution for file "+blockOps.blockToOperate + " Operation "+ blockOps.operation+" is unsuccessful");
				LOG.info(" No of retries left is "+maxRetry);
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
			LOG.info("Creation status update: " + status);
		}

		@Override
		public void onError(String error) {
			LOG.error("Creation Error:     " + error);
			lock.lock();
			isSuccess =false;
			try{
				if(isComplete== false ){
					isComplete=true;
					fileComplete.signal();
				}else{
					LOG.error(" isComplete is already true.");
				}
			}
			finally {
				lock.unlock();
			}
		}

		@Override
		public void onComplete() {
			lock.lock();
			isSuccess =true;
			LOG.info("File Creation Complete. ");
			try{
				if(isComplete== false ){
					isComplete=true;
					fileComplete.signal();
				}else{
					LOG.error(" isComplete is already true.");
				}
			}
			finally {
				lock.unlock();
			}
		}

	};


	public static void retrieveFile(String fileName){
		isComplete=false;
		LOG.info(" Retrieve file "+fileName);
		requested_file=fileName;
		MDFSFileRetriever retriever = new MDFSFileRetriever(fileName, fileName.hashCode());
		retriever.setListener(fileRetrieverListener);
		retriever.start();
	}





	private static FileRetrieverListener fileRetrieverListener = new FileRetrieverListener(){
		@Override
		public void statusUpdate(String status) {
			LOG.info("Retrieval status update: " + status);
		}

		@Override
		public void onError(String error) {
			LOG.error("Retrieval Error:     " + error);
			lock.lock();
			isSuccess =false;
			try{
				if(isComplete== false ){
					isComplete=true;
					fileComplete.signal();
				}else{
					LOG.error(" isComplete is already true.");
				}
			}
			finally {
				lock.unlock();
			}
		}

		@Override
		public void onComplete(File decryptedFile, MDFSFileInfo fileInfo, String fileName) {
			lock.lock();
			isSuccess =true;
			try{
				if(isComplete== false ){
					isComplete=true;
					fileComplete.signal();
					LOG.info("File Retrieval Complete. "+fileName);
				}else{
					LOG.error(" isComplete is already true.");
				}
			}
			finally {
				lock.unlock();
			}
		}

	};



}	


