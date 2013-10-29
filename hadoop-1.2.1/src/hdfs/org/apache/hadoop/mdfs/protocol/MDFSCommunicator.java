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



class ListOfBlocksToDistribute{
	private LinkedList<String> blocksToDistribute;

	ListOfBlocksToDistribute(){
		blocksToDistribute = new LinkedList<String>();
	}

	synchronized void addElem(String str){
		System.out.println("Adding Elem"+str);
		blocksToDistribute.add(str);
		notify();

	}

	synchronized String getElem(){
		String str=blocksToDistribute.poll();
		try{
			if(str == null){
				wait();
				str = blocksToDistribute.poll();
			}
			System.out.println("Removing Elem"+str);
		}  catch(InterruptedException e) {
			System.out.println("InterruptedException caught");
		}
		return str;
	}

}

class MDFSCommunicator implements Runnable{
	private ListOfBlocksToDistribute ll; 
	Thread t ;
	static Lock lock = new ReentrantLock();
	static  Condition fileComplete  = lock.newCondition(); 
	static boolean isComplete=false;


	MDFSCommunicator(ListOfBlocksToDistribute list){
		ll =list;
		t= new Thread(this,"MDFS Communication Thread");
		System.out.println(" MDFS Communication Thread Started ");
		t.start();
	}

	public void run() {
		System.out.println(" MDFS Communication Thread Run ");
		while(true){
			String fileName =ll.getElem();
			File fileBlock= new File(fileName);
			createFile(fileBlock);
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
		}

		@Override
		public void onComplete() {
			lock.lock();
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








}	


