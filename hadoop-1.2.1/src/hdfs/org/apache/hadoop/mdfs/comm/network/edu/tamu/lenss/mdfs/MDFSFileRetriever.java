package edu.tamu.lenss.mdfs;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import adhoc.aodv.Node;
import adhoc.aodv.RouteTableManager;
import adhoc.aodv.exception.NoSuchRouteException;
import adhoc.aodv.exception.RouteNotValidException;
import adhoc.etc.IOUtilities;
import adhoc.etc.Logger;
import adhoc.etc.MyPair;
import adhoc.etc.MyTextUtils;
import adhoc.tcp.TCPConnection;
import adhoc.tcp.TCPSend;
import edu.tamu.lenss.mdfs.comm.FileReplyHandler.FileREPListener;
import edu.tamu.lenss.mdfs.comm.ServiceHelper;
import edu.tamu.lenss.mdfs.crypto.FragmentInfo;
import edu.tamu.lenss.mdfs.crypto.FragmentInfo.KeyShareInfo;
import edu.tamu.lenss.mdfs.crypto.MDFSDecoder;
import edu.tamu.lenss.mdfs.models.FileFragPacket;
import edu.tamu.lenss.mdfs.models.FileREP;
import edu.tamu.lenss.mdfs.models.FileREQ;
import edu.tamu.lenss.mdfs.models.MDFSFileInfo;
import edu.tamu.lenss.mdfs.utils.AndroidIOUtils;
import edu.tamu.lenss.mdfs.utils.JCountDownTimer;

import org.apache.hadoop.mdfs.protocol.MDFSDirectoryProtocol;


public class MDFSFileRetriever {
	private static final String TAG = MDFSFileRetriever.class.getSimpleName();
	private String fileName;
	private long fileId;	// User the file create time currently
	private ServiceHelper serviceHelper;
	private List<KeyShareInfo> keyShares = new ArrayList<KeyShareInfo>();
	private Map<Integer, List<Integer>> fileFrags = new HashMap<Integer, List<Integer>>();
	private MDFSFileInfo fileInfo;
	private volatile boolean decoding;	// Has the decoding procedure started?
	//private boolean recoverable;
	private boolean keyOnly;	// Indicate that we only need keys
	private ExecutorService pool;
	private TCPConnection tcpConnection;
	private RouteTableManager manager;
	private final FileRetrieveLog fileRetLog = new FileRetrieveLog();
	
	public MDFSFileRetriever(String fileName, long fileId){
		serviceHelper = ServiceHelper.getInstance();
		this.fileName = fileName;
		this.fileId = fileId;
		this.pool = Executors.newCachedThreadPool(); 
		this.tcpConnection = TCPConnection.getInstance();
		this.manager = serviceHelper.getMyNode().getRouteManager();
		if(fileId > 0)
			this.fileInfo = serviceHelper.getDirectory().getFileInfo(fileId);
		else if(!MyTextUtils.isEmpty(fileName))
			this.fileInfo = serviceHelper.getDirectory().getFileInfo(fileName);
		else{
			Logger.e(TAG, "No such file exists in the directory");
			listener.onError("File Does not exist in the directory");
		}
	}
	
	public void start(){
		// Check if a decrypted file or encrypted file already exists on my device
		// If it is, returns it immediately.
		sendFileREQ();
		setUpTimer();
	}
	
	/**
	 * Non-blocking
	 */
	private void sendFileREQ(){
		FileREQ req = new FileREQ(fileName, fileId);
		//List<Integer> fileFrags = new ArrayList<Integer>();
		//List<Integer> keyFrags = new ArrayList<Integer>();
		/*fileFrags.add(0);fileFrags.add(1);fileFrags.add(2);fileFrags.add(3);fileFrags.add(4);fileFrags.add(5);
		keyFrags.add(1);keyFrags.add(2);keyFrags.add(3);keyFrags.add(4);keyFrags.add(5);keyFrags.add(6);
		req.setFileFragIndex(fileFrags);
		req.setKeyFragIndex(keyFrags);*/
		req.setAnyAvailable(true);
		fileRetLog.discStart = System.currentTimeMillis();
		serviceHelper.startFileRequest(req, fileRepListener);
		listener.statusUpdate("Searching for nearby fragments");
	}
	
	private int initFileFragsCnt = 0;
	private FileREPListener fileRepListener = new FileREPListener(){
		@Override
		public void onError(String msg) {
			Logger.e(TAG, msg);
			listener.onError("Can't locate any fragments in the vicinity. Please try again later");
		}

		@Override
		public void onComplete(Set<FileREP> fileREPs) {
			fileRetLog.fileReps = fileREPs;
			fileRetLog.discEnd = System.currentTimeMillis();
			
			// Cache the key fragment and retrieve the file fragments
			MDFSDirectoryProtocol directory = serviceHelper.getDirectory();
			Set<Integer> myfiles = directory.getStoredFileIndex(fileId,serviceHelper.getMyNode().getNodeId()).getItemSet();	// Get the current file fragments I have
			if(myfiles == null)
				myfiles = new HashSet<Integer>();	
			Set<Integer> uniqueFile = new HashSet<Integer>();				// add all the available fragments, including mine an others  
			
			for(FileREP rep : fileREPs){
				List<Integer> tmpList = rep.getFileFragIndex();
				if(tmpList != null){
					uniqueFile.addAll(tmpList);
					Logger.v(TAG, "Node " + rep.getSource() + " has " + tmpList.size() + " file Frags");
					tmpList.removeAll(myfiles);								// Retain only the fragments that I DO NOT have
					fileFrags.put(rep.getSource(), tmpList);				// add the file frags that I do no have into fileFrags
				}
				
				if(rep.getKeyShare() != null){
					keyShares.add(rep.getKeyShare());	// Key has no duplication
					Logger.v(TAG, "Node " + rep.getSource() + " has 1 key Frags");
				}
			}
			// Don't add my node. This causes problem when sorting and downloading
			//fileFrags.put(serviceHelper.getMyNode().getNodeId(), new ArrayList<Integer>(files));
			locFragCounter.set(myfiles.size());							// Init downloadCounter to the number of file frags I have
			initFileFragsCnt = locFragCounter.get();
			uniqueFile.addAll(myfiles);
			
			// Add My KeyShare.
			int keyIdx = directory.getStoredKeyIndex(fileId,serviceHelper.getMyNode().getNodeId());
			if(keyIdx >= 0){
				// add mine to keyShares
				String dirName = MDFSFileInfo.getDirName(fileName,fileId);
				String fName =  MDFSFileInfo.getShortFileName(fileName)+ "__key__" + keyIdx;
				File f = AndroidIOUtils.getExternalFile(Constants.DIR_ROOT + "/" + dirName + "/" + fName);
				//System.out.println(" FileName "+f.getAbsolutePath());
				KeyShareInfo key = IOUtilities.readObjectFromFile(f, KeyShareInfo.class);
				keyShares.add(key);
			}
			
			if((!keyOnly && keyShares.size() < fileInfo.getK1()) || 
					uniqueFile.size() < fileInfo.getK2()){
	//			recoverable = false;
				String s = keyShares.size() + " keys ";
				s += uniqueFile.size() + " file fragments";
				Logger.w(TAG, s + ". Insufficient fragments");
				listener.onError("Insufficient fragments. " + s + " Please try again later");
			}
			else{
				// Start to download file
				downloadFileFrags();
			}
		}
	};
	
	private AtomicInteger locFragCounter = new AtomicInteger();
	private JCountDownTimer timer;
	private void setUpTimer(){
		// Assume throughput is 800kBps
		timer = new JCountDownTimer(Constants.FRAGMENT_DISTRIBUTION_INTERVAL, Constants.FRAGMENT_DISTRIBUTION_INTERVAL){
			@Override
			public void onFinish() {
				if (locFragCounter.get() >= fileInfo.getK2() ){
					try {
						Thread.sleep(1200);		// Give a chance for the last fragment to complete download
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					Logger.v(TAG, (locFragCounter.get()-initFileFragsCnt) + " file fragments were downloaded");
					fileRetLog.retEnd = System.currentTimeMillis(); Logger.v(TAG, "RetrievalEnd: " + fileRetLog.retEnd);
					decoding = true;					
					decodeFile();
				}
				else{
					listener.onError("Fail to download file fragments. " +
							//"Only " + (fileInfo.getK2()+1-locFragCounter.get()) + " were successfully downloaded. Please try again later");
							"Only " + (locFragCounter.get()-initFileFragsCnt) + " were successfully downloaded. Please try again later");
				}
			}
			@Override
			public void onTick(long millisUntilFinished) {
			}
		};
	}
	/**
	 * Non-blocking
	 */
	private void downloadFileFrags(){
		// Create a folder for fragments
		String fDirName = MDFSFileInfo.getDirName(fileName, fileId);
		File tmp0 = AndroidIOUtils.getExternalFile(Constants.DIR_ROOT + "/" +	fDirName );
		if(!tmp0.exists()){
			if(!tmp0.mkdirs()){
			    listener.onError("File IO Error. Can't save file locally");
			    return;
			}
		}
		
		listener.statusUpdate("Downloading fragments");
		// If I have all the fragments already
		if(locFragCounter.get() >= fileInfo.getK2()){
			Logger.v(TAG," Decoding File without downloading File Fragaments "+locFragCounter.get()+" k2 val"+fileInfo.getK2());
			decodeFile();
			return;
		}
		int requiredDownloadCnt = fileInfo.getK2() + 1 - locFragCounter.get();	// Download one more fragment. Just in case...
		
		// Search for the best(closest) storages
		List<Integer> storageNodes = new ArrayList<Integer>(fileFrags.keySet());
		Collections.sort(storageNodes, new SortByHopCount());
		Set<Integer> requestedFrag = new HashSet<Integer>();	// Record the downloaded frag. Do not download duplicates.
		for(Integer sNode : storageNodes){
			List<Integer> nodeFrags = fileFrags.get(sNode);
			for(Integer fragNum : nodeFrags){
				if(!requestedFrag.contains(fragNum)){
					pool.execute(new FileFragmentDownloadloader(sNode, fragNum));
					requestedFrag.add(fragNum);
					fileRetLog.fileSources.add(MyPair.create(sNode, fragNum));    // Increment the downloadCounter as one more fileFrag is added into downloading queue
					if(--requiredDownloadCnt < 1){
						fileRetLog.retStart = System.currentTimeMillis(); Logger.v(TAG, "Retrieval Start: " + fileRetLog.retStart );
						timer.start();
						return;
					}
				}				
			}
		}
		fileRetLog.retStart = System.currentTimeMillis(); Logger.v(TAG, "Retrieval Start: " + fileRetLog.retStart );
		timer.start();
		// Currently I open "one connection for each fragment on a device". This should be fixed!
	}
	
	class SortByHopCount implements Comparator<Integer>{
		private int hopCount1, hopCount2;
		@Override
		public int compare(Integer nodeId1, Integer nodeId2) {
			//TODO commenting out the code for now sonce we don't have routing info. REVISIT
			/*
			try {
				hopCount1 = manager.getForwardRouteEntry(nodeId1).getHopCount();
				hopCount2 = manager.getForwardRouteEntry(nodeId2).getHopCount();
				if(hopCount1 == hopCount2){
					// Compare the number of fragments it has. Prefer nodes with more fragments.
					return -(fileFrags.get(nodeId1).size()-fileFrags.get(nodeId2).size());
				}
				else{
					return hopCount1-hopCount2;
				}
			} catch (NoSuchRouteException e) {
				e.printStackTrace();
			} catch (RouteNotValidException e) {
				e.printStackTrace();
			}
			*/
			return 0;
		}
	}
	
	/**
	 * This function can only be called when enough file and key fragments are available <br>
	 * Blocking call
	 */
	private void decodeFile(){
		// Read all file fragments
		List<FragmentInfo> fileFragments = new ArrayList<FragmentInfo>();
		File fileFragDir = AndroidIOUtils.getExternalFile(Constants.DIR_ROOT + "/" + 
				MDFSFileInfo.getDirName(fileName, fileId));
		File[] files = fileFragDir.listFiles();
		FragmentInfo frag;
		// Don't use the fragment that may not finish downloading yet
		Set<Integer> downloaded = 
			ServiceHelper.getInstance().getDirectory().getStoredFileIndex(fileId,serviceHelper.getMyNode().getNodeId()).getItemSet();
		for (File f : files) {
			if(f.getName().contains("__frag__")){
				frag = IOUtilities.readObjectFromFile(f, FragmentInfo.class);
				if(frag != null && downloaded.contains(frag.getFragmentNumber()))
					fileFragments.add(frag); 
			}
		}
		
		// Final Check. Make sure enough fragments are available
		if(keyShares.size() < fileInfo.getK1() || fileFragments.size() < fileInfo.getK2()){
			String s = keyShares.size() + " keys " + fileFragments.size() + " file fragments ";
			listener.onError("Insufficient fragments. " + s + " Please try again later");
			return;
		}
		String s = keyShares.size() + " keys " + fileFragments.size() + " file fragments "+" k1 "+ fileInfo.getK1()+" k2 "+fileInfo.getK2();
		System.out.println(" MDFSFileRetriever "+ s);
		listener.statusUpdate("Decoding fragments");
		fileRetLog.decryStart = System.currentTimeMillis();
		MDFSDecoder decoder = new MDFSDecoder(keyShares, fileFragments);
		if(!decoder.decode()){
			listener.onError("Fail to decode the fragments. You may try again");
			return;
		}
		fileRetLog.decryEnd = System.currentTimeMillis();
		
		MDFSDirectoryProtocol directory = ServiceHelper.getInstance().getDirectory();
		// save decrypted data as a file
		byte [] fileBytes = decoder.getPlainBytes();
		File tmp0 = AndroidIOUtils.getExternalFile(Constants.DIR_DECRYPTED);
		File tmp = IOUtilities.createNewFile(tmp0, MDFSFileInfo.getDirName(fileName,fileId));
		try {
			FileOutputStream fos = new FileOutputStream(tmp);
			fos.write(fileBytes, 0, fileBytes.length);
			fos.close();
			directory.addDecryptedFile(fileId);
			Logger.i(TAG, "File Decryption Complete:Decrypted file loc "+tmp.getAbsolutePath());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (NullPointerException e){
			e.printStackTrace();	// tmp may be null....
		}
		
		// save encryted file
		fileBytes = decoder.getEncryptedByteFile();
		tmp0 = AndroidIOUtils.getExternalFile(Constants.DIR_ENCRYPTED);
		tmp = IOUtilities.createNewFile(tmp0, MDFSFileInfo.getDirName(fileName,fileId));
		try {
			FileOutputStream fos = new FileOutputStream(tmp);
			fos.write(fileBytes, 0, fileBytes.length);
			fos.close();
			directory.addEncryptedFile(fileId);
			Logger.i(TAG, "File Decryption Complete:Encrypted file loc "+tmp.getAbsolutePath());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (NullPointerException e){
			e.printStackTrace();	// tmp may be null....
		}
		
		writeLog();
		listener.onComplete(tmp, fileInfo,fileName);
	}
	
	private void writeLog(){
		// Log Data
		//DataLogger dataLogger = ServiceHelper.getInstance().getDataLogger();
		StringBuilder str = new StringBuilder();
		str.append(System.currentTimeMillis() + ", ");
		str.append(fileInfo.getCreator() + ", ");
		str.append(fileInfo.getFileName() + ", ");
		str.append(fileInfo.getFileLength() + ", ");
		str.append(fileRetLog.getDiff(fileRetLog.discStart, fileRetLog.discEnd-Constants.FILE_REQUEST_TIMEOUT) + ", ");
		str.append(fileRetLog.getDiff(fileRetLog.decryStart, fileRetLog.decryEnd) + ", ");
		str.append(fileRetLog.getDiff(fileRetLog.retStart, fileRetLog.retEnd) + ", ");
		str.append("\n");
		
		String tmpStr = "	";
		Iterator<FileREP> iter = fileRetLog.fileReps.iterator();
		while(iter.hasNext()){
			FileREP rep = iter.next();
			if(rep.getKeyShare() != null){
				tmpStr += rep.getSource() + ", " + rep.getKeyShare().getIndex() + ", ";
			}
		}
		str.append(tmpStr + "\n");
		
		tmpStr = "	";
		for(MyPair<Integer, Integer> pair : fileRetLog.fileSources){
			tmpStr += pair.first + ", " + pair.second + ", ";
		}
		str.append(tmpStr);
		str.append("\n");
		//dataLogger.appendSensorData(LogFileName.FILE_RETRIEVAL, str.toString());
		
		// File Discovery
		str.delete(0, str.length()-1);
		str.append(Node.getInstance().getNodeId() + ", ");			
		str.append("FileDiscovery, ");
		str.append(fileRetLog.fileReps.size() + ", ");
		str.append(fileInfo.getN1() + ", " + fileInfo.getK1() + ", " + fileInfo.getN2() + ", " + fileInfo.getK2() + ", ");
		str.append(fileRetLog.discStart + ", ");
		str.append(fileRetLog.getDiff(fileRetLog.discStart, fileRetLog.discEnd) + ", ");
		str.append("\n");
		//dataLogger.appendSensorData(LogFileName.TIMES, str.toString());		
		
		// File Retrieval
		str.delete(0, str.length()-1);
		str.append(Node.getInstance().getNodeId() + ", ");			
		str.append("FileRetrieval, ");
		str.append(fileRetLog.fileReps.size() + ", ");
		str.append(fileInfo.getN1() + ", " + fileInfo.getK1() + ", " + fileInfo.getN2() + ", " + fileInfo.getK2() + ", ");		
		str.append(fileInfo.getFileLength() + ", ");
		str.append(fileRetLog.retStart + ", ");
		str.append(fileRetLog.getDiff(fileRetLog.retStart, fileRetLog.retEnd) + ", ");
		str.append("\n\n");
		//dataLogger.appendSensorData(LogFileName.TIMES, str.toString());
	}
	
	public boolean isKeyOnly() {
		return keyOnly;
	}

	public void setKeyOnly(boolean keyOnly) {
		this.keyOnly = keyOnly;
	}
	
	class FileFragmentDownloadloader implements Runnable{
		private int destId;
		private int fragmentIndex;
		
		private FileFragmentDownloadloader(int destination, int fragmentIdx){
			this.destId = destination;
			this.fragmentIndex = fragmentIdx;
		}
		
		@Override
		public void run() {
			boolean success = false;
			byte[] buffer = new byte[Constants.TCP_COMM_BUFFER_SIZE];
			
			TCPSend send = tcpConnection.creatConnection(IOUtilities.parsePrefix(IOUtilities.getLocalIpAddress()) +destId);
			if(send == null){
				Logger.e(TAG, "Connection Failed");
				return;
			}
			// Handshake
			FileFragPacket header = new FileFragPacket(fileName, 
					fileId, fragmentIndex, FileFragPacket.REQ_TO_RECEIVE);
			header.setFragmented(true);
			header.setNeedReply(false);
			
			
			// Maybe we should wait for another handshake response?
			File tmp0=null;
			try {
				ObjectOutputStream oos = new ObjectOutputStream(send.getOutputStream());
				oos.writeObject(header);
				
				// Start to download and save the file fragment
				Logger.v(TAG, "Start downloading frag " + fragmentIndex + " from " + destId);
				String fDirName = MDFSFileInfo.getDirName(fileName,fileId);
				String fName =  MDFSFileInfo.getShortFileName(fileName)+ "__frag__" + fragmentIndex;
				//tmp0 = IOUtilities.getExternalFile(Constants.DIR_ROOT + "/" +	fDirName );
				tmp0 = AndroidIOUtils.getExternalFile(Constants.DIR_ROOT + "/" + fDirName + "/" + fName );
				FileOutputStream fos = new FileOutputStream(tmp0);
				int readLen=0;
				DataInputStream din = send.getInputStream();
				while ((readLen = din.read(buffer)) >= 0) {
	                fos.write(buffer, 0, readLen);
	                //Logger.v(TAG, "read " + readLen + " bytes");
				}
				if(tmp0.length() > 0)
					Logger.v(TAG, "Finish downloading fragment " + fragmentIndex + " from node " + destId);
				else
					Logger.w(TAG, "Zero bytes file fragment " + fragmentIndex + " from node " + destId);
				
				fos.close(); 
				oos.close();
				din.close();
				send.close();
				success = true;
			} catch (IOException e) {
				e.printStackTrace();
			} finally{
				timer.cancel();
				if(success && tmp0.length() > 0){	// Hacky way to avoid 0 byte file
					// update directory
					MDFSDirectoryProtocol directory = ServiceHelper.getInstance().getDirectory();
					directory.addFileFragment(header.getCreatedTime(), header.getFragIndex(),serviceHelper.getMyNode().getNodeId());
					locFragCounter.incrementAndGet();
				}
				else if(tmp0 != null){ 
					tmp0.delete();
				}				
				
				// Timer is not needed after decoding procedure has started
				if(!decoding){
					//if(success && downloadCounter.decrementAndGet() <= 1){
					if(locFragCounter.get() >= fileInfo.getK2()){ 	//success && 
						decoding = true;
						timer.cancel(); 
						timer.onFinish();						
					}
					else
						timer.start();
				}
			}
		}
	}
	
	public FileRetrieverListener getListener() {
		return listener;
	}

	public void setListener(FileRetrieverListener listener) {
		this.listener = listener;
	}

	/*
	 * Default FileRetrieverListener. Do nothing.
	 */
	private FileRetrieverListener listener = new FileRetrieverListener(){
		@Override
		public void onError(String error) {
		}
		@Override
		public void onComplete(File decryptedFile, MDFSFileInfo fileInfo,String fileName) {
		}
		@Override
		public void statusUpdate(String status) {
		}
	};
	public interface FileRetrieverListener{
		public void onError(String error);
		public void statusUpdate(String status);
		public void onComplete(File decryptedFile, MDFSFileInfo fileInfo,String fileName);
	}
	
	private class FileRetrieveLog{
		public FileRetrieveLog(){}
		public long discStart, discEnd, retStart, retEnd, decryStart, decryEnd;
		public List<MyPair<Integer, Integer>> fileSources = new ArrayList<MyPair<Integer, Integer>>();
		public Set<FileREP> fileReps; 
		public String getDiff(long l1, long l2){
			return Long.toString(Math.abs(l2-l1));
		}
	}
}
