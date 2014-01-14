package edu.tamu.lenss.mdfs;

import java.io.BufferedInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.UUID;

import adhoc.aodv.Node;
import adhoc.etc.IOUtilities;
import adhoc.etc.Logger;
import adhoc.tcp.TCPConnection;
import adhoc.tcp.TCPSend;
//import android.content.Context;
import edu.tamu.lenss.mdfs.comm.ServiceHelper;
import edu.tamu.lenss.mdfs.comm.TopologyHandler.TopologyListener;
import edu.tamu.lenss.mdfs.crypto.FragmentInfo;
import edu.tamu.lenss.mdfs.crypto.FragmentInfo.KeyShareInfo;
import edu.tamu.lenss.mdfs.crypto.MDFSEncoder;
import edu.tamu.lenss.mdfs.models.DeleteFile;
import edu.tamu.lenss.mdfs.models.FileFragPacket;
import edu.tamu.lenss.mdfs.models.KeyFragPacket;
import edu.tamu.lenss.mdfs.models.MDFSFileInfo;
import edu.tamu.lenss.mdfs.models.NewFileUpdate;
import edu.tamu.lenss.mdfs.models.NodeInfo;
import edu.tamu.lenss.mdfs.placement.PlacementHelper;
import edu.tamu.lenss.mdfs.utils.AndroidIOUtils;
import edu.tamu.lenss.mdfs.utils.JCountDownTimer;
import edu.tamu.lenss.mdfs.Constants;

import org.apache.hadoop.mdfs.protocol.MDFSDirectoryProtocol;


public class MDFSFileCreator {
	private static final String TAG = MDFSFileCreator.class.getSimpleName();
	private double keyStorageRatio, fileStorageRatio;
	private double keyCodingRatio, fileCodingRatio;	
	
	private int networkSize, k1, n1, k2, n2;
	private File file;
	private TCPConnection tcpConnection;
	private MDFSFileCreatorListener listener;
	private ServiceHelper serviceHelper;
	private List<NodeInfo> nodeInfo = new ArrayList<NodeInfo>();
	private MDFSFileInfo fileInfo;
	private boolean isOptComplete, isEncryptComplete, deleteFileWhenComplete;
	private ExecutorService pool;
	private AtomicInteger fragCounter;
	private final FileCreationLog logger = new FileCreationLog();
	private String fileName;

	public MDFSFileCreator() {
		
	}

	public MDFSFileCreator(File file, double keyCodingRatio,
			double fileCodingRatio, MDFSFileCreatorListener lis) {
		/*SharedPreferences pref = PreferenceManager.getDefaultSharedPreferences(context);
		String keySto = pref.getString(AODVSetting.PREF_KEY_STORAGE_RATIO, "0.8");
		String fileSto = pref.getString(AODVSetting.PREF_FILE_STORAGE_RATIO, "0.8");
		String keyCod = pref.getString(AODVSetting.PREF_KEY_CODING_RATIO, "0.5");
		String fileCod = pref.getString(AODVSetting.PREF_FILE_CODING_RATIO, "0.5");
		
		this.keyCodingRatio = Double.parseDouble(keyCod);
		this.fileCodingRatio = Double.parseDouble(fileCod);
		this.keyStorageRatio = Double.parseDouble(keySto);
		this.fileStorageRatio = Double.parseDouble(fileSto);*/
		
		this.keyCodingRatio = 0.5;
		this.fileCodingRatio = 0.5;
		this.keyStorageRatio = 0.8;
		this.fileStorageRatio = 0.8;
		
		this.file = file;		
		this.listener = lis;
		this.serviceHelper = ServiceHelper.getInstance();
		this.tcpConnection = TCPConnection.getInstance();
		//this.fileInfo = new MDFSFileInfo(file.getName(), file.lastModified(),true);lastmodified is not unique.
		String absPath=file.getAbsolutePath();
		fileName=file.getName();;
		if(absPath.startsWith(Constants.MDFS_HADOOP_DATA_DIR))
			fileName=absPath.substring(Constants.MDFS_HADOOP_DATA_DIR.length());
		else
			System.out.println(" It doesn't start with MDFS_HADOOP_DATA_DIR"+Constants.MDFS_HADOOP_DATA_DIR);
		System.out.println(" File to be created "+fileName);
		
	
		//this.fileInfo = new MDFSFileInfo(file.getName(), UUID.randomUUID().getMostSignificantBits(),true);
		this.fileInfo = new MDFSFileInfo(fileName, fileName.hashCode(),true);
		this.fileInfo.setFileLength(file.length());
		this.fileInfo.setCreator(serviceHelper.getMyNode().getNodeId());
		this.pool = Executors.newCachedThreadPool();
		this.fragCounter = new AtomicInteger();
	}

	/**
	 * discoverTopology() is called first. Once it is complete, encrypteFile()
	 * and optimizePlacement() <br>
	 * are started simultaneously. Both these subroutines need n,k values. Once
	 * both <br>
	 * encrypteFile() and optimizePlacement() are complete, distributeFragment()
	 * is started. <br>
	 * Once distributeFragment() is complete, directoryUpdate() is triggered.<br>
	 * Non-blocking call
	 */
	public void start() {
		discoverTopology();
		setUpTimer();

	}

	private TopologyListener topologyListener;

	/**
	 * Non-blocking call
	 */
	private void discoverTopology() {
		topologyListener = new TopologyListener() {
			@Override
			public void onError(String msg) {
				listener.onError("Topology Disovery Fails. Please try again later");
				Logger.e(TAG, msg);
			}

			@Override
			public void onComplete(List<NodeInfo> topList) {
				logger.topEnd = System.currentTimeMillis();
				nodeInfo = topList;
				for (NodeInfo info : topList) {
					Logger.v(TAG, "Receive NodeInfo from " + info.getSource());
				}
				listener.statusUpdate("Topology Discovery Complete.");

				// Setup some global parameters
				networkSize = nodeInfo.size();
				n1 = (int) Math.ceil(networkSize * keyStorageRatio);
				n2 = (int) Math.ceil(networkSize * fileStorageRatio);
				k1 = (int) Math.round(n1 * keyCodingRatio);
				k2 = (int) Math.round(n2 * fileCodingRatio);
				Logger.v(TAG, "n1:" + n1 + " n2:" + n2 + " k1:" + k1 + " k2:"
						+ k2);
				fileInfo.setFragmentsParms(n1, k1, n2, k2);
				
				pool.execute(new Runnable() {
					@Override
					public void run() {
						optimizePlacement();
					}
				});
				
				pool.execute(new Runnable() {
					@Override
					public void run() {
						encryptFile();
					}
				});
			}
		};
		
		listener.statusUpdate("Starting topology Discovery");
		logger.topStart = System.currentTimeMillis();
		serviceHelper.startTopologyDiscovery(topologyListener);
	}

	private List<KeyShareInfo> keyShares; // Cache the key fragments

	/**
	 * Blocking call
	 */
	private void encryptFile() {
		isEncryptComplete = false;
		logger.encryStart = System.currentTimeMillis();
		if(file == null || !file.exists()){
			Logger.v(TAG," File doesn't exist.Hence returning from EncryptFile "+file.getName());
			return;
		}
		MDFSEncoder encoder = new MDFSEncoder(file, n1, n2, k1, k2,fileInfo.getCreatedTime());
		/*
		 * n1=4;n2=4;k1=3;k2=4; MDFSEncoder encoder = new MDFSEncoder(file, 4,
		 * 4, 3, 2);
		 */
		if (!encoder.encode()) {
			listener.onError("File Encryption Failed");
			return;
		}
		logger.encryStop = System.currentTimeMillis();
		List<FragmentInfo> fragInfos = encoder.getFileFragments();
		keyShares = encoder.getKeyShares();

		// Store the file fragments in local SDCard
		File fragsDir = AndroidIOUtils.getExternalFile(Constants.DIR_ROOT + "/"
				+ MDFSFileInfo.getDirName(fileName, fileInfo.getCreatedTime()));

		MDFSDirectoryProtocol directory = serviceHelper.getDirectory();
		// Create file fragments
		for (FragmentInfo frag : fragInfos) {
			File tmp = IOUtilities.createNewFile(fragsDir, frag.getFileName()
					+ "__frag__" + frag.getFragmentNumber());
			if (IOUtilities.writeObjectToFile(frag, tmp)) {
				directory.addFileFragment(fileInfo.getCreatedTime(),
						frag.getFragmentNumber(),serviceHelper.getMyNode().getNodeId());
			}
		}
		listener.statusUpdate("Encryption Complete");
		isEncryptComplete = true;
		distributeFragments();
	}

	private List<Integer> keyStorages, fileStorages;
	//private Map<Integer, List<Integer>> keySourceLocations,	fileSourceLocations;

	/**
	 * Blocking function call
	 */
	private void optimizePlacement() {
		listener.statusUpdate("Start Optimization");
		isOptComplete = false;
		logger.optStart = System.currentTimeMillis();
		PlacementHelper helper = new PlacementHelper(new HashSet<NodeInfo>(
				nodeInfo), n1, k1, n2, k2);
		helper.findOptimalLocations();
		logger.optStop = System.currentTimeMillis();
		fileStorages = helper.getFileStorages();
		//fileSourceLocations = helper.getFileSourceLocations();
		keyStorages = helper.getKeyStorages();
		//keySourceLocations = helper.getKeySourceLocations();
		
		fileInfo.setKeyStorage(new HashSet<Integer>(keyStorages));
		fileInfo.setFileStorage(new HashSet<Integer>(fileStorages));

		StringBuilder str = new StringBuilder("File Storages:");
		for (Integer i : fileStorages) {
			str.append(i + " ");
		}
		str.append("\n Key Storages:");
		for (Integer i : keyStorages) {
			str.append(i + " ");
		}
		Logger.v(TAG, str.toString());
		listener.statusUpdate("Optimization Complete");
		isOptComplete = true;
		distributeFragments();
	}
	private JCountDownTimer jTimer;
	
	private void setUpTimer(){
		
		jTimer = new JCountDownTimer(Constants.FRAGMENT_DISTRIBUTION_INTERVAL, Constants.FRAGMENT_DISTRIBUTION_INTERVAL){
			@Override
			public void onFinish() {
				if (fragCounter.get() > k2 || (fragCounter.get()==k2 && n2 == k2 )){
					Logger.v(TAG, fragCounter.get() + " fragments were distributed");
					logger.distStop = System.currentTimeMillis();
					updateDirectory();
				}
				else{
					// Delete the key fragments
					DeleteFile deleteFile = new DeleteFile();
					deleteFile.setFile(fileInfo.getFileName(), fileInfo.getCreatedTime());
					ServiceHelper.getInstance().deleteFiles(deleteFile);
					listener.onError("Fail to distribute file fragments. " +
							"Only " + fragCounter.get() + " were successfully sent. Please try again later");
				}
			}

			@Override
			public void onTick(long millisUntilFinished) {
			}
		};
	}
	
	/**
	 * Can only start after both optimizePlacement() and encryptFile() have
	 * completed
	 */
	private void distributeFragments() {
		if (!isOptComplete || !isEncryptComplete)
			return;

		// Scan through all files in the folder and upload them
		File fileFragDir = AndroidIOUtils.getExternalFile(Constants.DIR_ROOT + "/"
				+ MDFSFileInfo.getDirName(fileName, fileInfo.getCreatedTime()));
		File[] files = fileFragDir.listFiles();
		String name;

		// Distribute the key fragments
		/*
		 * keyStorages = new ArrayList<Integer>(); keyStorages.add(2);
		 * keyStorages.add(4); keyStorages.add(6); keyStorages.add(7);
		 */
		final Node node = serviceHelper.getMyNode();
		int destNode;
		
		// Hack: Try to deliver the key multiple times...
		for(int keyRun=0; keyRun < 1 ; keyRun++) {
		Iterator<Integer> nodesIter = keyStorages.iterator();
		for (KeyShareInfo key : keyShares) {
			if (nodesIter != null && nodesIter.hasNext()) {
				destNode = nodesIter.next();
				//Logger.v(TAG, "Key Frag: destNode "+destNode+" my node id "+node.getNodeId());
				if (destNode != node.getNodeId()) {
					final KeyFragPacket packet = new KeyFragPacket(node.getNodeId(),
							destNode, key, fileInfo.getCreatedTime());
					// This thread may not be necessary
					/*pool.execute(new Runnable(){	
						@Override
						public void run() {
							// Problem: There is no way to confirm that the key fragment is successfully sent ...
							// node.sendAODVDataContainer(packet);
						}
					});*/
					//Logger.v(TAG, "Sending key packet to destNode "+destNode);
					node.sendAODVDataContainer(packet);
					//code till here
				} else {
					// Just store the key fragment locally.
					//Logger.v(TAG, "Saving key packet locally");
					File tmp = IOUtilities.createNewFile(fileFragDir,
							key.getFileName() + "__key__" + key.getIndex());
					if (IOUtilities.writeObjectToFile(key, tmp)) {
						MDFSDirectoryProtocol directory = serviceHelper.getDirectory();
						directory.addKeyFragment(fileInfo.getCreatedTime(),
								key.getIndex(),serviceHelper.getMyNode().getNodeId());
					}
				}
			}
		}
		}
		
		// Distribute the file fragments
		/*
		 * fileStorages = new ArrayList<Integer>(); fileStorages.add(2);
		 * fileStorages.add(4); fileStorages.add(6); fileStorages.add(7);
		 */
		logger.distStart = System.currentTimeMillis();
		fragCounter.set(0);
		Iterator<Integer> nodesIter = fileStorages.iterator();
		for (File f : files) {
			name = f.getName();
			if (name.contains("__frag__")) {
				// Find the fragment Number
				if (nodesIter != null && nodesIter.hasNext()) {
					destNode = nodesIter.next();
					if (destNode == node.getNodeId()){
						fragCounter.incrementAndGet();
						continue; // Don't need to send to myself again
					}

					pool.execute(new FileFragmentUploader(f, fileInfo.getCreatedTime(), destNode, !nodesIter.hasNext()));
				}
			} else if (name.contains("__key__")) {
				/*
				 * This may only happens if the file creator is also chosen as
				 * the key storage node and the key fragment is copied to this
				 * folder faster than the files in the folder is read. Just
				 * ignore it for now
				 */
			}
		}
		
		jTimer.start(); //jTimer.start();
		listener.statusUpdate("Distributing file fragments");
	}

	private void updateDirectory() {
		NewFileUpdate update = new NewFileUpdate(fileInfo);
		serviceHelper.sendFileUpdate(update);
		Logger.v(TAG, "File Id: " + fileInfo.getCreatedTime());
		// Update my directory as well
		serviceHelper.getDirectory().addFile(fileInfo);
		if(this.deleteFileWhenComplete)
			file.delete();
		
		writeLog();
		listener.onComplete();
	}
	
	private void writeLog(){
		// Log data
		//AndroidDataLogger dataLogger = ServiceHelper.getInstance().getDataLogger();
		StringBuilder str = new StringBuilder();
		str.append(System.currentTimeMillis() + ", ");
		str.append(fileInfo.getCreator() + ", ");
		str.append(fileInfo.getFileName() + ", ");
		str.append(fileInfo.getFileLength() + ", ");
		str.append(networkSize + ", ");
		str.append(fileInfo.getN1() + ", ");
		str.append(fileInfo.getK1() + ", ");
		str.append(fileInfo.getN2() + ", ");
		str.append(fileInfo.getK2() + ", ");
		str.append(logger.getDiff(logger.topStart, logger.topEnd-Constants.TOPOLOGY_DISCOVERY_TIMEOUT)+ ", ");
		str.append(logger.getDiff(logger.optStart, logger.optStop) + ", ");
		str.append(logger.getDiff(logger.encryStart, logger.encryStop) + ", ");
		str.append(logger.getDiff(logger.distStart, logger.distStop) + "\n");
		
		String tmp="	";
		for(Integer i : fileInfo.getKeyStorage())
			tmp += i + ",";
		tmp += "\n";
		str.append(tmp);
		
		tmp="	";
		for(Integer i : fileInfo.getFileStorage())
			tmp += i + ",";
		tmp += "\n";
		str.append(tmp);		
		//dataLogger.appendSensorData(LogFileName.FILE_CREATION, str.toString());		
		
		// Topology Discovery
		str.delete(0, str.length()-1);
		str.append(Node.getInstance().getNodeId() + ", ");			
		str.append("TopologyDisc, ");
		str.append(networkSize + ", " + n1 + ", " + k1 + ", " + n2 + ", " + k2 + ", ");
		str.append(logger.topStart + ", ");
		str.append(logger.getDiff(logger.topStart, logger.topEnd-Constants.TOPOLOGY_DISCOVERY_TIMEOUT ) + ", ");
		str.append("\n");				
		//dataLogger.appendSensorData(LogFileName.TIMES, str.toString());
		
		// Optimization
		str.delete(0, str.length()-1);
		str.append(Node.getInstance().getNodeId() + ", ");			
		str.append("Optimization, ");
		str.append(networkSize + ", " + n1 + ", " + k1 + ", " + n2 + ", " + k2 + ", ");
		str.append(logger.optStart + ", ");
		str.append(logger.getDiff(logger.optStart, logger.optStop) + ", ");
		str.append("\n");
		//dataLogger.appendSensorData(LogFileName.TIMES, str.toString());		
		
		
		// File Distribution
		str.delete(0, str.length()-1);
		str.append(Node.getInstance().getNodeId() + ", ");			
		str.append("FileDistribution, ");
		str.append(networkSize + ", " + n1 + ", " + k1 + ", " + n2 + ", " + k2 + ", ");
		str.append(fileInfo.getFileLength() + ", ");
		str.append(logger.distStart + ", ");
		str.append(logger.getDiff(logger.distStart, logger.distStop) + ", ");
		str.append("\n\n");
		//dataLogger.appendSensorData(LogFileName.TIMES, str.toString());		
	}

	/**
	 * The source file will be deleted when set to true 
	 * @return
	 */
	public boolean isDeleteFileWhenComplete() {
		return deleteFileWhenComplete;
	}

	public void setDeleteFileWhenComplete(boolean deleteFileWhenComplete) {
		this.deleteFileWhenComplete = deleteFileWhenComplete;
	}



	class FileFragmentUploader implements Runnable {
		private File fileFrag;
		private int destId;
		private long fileCreatedTime;
		private int fragmentIndex;

		/**
		 * 
		 * @param frag
		 *            The Fragment File stored on SDCard
		 * @param fileCreationTime
		 *            The file_creation time of the original plain file
		 * @param dest
		 *            Destination
		 * @param last
		 *            Is this is the last fragment to send? It is used to
		 *            trigger the next step, directoryUpdate();
		 */
		public FileFragmentUploader(File frag, long fileCreationTime, int dest,
				boolean last) {
			this.fileFrag = frag;
			this.destId = dest;
			this.fileCreatedTime = fileCreationTime;
			this.fragmentIndex = parseFragNum(frag.getName());
		}

		private int parseFragNum(String fName) {
			String idx = fName.substring(fName.lastIndexOf("_") + 1);
			return Integer.parseInt(idx.trim());
		}

		@Override
		public void run() {
			boolean success = false;
			try {
				TCPSend send = tcpConnection
						.creatConnection(IOUtilities.parsePrefix(IOUtilities.getLocalIpAddress()) + destId);
				if (send == null) {
					Logger.e(TAG, "Connection Failed");
					return;
				}
				// Handshake
				FileFragPacket header = new FileFragPacket(fileFrag.getName(),
						fileCreatedTime, fragmentIndex,
						FileFragPacket.REQ_TO_SEND);
				header.setNeedReply(true);
				ObjectOutputStream oos = new ObjectOutputStream(
						send.getOutputStream());
				oos.writeObject(header);

				ObjectInputStream ois = new ObjectInputStream(
						send.getInputStream());
				header = (FileFragPacket) ois.readObject();
				if (!header.isReady()) {
					Logger.e(TAG, "Destination reject to receive");
					return;
				}

				byte[] mybytearray = new byte[Constants.TCP_COMM_BUFFER_SIZE];

				int readLen = 0;
				FileInputStream fis = new FileInputStream(fileFrag);
				BufferedInputStream bis = new BufferedInputStream(fis);
				DataOutputStream out = send.getOutputStream();
				while ((readLen = bis.read(mybytearray, 0, Constants.TCP_COMM_BUFFER_SIZE)) > 0) {
					out.write(mybytearray, 0, readLen);
				}
				// Logger.v(TAG, "Finish writing data to OutStream");
				send.close();
				fis.close();
				ois.close();
				oos.close();
				success = true;
			} catch (UnknownHostException e) {
				e.printStackTrace();
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} finally {
				jTimer.cancel();	// Reset the timer
				if (!success) {
					// Remove this node from the directory update
					fileInfo.getFileStorage().remove(destId);
					jTimer.start(); //jTimer.start();
				}
				else if(fragCounter.incrementAndGet() >= n2){	// all fragments have been sent 
					jTimer.onFinish();	
				}
				else{
					jTimer.start(); //jTimer.start();
				}
			}
		}
	}

	public interface MDFSFileCreatorListener {
		public void statusUpdate(String status);

		public void onError(String error);

		public void onComplete();
	}
	
	private class FileCreationLog{
		public FileCreationLog(){}
		public long topStart, topEnd, encryStart, encryStop, 
		optStart, optStop, distStart, distStop;
		public String getDiff(long l1, long l2){
			return Long.toString(Math.abs(l2-l1));
		}
	}
}
