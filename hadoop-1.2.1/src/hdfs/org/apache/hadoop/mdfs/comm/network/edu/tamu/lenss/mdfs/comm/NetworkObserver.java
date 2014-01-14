package edu.tamu.lenss.mdfs.comm;

import java.util.List;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import adhoc.aodv.Node;
import adhoc.aodv.Node.AODVDataToObserver;
import adhoc.aodv.RouteTableManager;
import adhoc.aodv.pdu.AODVDataContainer;
import adhoc.aodv.routes.ForwardRouteEntry;
import adhoc.etc.Logger;
import adhoc.tcp.TCPConnection;
import adhoc.tcp.TCPReceive.TCPReceiverData;
import edu.tamu.lenss.mdfs.FileRequestHandler;
import edu.tamu.lenss.mdfs.FragExchangeHelper;
//import edu.tamu.lenss.mdfs.MDFSDirectory;
//import edu.tamu.lenss.mdfs.ScheduledTask;
import edu.tamu.lenss.mdfs.comm.FileReplyHandler.FileREPListener;
import edu.tamu.lenss.mdfs.comm.TopologyHandler.TopologyListener;
import edu.tamu.lenss.mdfs.models.DeleteFile;
import edu.tamu.lenss.mdfs.models.RenameFile;
import edu.tamu.lenss.mdfs.models.FileREP;
import edu.tamu.lenss.mdfs.models.FileREQ;
import edu.tamu.lenss.mdfs.models.JobComplete;
import edu.tamu.lenss.mdfs.models.KeyFragPacket;
import edu.tamu.lenss.mdfs.models.MDFSFileInfo;
import edu.tamu.lenss.mdfs.models.MDFSPacketType;
import edu.tamu.lenss.mdfs.models.NewFileUpdate;
import edu.tamu.lenss.mdfs.models.NodeInfo;
import edu.tamu.lenss.mdfs.models.TaskResult;
import edu.tamu.lenss.mdfs.models.TaskSchedule;
import edu.tamu.lenss.mdfs.models.TopologyDiscovery;

import org.apache.hadoop.mdfs.protocol.MDFSDirectoryProtocol;

//import edu.tamu.lenss.mdfs.activities.JobProcessing.JobManagerListener;
//import android.os.Handler;
//import android.os.Looper;
//import android.app.Service;
//import android.os.Message;
//import android.preference.PreferenceManager;
//import android.os.AsyncTask;
//import android.os.PowerManager;
/**
 * This class handles all the packets sent from Network layer.  
 * @author Jay
 */
public class NetworkObserver implements Observer {
	private static final String TAG = NetworkObserver.class.getSimpleName();
	private Node myNode;
	private FragExchangeHelper fragDownloadHelper = new FragExchangeHelper();
	private TopologyHandler topologyHandler = new TopologyHandler();
	private FileRequestHandler fileReqHandler = new FileRequestHandler();
	private FileReplyHandler fileRepHandler = new FileReplyHandler();
	//private TaskProcessingHandler taskProcessingHandler = new TaskProcessingHandler();
	private DeleteFileHandler deleteFileHandler = new DeleteFileHandler();
	private RenameFileHandler renameFileHandler = new RenameFileHandler();
	//private ScheduledTask scheduledTask = new ScheduledTask();
	//private FailureEstimator failureEstimator;
	private ExecutorService pool;
	//private PowerManager pm; 
	//private PowerManager.WakeLock wl;
	
	
	public NetworkObserver(){
		TCPConnection.getInstance().addObserver(this);	// This is an observer
		myNode=Node.getInstance();
		if(myNode == null){
			//stopSelf();
			Logger.e(TAG, "Can't start Network Server");
			return;
		}
		else{
			myNode.addObserver(this);
		}
		myNode.startThread();
		//failureEstimator = new FailureEstimator(this); failureEstimator.start();
		pool = Executors.newCachedThreadPool();
		//scheduledTask.start();
	}
	/*@Override
	public void onCreate() {
		TCPConnection.getInstance().addObserver(this);	// This is an observer
		Node.getInstance().addObserver(this);
		myNode=Node.getInstance();
		if(myNode == null){
			stopSelf();
			return;
		}
		//String blockingIps = PreferenceManager.getDefaultSharedPreferences(getBaseContext()).getString(AODVSetting.PREF_BLOCKING_IPS, "");
		//myNode.getRouteManager().setBlockingIpSet(blockingIps);
		myNode.startThread();
		failureEstimator = new FailureEstimator(this); failureEstimator.start();
		pool = Executors.newCachedThreadPool();
		scheduledTask.start();
		pm = (PowerManager) getSystemService(Context.POWER_SERVICE);
		wl = pm.newWakeLock(PowerManager.SCREEN_DIM_WAKE_LOCK, TAG);
		wl.acquire();
	}*/
	
	/**
	 * Non-blocking function call. Returns Immediately
	 * @param lis
	 */
	protected void startTopologyDiscovery(TopologyListener lis){
		topologyHandler.broadcastRequest(lis);
	}
	
	//private Handler mHandler = new Handler(Looper.getMainLooper());
	protected void startFileRequest(final FileREQ request, final FileREPListener lis){
		//fileRepHandler.sendFileRequest(request, lis);
		/*mHandler.post(new Runnable() {
			public void run() {
				fileRepHandler.sendFileRequest(request, lis);
			}
		});*/
		fileRepHandler.sendFileRequest(request, lis);
	}
	
	protected void deleteFiles(DeleteFile files){
		deleteFileHandler.sendFileDeletionPacket(files);
		deleteFileHandler.processPacket(files,true);	// Also delete my file locally
	}

	protected void renameFiles(RenameFile files){
		renameFileHandler.sendFileRenamePacket(files);
		renameFileHandler.processPacket(files,true);	// Also delete my file locally
	}


	protected void sendFileUpdate(NewFileUpdate update){
		//myNode.sendAODVDataContainer(update);
	}
	
	/*private JobManagerListener jobManagerListener;
	protected void broadcastJobSchedule(TaskSchedule schedule, JobManagerListener list){
		myNode.sendAODVDataContainer(schedule);
		jobManagerListener = list;
	}*/
	
	protected void broadcastTaskResult(TaskResult result){
		myNode.sendAODVDataContainer(result);
	}
	
	protected void broadcastJobComplete(){
		myNode.sendAODVDataContainer(new JobComplete());
	}
	
	/*protected void broadcastMyDirectory(){
		List<MDFSFileInfo> list = ServiceHelper.getInstance().getDirectory().getFileList();
		for(MDFSFileInfo fInfo : list){
			NewFileUpdate fUpdate = new NewFileUpdate(fInfo);
			try {
				Thread.sleep(1200);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			// The file may be deleted within this delay
			if(ServiceHelper.getInstance().getDirectory().getFileInfo(fInfo.getCreatedTime()) != null){
				fUpdate.setMaxHop(1);	// Only Broadcast to the one hop neighbor
				myNode.sendAODVDataContainer(fUpdate);
			}			
		}
	}*/
	
	protected Node getMyNode(){
		return myNode;
	}
	
	private void receiveTopologyDiscovery(TopologyDiscovery top){
		// Failure Probability Estimation
		NodeInfo info = new NodeInfo();
		info.setSource(myNode.getNodeId());
		info.setRank((int)Math.round(Math.random()*3));
		info.setTimeSinceInaccessible((int)Math.round(Math.random()*1000));
		//info.setFailureProbability(failureEstimator.getFailureProbability());
		RouteTableManager manager = myNode.getRouteManager();
		//AndroidDataLogger dataLogger = myNode.getDatalogger();
		String str = System.currentTimeMillis() + ", " + myNode.getNodeId() + ", ";
		for(ForwardRouteEntry entry : manager.getNeighbors()){
			info.addNeighbor(entry.getDestinationAddress());
			str += entry.getDestinationAddress() + ", ";
		}
		str += "\n";
		//dataLogger.appendSensorData(LogFileName.NEIGHBORS, str);
		info.setDest(top.getSource());
		myNode.sendAODVDataContainer(info);
	}
	
	@Override
	public void update(Observable observable, Object arg) {
		/*
		 * The work in this update() method should be completed ASAP. It blocks the Observable object.
		 * Should execute every task in a Thread.
		 */
		
		if(observable instanceof TCPConnection){
			final TCPReceiverData data = (TCPReceiverData)arg;
			Logger.v(TAG, "New incoming TCP");
			pool.execute(new Runnable(){
				@Override
				public void run() {
					fragDownloadHelper.newIncomingTCP(data);
				}
			});
		}
		else if (observable instanceof Node && arg instanceof AODVDataToObserver){
			try{
				AODVDataToObserver msg = (AODVDataToObserver)arg;
				AODVDataContainer container = (AODVDataContainer)msg.getContainedData();
				//Logger.v(TAG, myNode.countObservers() + " registered observers");
				Logger.v(TAG, "Receive Packet Type: " + container.getPacketType());
				switch(container.getPacketType()){
				case MDFSPacketType.TOPOLOGY_DISCOVERY:
					final TopologyDiscovery top = (TopologyDiscovery)msg.getContainedData();
					pool.execute(new Runnable(){
						@Override
						public void run() {
							receiveTopologyDiscovery(top);
						}
					});
					showToast("Receive Discovery from " + top.getSource());
					break;
				case MDFSPacketType.NODE_INFO:
					final NodeInfo info = (NodeInfo)msg.getContainedData();
					pool.execute(new Runnable(){
						@Override
						public void run() {
							topologyHandler.receiveNewPacket(info);
						}
					});
					showToast("Receive NodeInfo from " + info.getSource());
					break;
				case MDFSPacketType.KEY_FRAG_PACKET:
					final KeyFragPacket keyFrag = (KeyFragPacket)msg.getContainedData();
					pool.execute(new Runnable(){
						@Override
						public void run() {
							fragDownloadHelper.downloadKeyFragment(keyFrag);
						}
					});
					showToast("Receive Key Fragment from " + keyFrag.getSource());
					break;
				case MDFSPacketType.NEW_FILE_UPDATE:
					final NewFileUpdate dirUpdate = (NewFileUpdate)msg.getContainedData();
					MDFSDirectoryProtocol dir = ServiceHelper.getInstance().getDirectory();
					dir.addFile(dirUpdate.getFileInfo());
					showToast("Receive Directory Update from " + dirUpdate.getSource());
					break;
				case MDFSPacketType.FILE_REQ:
					final FileREQ fileReq = (FileREQ)msg.getContainedData();
					pool.execute(new Runnable(){
						@Override
						public void run() {
							fileReqHandler.processRequest(fileReq);
						}
					});
					showToast("Receive File Request from " + fileReq.getSource());
					break;
				case MDFSPacketType.FILE_REP:
					final FileREP fileRep = (FileREP)msg.getContainedData();
					pool.execute(new Runnable(){
						@Override
						public void run() {
							fileRepHandler.receiveNewPacket(fileRep);
						}
					});
					showToast("Receive File Reply from " + fileRep.getSource());
					break;
				case MDFSPacketType.DELETE_FILE:
					final DeleteFile deleteFile = (DeleteFile)msg.getContainedData();
					pool.execute(new Runnable(){
						@Override
						public void run() {
							deleteFileHandler.processPacket(deleteFile,false);
						}
					});
					showToast("Receive DeleteFile from " + deleteFile.getSource());
					break;
				case MDFSPacketType.RENAME_FILE:
					final RenameFile renameFile = (RenameFile)msg.getContainedData();
					pool.execute(new Runnable(){
						@Override
						public void run() {
							renameFileHandler.processPacket(renameFile,false);
						}
					});
					showToast("Receive RenameFile from " + renameFile.getSource());
					break;
				case MDFSPacketType.JOB_SCHEDULE:
					final TaskSchedule schedule = (TaskSchedule)msg.getContainedData();
					/*mHandler.post(new Runnable() {
						public void run() {
							taskProcessingHandler.receiveNewSchedule(schedule);
						}
					});*/
					//taskProcessingHandler.receiveNewSchedule(schedule);
					
					/*pool.execute(new Runnable(){
						@Override
						public void run() {
							taskProcessingHandler.receiveNewSchedule(schedule);
						}
					});*/
					//new ExecuteScheduleTask().execute(schedule);
					
					showToast("Receive Job Schedule from " + schedule.getSource());
					break;
				/*case MDFSPacketType.JOB_RESULT:
					final TaskResult result = (TaskResult)msg.getContainedData();
					if(jobManagerListener != null)
						jobManagerListener.onNewResult(result);
					
					showToast("Receive task Result from " + result.getSource());
					break;*/
				/*case MDFSPacketType.JOB_COMPLETE:
					taskProcessingHandler.receiveJobComplete();
					break;*/
				default:
					Logger.w(TAG, "Unrecognized AODVDataToObserver Packet");
					return;
				}
			}
			catch(ClassCastException e){
				Logger.e(TAG, e.toString());
			}
		}
	}
	
	/*private class ExecuteScheduleTask extends AsyncTask<TaskSchedule, Void, Void> {
		@Override
		protected Void doInBackground(TaskSchedule... schedules) {
			taskProcessingHandler.receiveNewSchedule(schedules[1]);
			return null;
		}
	 }*/
	
	/**
	 * Methods used to show some debugging messages
	 * @param msg
	 */
	private void showToast(String msg){
		/*Message m = new Message();
		m.obj = msg;
		uiHandler.sendMessage(m);*/
		Logger.i(TAG, msg);
	}
	/*private Handler uiHandler = new Handler(){
		@Override
		public void handleMessage(Message msg) {
			String str = (String)msg.obj;
			AndroidUIUtilities.showToast(getBaseContext(), str, false);
		}
	};*/
	
	
	/*@Override
    public int onStartCommand(Intent intent, int flags, int startId) {
		Logger.i(TAG, "Received start id " + startId + ": " + intent);
        // We want this service to continue running until it is explicitly
        // stopped, so return sticky.
		return START_STICKY;
	}*/
	
	public void cancel(){
		TCPConnection.getInstance().deleteObserver(this); 
		myNode.deleteObserver(this);
		myNode.stopThread();
		pool.shutdown();
		//scheduledTask.stop();
	}
	
	/*@Override
    public void onDestroy() {
		TCPConnection.getInstance().deleteObserver(this); 
		myNode.deleteObserver(this);
		myNode.stopThread();
		//failureEstimator.stop();
		pool.shutdown();
		scheduledTask.stop();
		//wl.release();
        // Tell the user we stopped.
        //AndroidUIUtilities.showToast(this, "NetworkObserver Service stopped", true);
    }
	
	@Override
	public IBinder onBind(Intent arg0) {
		return mBinder;
	}

    // This is the object that receives interactions from clients.  See
    private final IBinder mBinder = new LocalBinder();

	*//**
     * Class for clients to access.  Because we know this service always
     * runs in the same process as its clients, we don't need to deal with
     * IPC.
     *//*
    public class LocalBinder extends Binder {
    	NetworkObserver getService() {
            return NetworkObserver.this;
        }
    }*/

}
