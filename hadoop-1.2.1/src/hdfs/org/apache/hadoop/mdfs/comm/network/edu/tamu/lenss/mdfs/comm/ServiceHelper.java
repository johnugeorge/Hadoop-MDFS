package edu.tamu.lenss.mdfs.comm;

import adhoc.aodv.Node;
import adhoc.etc.Logger;
import edu.tamu.lenss.mdfs.MDFSDirectory;
import edu.tamu.lenss.mdfs.comm.FileReplyHandler.FileREPListener;
import edu.tamu.lenss.mdfs.comm.TopologyHandler.TopologyListener;
import edu.tamu.lenss.mdfs.models.DeleteFile;
import edu.tamu.lenss.mdfs.models.FileREQ;
import edu.tamu.lenss.mdfs.models.NewFileUpdate;
import edu.tamu.lenss.mdfs.models.TaskResult;
//import edu.tamu.lenss.mdfs.activities.JobProcessing.JobManagerListener;
//import android.content.ComponentName;
//import android.content.ServiceConnection;
//import android.os.IBinder;

public class ServiceHelper {
	private static final String TAG = ServiceHelper.class.getSimpleName();
	
	/* Global Shared Instances */
	private static ServiceHelper instance = null;
	private static NetworkObserver service;
	private static MDFSDirectory directory;
	//private static Context context;
	private static volatile boolean connected = false;
	
	private ServiceHelper() {
		/*cont.bindService(new Intent(cont, NetworkObserver.class),
				mConnection, Context.BIND_AUTO_CREATE);*/
		Logger.v(TAG, "Start to bind service");
		service = new NetworkObserver();
		//context = cont;
		directory = MDFSDirectory.readDirectory();
		directory.syncLocal();
	}
	
	public static synchronized ServiceHelper getInstance() {
		if (instance == null) {
			instance = new ServiceHelper();
			Logger.v(TAG, "New Instance Created");
		}
		return instance;
	}
	
	/*public static ServiceHelper getInstance(Context context) {
		if (instance == null) {
			instance = new ServiceHelper(context);
			Logger.v(TAG, "New Instance Created");
		}
		return instance;
	}
	
	public static ServiceHelper getInstance(){
		return instance;
	}*/
	
	/*public static Context getContext(){
		return context;
	}*/

	public Node getMyNode(){
		return service.getMyNode();
	}
	
	/*public AndroidDataLogger getDataLogger(){
		return service.getMyNode().getDatalogger();
	}*/
	
	public MDFSDirectory getDirectory() {
		return directory;
	}

	public static void setDirectory(MDFSDirectory directory) {
		ServiceHelper.directory = directory;
	}

	public static void releaseService(){
		if(instance != null ){ //&& connected
			Logger.v(TAG, "releaseService");
			//context.unbindService(mConnection);
			//connected = false;
			
			service.cancel();
			directory.saveDirectory();
			//service.getMyNode().getDatalogger().closeAllFiles();
			instance = null;
		}
	}
	
/*	private static final ServiceConnection mConnection = new ServiceConnection() {
		@Override
		public void onServiceConnected(ComponentName className, IBinder iBinder) {
			service = ((NetworkObserver.LocalBinder) iBinder).getService();
			Logger.v(TAG, "Service Connected!");
			connected = true;
			//service.init();
		}
		@Override
		public void onServiceDisconnected(ComponentName className) {
			Logger.v(TAG, "Service Disonnected!");
			connected = false;
		}
	};*/
	
	
	// Service Calls
	/**
	 * Non-blocking function call. Returns Immediately
	 * @param lis
	 */
	public void startTopologyDiscovery(TopologyListener lis){
		service.startTopologyDiscovery(lis);
	}
	
	public void startFileRequest(FileREQ request, FileREPListener lis){
		service.startFileRequest(request, lis);
	}
	
	public void deleteFiles(DeleteFile files){
		service.deleteFiles(files);
	}
	
	public void sendFileUpdate(NewFileUpdate update){
		service.sendFileUpdate(update);
	}
	
	/*public void broadcastJobSchedule(TaskSchedule schedule, JobManagerListener list){
		service.broadcastJobSchedule(schedule, list);
	}*/
	
	public void broadcastTaskResult(TaskResult result){
		service.broadcastTaskResult(result);
	}
	
	public void broadcastJobComplete(){
		service.broadcastJobComplete();
	}
	
	public void broadcastMyDirectory(){
		service.broadcastMyDirectory();
	}
}
