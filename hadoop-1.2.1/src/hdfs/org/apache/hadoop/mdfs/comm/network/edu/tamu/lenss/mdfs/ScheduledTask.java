package edu.tamu.lenss.mdfs;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import edu.tamu.lenss.mdfs.comm.ServiceHelper;

public class ScheduledTask {
	/*private ScheduledThreadPoolExecutor taskExecutor = new ScheduledThreadPoolExecutor(1);
	public static final int NODEINFO_PERIOD = 45000;
	public ScheduledTask(){
		
	}
	
	public void start(){
		broadcastMyInfo();
	}
	
	public void stop(){
		taskExecutor.shutdown();
	}
	
	private void broadcastMyInfo(){
		taskExecutor.scheduleAtFixedRate(new Runnable(){
    		@Override
    		public void run() {
    			ServiceHelper.getInstance().broadcastMyDirectory();
    		}
    	}, 0, NODEINFO_PERIOD, TimeUnit.MILLISECONDS);
	}*/
}
