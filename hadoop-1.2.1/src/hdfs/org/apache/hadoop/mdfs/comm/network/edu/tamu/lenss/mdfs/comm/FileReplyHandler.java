package edu.tamu.lenss.mdfs.comm;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.tamu.lenss.mdfs.Constants;
import edu.tamu.lenss.mdfs.models.FileREP;
import edu.tamu.lenss.mdfs.models.FileREQ;
import edu.tamu.lenss.mdfs.utils.JCountDownTimer;

public class FileReplyHandler {
	private Map<Long, FileReplyWaiter> runnerMap;
	//private Handler mHandler = new Handler(Looper.getMainLooper());
	public FileReplyHandler() {
		runnerMap = new HashMap<Long, FileReplyWaiter>();
	}

	/**
	 * A convenient way to send the FileREQ and start waiting for the FileREP <br>
	 * If FileREQ is not sent through this function, the Observer can't know which FileREP to expect
	 * @param fileReq
	 */
	public void sendFileRequest(final FileREQ fileReq, final FileREPListener lis) {
		/*mHandler.post(new Runnable() {
			public void run() {
				runnerMap.put(fileReq.getFileCreatedTime(),
						new FileReplyWaiter(fileReq, lis));
			}
		});*/
		runnerMap.put(fileReq.getFileCreatedTime(), new FileReplyWaiter(fileReq, lis));
		ServiceHelper.getInstance().getMyNode().sendAODVDataContainer(fileReq);
	}
	
	protected void receiveNewPacket(FileREP rep) {
		FileReplyWaiter runner = runnerMap.get(rep.getFileCreatedTime());
		// Make sure this is indeed the file fragment I'm waiting for
		if(runner != null){
			runner.receiveNewPacket(rep);
		}
	}
	
	private class FileReplyWaiter{
		private FileREQ fileRequest;
		private JCountDownTimer timer;
		private FileREPListener listener;
		private volatile boolean waitingReply = false;
		private Set<FileREP> replys = new HashSet<FileREP>();
		
		
		private FileReplyWaiter(FileREQ fileReq, FileREPListener lis){
			this.fileRequest = fileReq;
			this.listener = lis;
			this.timer = new JCountDownTimer(Constants.FILE_REQUEST_TIMEOUT,
					Constants.FILE_REQUEST_TIMEOUT) {
				public void onTick(long millisUntilFinished) {
				}

				public void onFinish() {
					if (waitingReply) {
						if (replys.isEmpty())
							listener.onError("Timeout. No fragments received.");
						else
							listener.onComplete(replys);
					}
					waitingReply = false;
					runnerMap.remove(fileRequest.getFileCreatedTime());
				}
			};
			waitingReply = true;
			replys.clear();
			timer.start();
		}
		
		public void receiveNewPacket(FileREP req) {
			replys.add(req);
			timer.cancel(); // Reset the timer
			timer.start(); 
		}
	}
	public interface FileREPListener {
		public void onError(String msg);
		public void onComplete(Set<FileREP> fileREPs);
	}
}
