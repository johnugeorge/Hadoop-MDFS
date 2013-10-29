package adhoc.tcp;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Observable;

import adhoc.aodv.Constants;
import adhoc.aodv.Node;
import adhoc.aodv.RouteTableManager;
import adhoc.aodv.routes.ForwardRouteEntry;
import adhoc.etc.IOUtilities;
import adhoc.etc.Logger;
import adhoc.tcp.TCPReceive.TCPReceiverData;
import adhoc.tcp.TCPReceive.TCPReceiverListener;

/**
 * Singleton Class
 * @author Jay
 *
 */
public class TCPConnection extends Observable {
	private int inPort = 8866;
	private static TCPConnection instance = null;
	private static TCPReceive tcpReceiver;
	private RouteTableManager manager;
	private static final String TAG = TCPConnection.class.getSimpleName();
	
	/**
	 * Can't be extended
	 */
	private TCPConnection(){
		tcpReceiver = new TCPReceive(tcpRcvListener, inPort);
		init();
	}
	
	public static synchronized TCPConnection getInstance() {
		if (instance == null) {
			instance = new TCPConnection();
		}
		return instance;
	}
	
	public static void stopAllTCP(){
		tcpReceiver.close();
		instance=null;
	}
	
	private void init(){
		try {
			tcpReceiver.init();
			tcpReceiver.start();
		} catch (IOException e) {
			Logger.e(TAG, e.toString());
		}
		
	}
	
	public void setRouteTableManager(RouteTableManager m){
		this.manager = m;
	}
	
	public RouteTableManager getRouteTableManager(){
		return this.manager;
	}
	
	/**
	 * Blocking function call
	 * @param ip
	 * @return return null if the connection fails
	 * @throws IOException 
	 * @throws UnknownHostException 
	 */
	public TCPSend creatConnection(String ip) {
		TCPSend send = new TCPSend(ip, inPort);
		if(send.init())
			return send;
		else 
			return null;
	}
	
	/*
	 * Call back when new connection is received
	 */
	private TCPReceiverListener tcpRcvListener = new TCPReceiverListener(){
		@Override
		public void onNewConnectioin(TCPReceiverData data) {
			notifyRegisters(data);			
		}
	};	
	private void notifyRegisters(final TCPReceiverData data){
		Logger.v(TAG, "Receiver notify Observered");
		setChanged();
		notifyObservers(data);
		clearChanged();
		
		/*new Thread(new Runnable(){
			@Override
			public void run() {
				// notifyObservers() won't be returned until all Observers have finished update()
				notifyObservers(data);
				clearChanged();
			}
		}).start();*/
	}
	
	/**
	 * Search for the next hop
	 * Blocking call
	 * @param ip
	 * @return next hop's IP
	 */
	/*protected static String nextHop(String ip){
		RouteTableManager manager = Node.getInstance().getRouteManager();
		ForwardRouteEntry entry = manager.getForwardRouteEntry(IOUtilities.parseNodeNumber(ip), 2000);
		if(entry != null){
			return Constants.IP_PREFIX + entry.getNextHop();
		}
		else
			return null;
	}*/
}
