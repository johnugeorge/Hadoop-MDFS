package adhoc.udp;

import java.io.IOException;
import java.net.BindException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import adhoc.aodv.Constants;
import adhoc.aodv.Node;
import adhoc.aodv.Receiver;
import adhoc.etc.IOUtilities;
import adhoc.etc.Logger;

/**
 * Class running as a separate thread, and responsible for receiving data
 * packets over the UDP protocol.
 * 
 * @author Jay
 * 
 */
public class UdpReceiver implements Runnable {
	private Receiver parent;
	private UdpBroadcastReceiver udpBroadcastReceiver;

	private volatile boolean keepRunning = true;
	private Thread udpReceiverthread;
	private HashSet<Integer> blockingIps;
	public static final String TAG = UdpReceiver.class.getSimpleName();
	
	private ServerSocket tcpSocket;
	private ExecutorService pool;

	public UdpReceiver(Receiver parent, int nodeAddress)
			throws SocketException, UnknownHostException, BindException {
		this.parent = parent;
		udpBroadcastReceiver = new UdpBroadcastReceiver(Constants.UDP_RCV_PORT);
		pool = Executors.newCachedThreadPool();
		try {
			tcpSocket = new ServerSocket(Constants.UDP_RCV_PORT);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void startThread() {
		keepRunning = true;
		udpBroadcastReceiver.startBroadcastReceiverthread();
		udpReceiverthread = new Thread(this, UdpReceiver.class.getSimpleName());
		udpReceiverthread.start();
	}

	public void stopThread() {
		keepRunning = false;
		udpBroadcastReceiver.stopBroadcastThread();
		udpReceiverthread.interrupt();
		pool.shutdown();
		try {
			if(tcpSocket != null)
				tcpSocket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void run() {
		while (keepRunning) {
			try {
				Socket socket = tcpSocket.accept();
				//Logger.v(TAG, "Receive Connection from " + socket.getRemoteSocketAddress());
				pool.execute(new TcpPacketReceiver(socket));
				
			} catch (IOException e) {
				Logger.e(TAG, e.toString());
			}
		}
	}
	
	private class TcpPacketReceiver implements Runnable{
		private Socket tcpSocket;
		private byte[] buffer = new byte[Constants.UDP_MAX_PACKAGE_SIZE];
		public TcpPacketReceiver(Socket socket){
			tcpSocket = socket;
		}
		
		@Override
		public void run() {
			String tmp = tcpSocket.getInetAddress().getHostAddress();
			int nodeId = IOUtilities.parseNodeNumber(tmp);
			// Filter out the blocking IPs
			HashSet<Integer> blockingIps = Node.getInstance().getRouteManager()
					.getBlockingIpSet();
			if (blockingIps.contains(nodeId))
				return;
			
			int len;
			try {
				len = tcpSocket.getInputStream().read(buffer);
				if(len <=0 )
					return;
				byte[] result = new byte[len];
				System.arraycopy(buffer, 0, result, 0, len);
				parent.addMessage(nodeId, result);
			} catch (IOException e) {
				e.printStackTrace();
			} finally{
				try {
					tcpSocket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Use different port to receive broadcast packet.
	 * 
	 * @author Jay
	 */
	private class UdpBroadcastReceiver implements Runnable {
		private DatagramSocket brodcastDatagramSocket;
		private volatile boolean keepBroadcasting = true;
		private Thread udpBroadcastReceiverThread;
		private int myNodId;

		public UdpBroadcastReceiver(int receiverPort) throws SocketException,
				BindException {
			brodcastDatagramSocket = new DatagramSocket(receiverPort + 1);
			myNodId = IOUtilities.parseNodeNumber(IOUtilities.getLocalIpAddress());
		}

		public void startBroadcastReceiverthread() {
			keepBroadcasting = true;
			udpBroadcastReceiverThread = new Thread(this, "Broadcast_" + UdpReceiver.class.getSimpleName());
			udpBroadcastReceiverThread.start();
		}

		private void stopBroadcastThread() {
			keepBroadcasting = false;
			if (!brodcastDatagramSocket.isClosed())
				brodcastDatagramSocket.close();
			udpBroadcastReceiverThread.interrupt();
		}
		
		private byte[] bcBuffer = new byte[Constants.UDP_MAX_PACKAGE_SIZE];
		private DatagramPacket brodcastReceivePacket = new DatagramPacket(
				bcBuffer, bcBuffer.length);
		private byte[] bdResult;
		private String bdTmp;
		private int bdNodeId;
		public void run() {
			while (keepBroadcasting) {
				try {
					brodcastDatagramSocket.receive(brodcastReceivePacket);
					bdTmp = brodcastReceivePacket.getAddress().getHostAddress();
					bdNodeId = IOUtilities.parseNodeNumber(bdTmp);

					// Filter out the blocking IPs
					blockingIps = Node.getInstance().getRouteManager()
							.getBlockingIpSet();
					if (blockingIps.contains(bdNodeId) || bdNodeId == myNodId) {
						continue;
					}

					bdResult = new byte[brodcastReceivePacket.getData().length];
					
					System.arraycopy(brodcastReceivePacket.getData(), 0, bdResult, 0, brodcastReceivePacket.getData().length);	// brodcastReceivePacket.getLength()

					//Logger.v(TAG, "Receive Broadcast of " + brodcastReceivePacket.getSocketAddress());

					parent.addMessage(bdNodeId, bdResult);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
