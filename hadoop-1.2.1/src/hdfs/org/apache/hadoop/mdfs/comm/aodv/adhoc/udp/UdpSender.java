package adhoc.udp;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import adhoc.aodv.Constants;
import adhoc.aodv.exception.DataExceedsMaxSizeException;
import adhoc.etc.IOUtilities;
import adhoc.etc.Logger;

/**
 * This is a single UDPSender. All data, including broadcast, are sent through a single port though the same thread 
 */
public class UdpSender {
	private DatagramSocket datagramSocket;
	private int receiverPort = Constants.UDP_RCV_PORT;
	private static final String TAG = UdpSender.class.getSimpleName();
	private static final String subNet;
	static{
		subNet = IOUtilities.parsePrefix(IOUtilities.getLocalIpAddress());
		Logger.v(TAG, "subnet: " + subNet);
	}
	
	private ExecutorService pool;
	
	public UdpSender(){
	    try {
			datagramSocket = new DatagramSocket(Constants.UDP_SNT_PORT);
		} catch (SocketException e) {
			e.printStackTrace();
		}
	    this.pool = Executors.newCachedThreadPool();
	}

	/**
	 * Sends data using the UDP protocol to a specific receiver
	 * @param destinationNodeID indicates the ID of the receiving node. Should be a positive integer.
	 * @param data is the message which is to be sent. 
	 * @throws IOException 
	 * @throws SizeLimitExceededException is thrown if the length of the data to be sent exceeds the limit
	 */
	//private InetAddress IPAddress;
	private byte[] data = new byte[Constants.UDP_MAX_PACKAGE_SIZE];
	private DatagramPacket sendPacket = new DatagramPacket(data,data.length);
	
	public boolean sendPacket(int destinationNodeID, final byte[] data) throws IOException, DataExceedsMaxSizeException {
		if (data.length <= Constants.UDP_MAX_PACKAGE_SIZE) {
			final InetAddress IPAddress = InetAddress.getByName(subNet+ destinationNodeID);
			// do we have a packet to be broadcasted?
			
			if (destinationNodeID == Constants.BROADCAST_ADDRESS) {
				datagramSocket.setBroadcast(true);
				sendPacket.setData(data);
				sendPacket.setAddress(IPAddress);
				sendPacket.setPort(receiverPort+1);
				Logger.v(TAG, "data of length: " + data.length + " bytes is broadcasted"	);
				datagramSocket.send(sendPacket);
			} else {
				// Hack! Use TCP instead of UDP
				pool.execute(new UdpPacketRunnable(destinationNodeID, data));
			}
			return true;
		} else {
			throw new DataExceedsMaxSizeException();
		}
	}
	
	private class UdpPacketRunnable implements Runnable{
		private int destinationNodeID;
		private byte[] data;
		
		public UdpPacketRunnable(int destinationNodeIDIn, final byte[] dataIn){
			this.destinationNodeID = destinationNodeIDIn;
			this.data = dataIn;
		}
		@Override
		public void run() {
			Socket tcpSocket=null;
			try {
				InetAddress IPAddress = InetAddress.getByName(subNet+ destinationNodeID);
				tcpSocket = new Socket(IPAddress, receiverPort);
				//Logger.v(TAG, "Start TCP Connection Established");
				tcpSocket.getOutputStream().write(data);
			} catch (IOException e) {
				e.printStackTrace();
				Logger.v(TAG, " Error in TCPSocket  in UDPSender when write");

			} finally{
				if(tcpSocket != null){
					try {
						tcpSocket.getOutputStream().flush();
						tcpSocket.close();
						Logger.v(TAG, "data of length: " + data.length + " bytes is sent to "	+ tcpSocket.getInetAddress().getHostAddress());
					} catch (IOException e) {
						e.printStackTrace();
						Logger.v(TAG, " Error in TCPSocket  in UDPSender when close");
					}
				}
			}
			
		}
		
	}
	
	public void closeSoket(){
		if(!datagramSocket.isClosed())
			datagramSocket.close();
		pool.shutdown();
	}
}
