package adhoc.tcp;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import adhoc.aodv.Constants;
import adhoc.etc.IOUtilities;
import adhoc.etc.Logger;
import adhoc.tcp.TCPControlPacket.TCPPacketType;

/**
 * Call close() after finishing the socket
 * @author Jay
 *
 */
public class TCPSend {
	private Socket tcpSocket; 
	private DataInputStream in;
	private DataOutputStream out;
	private String destIp;
	private String nextHopIp;
	private int destPort;
	private static final String TAG = TCPSend.class.getSimpleName();
	//private static final int BUFFER_SIZE = Constants.TCP_COMM_BUFFER_SIZE;
	
	protected TCPSend(String ip, int port){
		this.destIp = ip;
		this.destPort = port;
	}
	
	/**
	 * Blocking function call
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	protected boolean init() {
		boolean success = false;
		/*nextHopIp = TCPConnection.nextHop(destIp);
		if(TextUtils.isEmpty(nextHopIp)){
			Logger.w(TAG, "Route to the destination is not available");
			return false;
		}*/
		nextHopIp = destIp;
		try{
			//Logger.v(TAG, "Connecting to " + nextHopIp + "...");
			tcpSocket = new Socket(nextHopIp, destPort);
			Logger.v(TAG, "Connected with " + nextHopIp);
			
			in = new DataInputStream(tcpSocket.getInputStream());
			out = new DataOutputStream(tcpSocket.getOutputStream());
			tcpSocket.setSoTimeout(Constants.TCP_SEND_READ_TO);
			sendControlPacket(out);
			
			TCPControlPacket ctrPkt=null;
			ObjectInputStream oin = new ObjectInputStream(in);
			ctrPkt = (TCPControlPacket)oin.readObject();
			
			
			if(ctrPkt != null){
				success = verifyControlPacket(ctrPkt);
			}
			else{
				close();
			}
			
		} catch(IOException e){
			e.printStackTrace();
			Logger.w(TAG, "Fail connection to " + nextHopIp);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return success;
	}
	
	/**
	 * Send the TCPControlPacket to the next hop.
	 * @param out
	 * @throws IOException 
	 */
	private void sendControlPacket(DataOutputStream out) throws IOException{
		TCPControlPacket packet = new TCPControlPacket();
		packet.setSourceIP(IOUtilities.getLocalIpAddress());
		packet.setDestIP(destIp);
		packet.setDestPort(destPort);
		packet.setNextHopPort(destPort);
		packet.setNextHopIP(nextHopIp);
		packet.setStatus(TCPPacketType.CreateRoute);
		
		ObjectOutputStream oos = new ObjectOutputStream(out);
		oos.writeObject(packet);
		//Logger.v(TAG, "Control Packet is successfully sent");
	}
	
	/**
	 * Verify that the TCPControlPacket is valid
	 * @param packetData
	 * @return
	 */
	private boolean verifyControlPacket(TCPControlPacket packet) {
		if( packet != null &&
			packet.getStatus()==TCPPacketType.RouteEstablished &&
			packet.getSourceIP().equalsIgnoreCase(destIp)){
			
			//Logger.v(TAG, "Packet verified!!!!");
			return true;
		}
		else{
			Logger.v(TAG, "Packet verification fails!!!!");
			return false;
		}
	}
	
	public DataOutputStream getOutputStream(){
		return out;
	}
	
	public DataInputStream getInputStream(){
		return in;
	}
	
	/**
	 * Always close outputstream first. It will call flush() first and close the outputstream and socket. 
	 */
	public void close() {
		try {
			// This delay assure that all data has been transmitted. Otherwise, some packet always lost..
			Thread.sleep(500);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		if(out != null){
			try {
				out.flush();
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
		if(in != null){
			try {
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if(!tcpSocket.isClosed())
			try {
				tcpSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
	}
	
	/*public class ReadControl{
		private boolean flag1;
		private byte[] buffer;
		private int length;
		
		public ReadControl(){}
		public void setFlag(boolean b){
			flag1 = b;
		}
		public boolean getFlag(){
			return flag1;
		}
		public byte[] getBuffer() {
			return buffer;
		}
		public void setBuffer(byte[] buffer) {
			this.buffer = buffer;
		}
		public int getLength() {
			return length;
		}
		public void setLength(int length) {
			this.length = length;
		}
	}*/
}
