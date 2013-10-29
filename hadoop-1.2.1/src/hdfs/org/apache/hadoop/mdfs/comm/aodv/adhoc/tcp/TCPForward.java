package adhoc.tcp;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import adhoc.aodv.Constants;
import adhoc.etc.IOUtilities;
import adhoc.etc.Logger;

public class TCPForward {
	private Socket tcpSocket1, tcpSocket2; 
	private DataInputStream in1, in2;
	private DataOutputStream out1, out2;
	private TCPControlPacket controlPacket;
	private String nextHopIp;
	private TCPForwardListener forwardListener;
	private static final String TAG = TCPForward.class.getSimpleName();
	private static final int BUFFER_SIZE = Constants.TCP_COMM_BUFFER_SIZE;
	private volatile boolean connected = true;
	
	
	protected TCPForward(Socket s, DataInputStream in, DataOutputStream out, TCPControlPacket packet, TCPForwardListener lis){
		this.tcpSocket1 = s;
		this.in1 = in;
		this.out1 = out;
		this.controlPacket = packet;
		this.forwardListener = lis;
	}
	
	/**
	 * This is a blocking call. It is returned after the connections between two sides have been established.
	 * @return
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	protected void start() {
		/*nextHopIp = TCPConnection.nextHop(controlPacket.getDestIP());
		if(TextUtils.isEmpty(nextHopIp)){
			Logger.e(TAG, "Route to the destination is not available");
			return;
		}*/
		
		controlPacket.addInterNodes(IOUtilities.getLocalIpAddress());
		controlPacket.setNextHopIP(nextHopIp);
		Logger.v(TAG, "Creating forwarding connection to " + nextHopIp + "...");
		try {
			tcpSocket2 = new Socket(nextHopIp, 8866);
			tcpSocket2.setSoTimeout(Constants.TCP_FORWARD_READ_TO);	// The timeout needs to be reasonably large so that idle channel won't become timeout
			tcpSocket1.setSoTimeout(Constants.TCP_FORWARD_READ_TO);
			
			in2 = new DataInputStream(tcpSocket2.getInputStream());
			out2 = new DataOutputStream(tcpSocket2.getOutputStream());
			Logger.v(TAG, "Forwarding connection to " + nextHopIp + " has been established");
			
			ObjectOutputStream oos = new ObjectOutputStream(out2);
			oos.writeObject(controlPacket);	
			startPiping();
		} catch (IOException e) {
			close();
			e.printStackTrace();
		}
	}
	
	public void startPiping(){
		Logger.v(TAG, "Start Piping");
		
		/* Start another Thread for forwarding data from receiver to the sender.
		 The main thread is forwarding data from the sender to the receiver
		 read() is blocking function call, so threading is necessary */
		new Thread(new Runnable() {
			byte[] buff1 = new byte[BUFFER_SIZE];
			int dataLen1 = 0;
			@Override
			public void run() {
				while (connected) {
					try {
						dataLen1 = in2.read(buff1);
						if (dataLen1 >= 0) {
							//Logger.v(TAG,"Reading and forwarding data to sender");
							out1.write(buff1, 0, dataLen1);
						} else {
							Logger.w(TAG, "Connection to receiver reachs EOF");
							closeBySocket2();
							break;
						}
					} catch (IOException e) {
						Logger.e(TAG, "buff1: " + e.toString());
						e.printStackTrace();
						if(connected)
							close();
					}
				}
			}
		}).start();
		
		byte[] buff2 = new byte[BUFFER_SIZE];
		int dataLen2=0;
		while(connected){
			try {
				dataLen2 = in1.read(buff2);
				if(dataLen2 >= 0){
					out2.write(buff2, 0, dataLen2);
					//Logger.v(TAG, "Reading and forwarding data to receiver " + dataLen2 + " bytes");
				}
				else{
					Logger.w(TAG, "Connection to sender reachs EOF");  
					closeBySocket1();
					break;
				}
			} catch (IOException e) {
				Logger.e(TAG, "buff2: " + e.toString());
				e.printStackTrace();
				if(connected)
					close();
			}
		}
	}
	
	/**
	 * In2 reaches EOF and needs to flush its output and terminate
	 */
	public void closeBySocket2(){
		connected = false;
		try {
			out1.flush();
			out2.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		try {
			in2.close();
			out1.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			out2.close();
			in1.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			if(!tcpSocket2.isClosed())
				tcpSocket2.close();
			if(!tcpSocket1.isClosed())
				tcpSocket1.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		forwardListener.onConnectionClose();
	}
	
	/**
	 * In2 reaches EOF and needs to flush its output and terminate
	 */
	public void closeBySocket1(){
		connected = false;
		try {
			out2.flush();
			out1.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		try {
			in1.close();
			out2.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			out1.close();
			in2.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			if(!tcpSocket1.isClosed())
				tcpSocket1.close();
			if(!tcpSocket2.isClosed())
				tcpSocket2.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		forwardListener.onConnectionClose();
	}
	
	/**
	 * Close unexpectedly
	 */
	public void close(){
		connected = false;
		try {
			if(out1 != null)
				out1.close();
			if(out2 != null)
				out2.close();
			if(in1 != null)
				in1.close();
			if(in2 != null)
				in2.close();
			if(!tcpSocket1.isClosed())
				tcpSocket1.close();
			if(!tcpSocket2.isClosed())
				tcpSocket2.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Used to check the status of TCPForward Thread
	 * @author Jay
	 */
	public static interface TCPForwardListener{
		public void onConnectionClose();
	}
}
