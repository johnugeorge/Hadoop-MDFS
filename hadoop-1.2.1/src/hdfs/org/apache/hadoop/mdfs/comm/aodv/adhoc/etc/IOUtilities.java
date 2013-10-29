package adhoc.etc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;


public final class IOUtilities {
	private static final String TAG = "IOUtilities";
	
	public static final int IO_BUFFER_SIZE = 8 * 1024;
	
	
	 /**
     * Copy the content of the input stream into the output stream, using a temporary
     * byte array buffer whose size is defined by {@link #IO_BUFFER_SIZE}.
     *
     * @param in The input stream to copy from.
     * @param out The output stream to copy to.
     *
     * @throws java.io.IOException If any error occurs during the copy.
     */
	
	public static void copy(InputStream in, OutputStream out) throws IOException {
		byte[] b = new byte[IO_BUFFER_SIZE];
		int read;
		while ((read = in.read()) != -1) {
			out.write(b, 0, read);
		}
	}
	
	/**
     * Closes the specified stream.
     *
     * @param stream The stream to close.
     */
	public static void closeStream(Closeable stream) {
		if (stream != null) {
			try {
				stream.close();
			} catch (IOException e) {
				Logger.e(TAG, "Could not close stream");
			}
		}
	}
	
	public static String streamToString(java.io.InputStream is) {		
		java.io.DataInputStream din = new java.io.DataInputStream(is);
		StringBuffer sb = new StringBuffer();
		try {
			String line = null;
			while ((line = din.readLine()) != null) {
				sb.append(line + "\n");
			}
			
		} catch (Exception ex) {
			ex.getMessage();
		} finally {
			try {
				is.close();
			} catch (Exception ex) {
			}
		}
		return sb.toString();
	}
	
	/**
	 * 
	 * @param file	A valid file handler.
	 * @param inputStream
	 */
	public static void streamToFile(File file, InputStream inputStream){
		// write the inputStream to a FileOutputStream
		try {
			OutputStream out = new FileOutputStream(file);
			int read = 0;
			byte[] bytes = new byte[1024];
			while ((read = inputStream.read(bytes)) != -1) {
				out.write(bytes, 0, read);
			}
		 
			inputStream.close();
			out.flush();
			out.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			Logger.e(TAG, e.toString());
		} catch (IOException e) {
			e.printStackTrace();
			Logger.e(TAG, e.toString());
	    }
		
	}
	
	/**
	 * Create a new file and directory in the specified directory <br>
	 * Return the file handler if the file already exists <br>
	 * Create one if it does not exist
	 */
	public static File createNewFile(File dir, String name){
		if(dir == null || MyTextUtils.isEmpty(name))
			return null;
		
		if( !dir.exists()){
			if(!dir.mkdirs()){
				Logger.e(TAG, "Fail to create directory");
				return null;
			}
		}
		
		File f = new File(dir, name);
		if(f.exists())
			return f;
		else{
			try{
				if(f.createNewFile())
					return f;
			}catch(IOException e){
				Logger.e(TAG, e.toString());
				f = null;
			}
		}
		return f;
	}
	
	
	/**
	 * Get the local IP Address
	 * @return null if the IP is not available
	 */
	public static String getLocalIpAddress() {
		try {
			for (Enumeration<NetworkInterface> en = NetworkInterface
					.getNetworkInterfaces(); en.hasMoreElements();) {
				NetworkInterface intf = en.nextElement();
				for (Enumeration<InetAddress> enumIpAddr = intf
						.getInetAddresses(); enumIpAddr.hasMoreElements();) {
					InetAddress inetAddress = enumIpAddr.nextElement();
					// Ignore IPv6 address.
					if (!inetAddress.isLoopbackAddress() && !inetAddress.isAnyLocalAddress() && !inetAddress.isLinkLocalAddress()) {
						//Logger.v(TAG, "Address: " + inetAddress.getHostAddress().toString());
						return inetAddress.getHostAddress().toString();						
					}
				}
			}
		} catch (SocketException ex) {
			Logger.e("ServerActivity", ex.toString());
		}
		return null;
	}
	
	/**
	 * Parse the last part of the IP and return as an int
	 * @param IP
	 * @return	-1 if fails to parse the IP
	 */
	public static int parseNodeNumber(String IP){
		try{
			//int id = Integer.parseInt(IP.substring(IP.lastIndexOf(".")+1));
			return Integer.parseInt(IP.substring(IP.lastIndexOf(".")+1));
		}
		catch(NumberFormatException e){
			Logger.e(TAG, "Fail to parse the IP. " + e.toString());
			return -1;
		}
	}
	
	/**
	 * Parse the first 3 parts of the IP address. Including the last dot, . 
	 * @param IP
	 * @return	-1 if fails to parse the IP
	 */
	public static String parsePrefix(String IP){
		try{
			//String prefix = IP.substring(0,IP.lastIndexOf("."));
			return IP.substring(0,IP.lastIndexOf(".")+1);
		}
		catch(IndexOutOfBoundsException e){
			Logger.e(TAG, "Fail to parse the IP prefix. " + e.toString());
			return null;
		}
	}
	
	/**
	 * Convert byte array to a file
	 * @param data
	 * @param name
	 * @return
	 */
	public static File byteToFile(byte[] data, File dir, String name){
		
		File f = createNewFile(dir, name);
		try {
			FileOutputStream fos = new FileOutputStream(f);
			fos.write(data);
			fos.close();
		} catch (FileNotFoundException e) {
			Logger.e(TAG, e.toString());
			return null;
		} catch (IOException e) {
			Logger.e(TAG, e.toString());
			return null;
		}
		return f;
	}
	/**
	 * Convert a File to a byte array
	 * @param file
	 * @return null if the conversion fails
	 */
	public static byte[] fileToByte(File file){
		try {
			RandomAccessFile randF = new RandomAccessFile(file, "r");
			byte[] b = new byte[(int)randF.length()];
			randF.read(b);
			return b;

		} catch (FileNotFoundException e) {
			Logger.e(TAG, e.toString());
		} catch (IOException e) {
			Logger.e(TAG, e.toString());
		}
		return null;
	}
	
	/**
	 * Covert a Serializable Object to a byte array
	 * @param o
	 * @return
	 */
	public static <T extends Object> byte[] objectToByteArray(T object){
		ByteArrayOutputStream byteStr = new ByteArrayOutputStream(10240);
		try {
			ObjectOutputStream output = new ObjectOutputStream(byteStr);
			output.writeObject(object);
			byte[] byteData = byteStr.toByteArray();
			byteStr.close();
			output.close();
			return byteData;
		} catch (IOException e) {
			Logger.e(TAG, e.toString());
			return null;
		}
	}
	
	/**
	 * Convert a bytes array to an Serializable Object
	 * @param bytes
	 * @return
	 */
	public static <T extends Object> T bytesToObject(byte[] packetData, Class<T> type){
		T packet=null;
		try {
			ByteArrayInputStream byteStr = new ByteArrayInputStream(packetData);
			ObjectInputStream input = new ObjectInputStream(byteStr);
			packet = type.cast(input.readObject());
			
		} catch (OptionalDataException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch(ClassCastException e){
			e.printStackTrace();
		}
		return packet;
	}
	
	/**
	 * Write a serializable object to a file
	 * @param <T>
	 * @param object
	 * @param fileHandle
	 * @return	true if the file is written successfully
	 */
	public static <T extends Object> boolean writeObjectToFile(T object, File fileHandle){
		try {
			FileOutputStream fos = new FileOutputStream(fileHandle);
			ObjectOutputStream output = new ObjectOutputStream(fos);
			output.writeObject(object);
			output.close();
			fos.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	/**
	 * Read an Object from a file
	 * @param file
	 * @param type
	 * @return	null if the file does not exist or the conversion process fails
	 */
	public static <T extends Object> T readObjectFromFile(File file, Class<T> type){
		if(!file.exists())
			return null;
		T object=null;
		try {
			FileInputStream fis = new FileInputStream(file);
			ObjectInputStream input = new ObjectInputStream(fis);
			object = type.cast(input.readObject());
			input.close();
			fis.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (StreamCorruptedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return object;
	}
	
	
	
	/**
	 * Clean up the files that are more than pastTime old in the specify
	 * directory <br>
	 * Set pastTime to 0 will remove all the files in the directory
	 * 
	 * @param directory
	 *            The path of the directory that need to be cleaned up
	 * @param pastTime
	 *            The time in milli-seconds.
	 * @throws FileNotFoundException
	 */
	public static void cleanCache(File directory, long pastTime)
			throws FileNotFoundException {
		final long timePoint = System.currentTimeMillis() - pastTime;
		if (directory == null || !directory.exists()) {
			throw new FileNotFoundException("Directory Do Not Exist! "+directory.getName());
		} else if (!directory.isDirectory()) {
			throw new FileNotFoundException("The path is not a valid directory");
		}
		FileFilter fileFilter = new FileFilter() {
			public boolean accept(File file) {
				if (!file.isDirectory()) {
					if (file.lastModified() < timePoint)
						return true;
				}
				return false;
			}
		};
		File[] files = directory.listFiles(fileFilter);
		for (File f : files) {
			f.delete();
		}
	}
	
	public static void deleteRecursively(File f) throws IOException {
		if (f.isDirectory()) {
			for (File c : f.listFiles())
				deleteRecursively(c);
		}
		if (!f.delete())
			throw new FileNotFoundException("Failed to delete file: " + f);
	}

	public static boolean deleteFile(String filepath) {
		if (filepath == null) {
			return false;
		}
		File file = new File(filepath);
		try {
			boolean deleted = file.delete();
			if (deleted) {
				return true;
			}
		} catch (SecurityException exception) {

		}
		return false;
	}
}
