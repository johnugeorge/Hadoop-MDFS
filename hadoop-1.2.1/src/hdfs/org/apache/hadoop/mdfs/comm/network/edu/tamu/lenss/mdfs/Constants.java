package edu.tamu.lenss.mdfs;

public final class Constants {
	//public static final String IP_PREFIX = "192.168.2.";
	//public static final String IP_PREFIX = "192.168.0.";
	public static final long TOPOLOGY_DISCOVERY_TIMEOUT = 6500;
	public static final long FRAGMENT_DISTRIBUTION_INTERVAL = 30000;	// Timeout of sending file fragments 	
	public static final long FILE_REQUEST_TIMEOUT = 6500;
	public static final long TOPOLOGY_REBROADCAST_THRESHOLD = 1000;	// Time between each topology discovery request
	public static final long DIRECTORY_LIST_REFRESH_PERIOD = 2500;
	public static final long TOPOLOGY_CACHE_EXPIRY_TIME = 1000 *60*10;//After 10 minutes, cached topology info expires
	public static final int TOPOLOGY_DISCOVERY_MAX_RETRIES =5;
	
	public static final double KEY_STORAGE_RATIO=1;
	public static final double FILE_STORAGE_RATIO=1;
	public static final double KEY_CODING_RATIO = 1;
	public static final double FILE_CODING_RATIO = 1;
	
	//public static final String DIR_ROOT = "MDFS";
	public static final String MDFS_HADOOP_DATA_DIR = "/tmp/MDFS/hadoop";
	public static final String DIR_ROOT = MDFS_HADOOP_DATA_DIR;
	public static final String DIR_CACHE = DIR_ROOT + "/cache";
	public static final String DIR_DECRYPTED = DIR_ROOT + "/decrypted";
	public static final String DIR_ENCRYPTED = DIR_ROOT + "/encrypted";
	
	
	public static final String NAME_MDFS_DIRECTORY = "mdfs_directory";
	
	public static final int TCP_COMM_BUFFER_SIZE = 128;
	
}
