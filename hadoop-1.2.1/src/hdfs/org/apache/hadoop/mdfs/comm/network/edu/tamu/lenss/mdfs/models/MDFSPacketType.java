package edu.tamu.lenss.mdfs.models;

public final class MDFSPacketType {
	public static final int TOPOLOGY_DISCOVERY = 1;
	public static final int NODE_INFO = 2;
	public static final int KEY_FRAG_PACKET = 3;
	public static final int FILE_REQ = 4;
	public static final int FILE_REP = 5;
	public static final int NEW_FILE_UPDATE = 6;
	public static final int DELETE_FILE = 7;
	public static final int JOB_SCHEDULE = 8;
	public static final int JOB_COMPLETE = 9;
	public static final int JOB_RESULT = 10;
	public static final int RENAME_FILE = 11;
}
