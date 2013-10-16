package org.apache.hadoop.mdfs;

import java.io.IOException;
import java.net.URI;
import java.io.FileNotFoundException;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.lang.StringUtils;

class MDFSClient extends AbstractMDFS {

	private short defaultReplication;

	public MDFSClient(Configuration conf) {
	}

	private String pathString(Path path) {
		return path.toUri().getPath();
	}

	void initialize(URI uri, Configuration conf) throws IOException {

	}

	
	int open(Path path, int flags, int mode) throws IOException {
	}



	void lstat(Path path, MDFSFileStatus stat) throws IOException {
	}

	void rmdir(Path path, boolean isDir) throws IOException {
	}


	void rename(Path src, Path dst) throws IOException {
	}

	String[] listdir(Path path) throws IOException {
		return null ;
	}

	void mkdirs(Path path, int mode) throws IOException {
	}

	void close(int fd) throws IOException {
	}

	void chmod(Path path, int mode) throws IOException {
	}

	void shutdown() throws IOException {
	}

	short getDefaultReplication() {
		return defaultReplication;
	}

	void setattr(Path path, MDFSFileStatus stat, int mask) throws IOException {
	}

	long lseek(int fd, long offset, int whence) throws IOException {
		return 0 ;
	}

	int write(int fd, byte[] buf, long size, long offset) throws IOException {
		return 0 ;
	}

	int read(int fd, byte[] buf, long size, long offset) throws IOException {
		return 0 ;
	}
}
