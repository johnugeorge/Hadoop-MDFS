package org.apache.hadoop.mdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mdfs.MDFSFileStatus;

abstract class AbstractMDFS {

	abstract void initialize(URI uri, Configuration conf) throws IOException;
	abstract int open(Path path, int flags, int mode) throws IOException;
	abstract void lstat(Path path, MDFSFileStatus stat) throws IOException;
	abstract void rmdir(Path path, boolean isDir) throws IOException;
	abstract String[] listdir(Path path) throws IOException;
	abstract void setattr(Path path, MDFSFileStatus stat, int mask) throws IOException;
	abstract void chmod(Path path, int mode) throws IOException;
	abstract long lseek(int fd, long offset, int whence) throws IOException;
	abstract void close(int fd) throws IOException;
	abstract void shutdown() throws IOException;
	abstract void rename(Path src, Path dst) throws IOException;
	abstract int write(int fd, byte[] buf, long size, long offset) throws IOException;
	abstract int read(int fd, byte[] buf, long size, long offset) throws IOException;
	abstract void mkdirs(Path path, int mode) throws IOException;
}
