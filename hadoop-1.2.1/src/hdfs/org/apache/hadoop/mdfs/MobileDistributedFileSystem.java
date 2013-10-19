package org.apache.hadoop.mdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mdfs.DFSUtil;
import org.apache.hadoop.mdfs.MountFlags;
import org.apache.hadoop.util.Progressable;



public class MobileDistributedFileSystem extends FileSystem{
	private Path workingDir;
	private URI uri;
	private MDFSClient mdfs;

	static{
		Configuration.addDefaultResource("hdfs-default.xml");
		Configuration.addDefaultResource("hdfs-site.xml");
	}

	public MobileDistributedFileSystem() {
	}



	public URI getUri() { return uri; }

	public void initialize(URI uri, Configuration conf) throws IOException{
		super.initialize(uri, conf);


		if (mdfs == null) {
			mdfs = new MDFSClient(conf);
			mdfs.initialize(uri,conf);
		}
		setConf(conf);

		String host = uri.getHost();
		if (host == null) {
			throw new IOException("Incomplete FileSystem URI, no host: "+ uri);
		}

		this.uri = URI.create(uri.getScheme()+"://"+uri.getAuthority());
		this.workingDir = getHomeDirectory();

	}

	private Path makeAbsolute(Path f){
		if (f.isAbsolute()) {
			return f;
		} else {
			return new Path(workingDir, f);
		}
	}

	public Path getWorkingDirectory(){
		return workingDir;
	}


	public void setWorkingDirectory(Path dir){
		String result = makeAbsolute(dir).toUri().getPath();
		if (!DFSUtil.isValidName(result)) {
			throw new IllegalArgumentException("Invalid MDFS directory name " + 
					result);
		}
		workingDir = makeAbsolute(dir);
	}



	private String getPathName(Path file){
		checkPath(file);
		String result = makeAbsolute(file).toUri().getPath();
		if (!DFSUtil.isValidName(result)) {
			throw new IllegalArgumentException("Pathname " + result + " from " +
					file+" is not a valid MDFS filename.");
		}
		return result;
	}

	@Override
	public short getDefaultReplication(){
		//TODO choose number ofbest locations
		LOG.error(" Not Implemented Block replication");
		return 3; 
	}

	@Override
	public long getDefaultBlockSize(){
		//TODO choose default block Size
		LOG.error(" Not Implemented BlockSize");
		return 4*1024*1024;
	}

	public FSDataInputStream open(Path path, int bufferSize) throws IOException {
		path = makeAbsolute(path);


		/* get file size */
		MDFSFileStatus stat = mdfs.lstat(path);

//		MDFSInputStream istream = new MDFSInputStream(
//				stat.getLen(), bufferSize);
//		return new FSDataInputStream(istream);
		return null;
	}



	@Override
	  public FSDataOutputStream create(Path path, FsPermission permission,
			        boolean overwrite, int bufferSize, short replication, long blockSize,
				      Progressable progress) throws IOException {
		statistics.incrementWriteOps(1);

		path = makeAbsolute(path);

		boolean exists = exists(path);
		int flags = MountFlags.O_WRONLY.getValue() | MountFlags.O_CREAT.getValue(); 

		if (progress != null) {
			progress.progress();
		}


		if (exists) {
			if (overwrite){
				System.out.println(" Overwriting the existing path "+path.toString());
				flags = flags|MountFlags.O_TRUNCAT.getValue();
			}
			else{
				throw new IOException("File Already Exists");
			}
		} 

		if (progress != null) {
			progress.progress();
		}

		return new FSDataOutputStream
			(mdfs.create(path, flags,permission, 
				     true, replication, blockSize, progress, bufferSize), 
			 statistics);
	}


	public FSDataOutputStream append(Path path, int bufferSize,
			Progressable progress) throws IOException {
		path = makeAbsolute(path);

		if (progress != null) {
			progress.progress();
		}
		int flags = MountFlags.O_WRONLY.getValue() |MountFlags.O_APPEND.getValue();
		
		if (progress != null) {
			progress.progress();
		}

		MDFSFileStatus stat =mdfs.lstat(path);

		return new FSDataOutputStream
			(mdfs.create(path,flags,null,false,stat.getReplication(),stat.getBlockSize(),progress, bufferSize),
			 statistics);
	}

	@Override
	public boolean mkdirs(Path path, FsPermission perms) throws IOException {
		path = makeAbsolute(path);

		boolean result = false;
		mdfs.mkdirs(path, perms);
		result = true;

		return result;
	}
	
	public boolean delete(Path path) throws IOException {
		return delete(path, false);
	}

	public boolean delete(Path path, boolean recursive) throws IOException {
		path = makeAbsolute(path);

		/* path exists? */
		FileStatus status;
		try {
			status = getFileStatus(path);
		} catch (FileNotFoundException e) {
			System.out.println(" Not deleting the file as path doesn't exist "+path.toString());
			return false;
		}

		/* we're done if its a file */
		if (!status.isDir()) {
			//TODO how to delete file
			mdfs.rmdir(path,false);
			return true;
		}

		/* get directory contents */
		FileStatus[] dirlist = listStatus(path);

		if (!recursive && dirlist.length > 0)
			throw new IOException("Directory " + path.toString() + "is not empty.");

		for (FileStatus fs : dirlist) {
			if (!delete(fs.getPath(), recursive))
				return false;
		}

		mdfs.rmdir(path,false);
		return true;
	}

	public FileStatus getFileStatus(Path path) throws IOException {
		path = makeAbsolute(path);

		MDFSFileStatus stat= mdfs.lstat(path);

		if(stat == null){
			throw new FileNotFoundException();
		}

		FileStatus status = new FileStatus(stat.getLen(), stat.isDir(),
				stat.getReplication(), stat.getBlockSize(), stat.getModificationTime(),
				stat.getAccessTime(), new FsPermission(stat.getPermission().toShort()),
				System.getProperty("user.name"), null, path.makeQualified(this));

		return status;
	}
	   

	public FileStatus[] listStatus(Path path) throws IOException{
		path = makeAbsolute(path);

		String[] dirlist = mdfs.listdir(path);
		if (dirlist != null) {
			FileStatus[] status = new FileStatus[dirlist.length];
			for (int i = 0; i < status.length; i++) {
				status[i] = getFileStatus(new Path(path, dirlist[i]));
			}
			return status;
		}

		if (isFile(path))
			return new FileStatus[] { getFileStatus(path) };

		return null;
	}

	@Override
	public void setPermission(Path path, FsPermission permission) throws IOException {
		path = makeAbsolute(path);
		mdfs.chmod(path, permission.toShort());
	}

	@Override
	public void setTimes(Path path, long mtime, long atime) throws IOException {
		path = makeAbsolute(path);
		int mask=0;//TODO modify mask

		MDFSFileStatus stat = mdfs.lstat(path);

		mdfs.setattr(path, stat,mask);
	}


	@Override
	public boolean rename(Path src, Path dst) throws IOException {
		src = makeAbsolute(src);
		dst = makeAbsolute(dst);

		try {
			MDFSFileStatus stat = mdfs.lstat(src);

			if (stat.isDir())
				return rename(src, new Path(dst, src.getName()));
			return false;
		} catch (FileNotFoundException e) {}

		try {
			mdfs.rename(src, dst);
		} catch (FileNotFoundException e) {
			return false;
		}

		return true;
	}

	@Override
	public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
		Path path = makeAbsolute(file.getPath());


		/* Get block size */
		MDFSFileStatus stat = mdfs.lstat(path);
		long blockSize = stat.getBlockSize();

		BlockLocation[] locations = new BlockLocation[(int) Math.ceil(len / (float) blockSize)];

		for (int i = 0; i < locations.length; ++i) {
			long offset = start + i * blockSize;
			long blockStart = start + i * blockSize - (start % blockSize);
			locations[i] = new BlockLocation(null, null, blockStart, blockSize);
			LOG.debug("getFileBlockLocations: location[" + i + "]: " + locations[i]);
		}

		return locations;
	}

}
