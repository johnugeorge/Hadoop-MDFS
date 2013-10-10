public class MobileDistributedFileSystem extends FileSystem
{
	private Path workingDir;
	private URI uri;

	static
	{
		Configuration.addDefaultResource("hdfs-default.xml");
		Configuration.addDefaultResource("hdfs-site.xml");
	}

	public MobileDistributedFileSystem() {
	}



	public URI getUri() { return uri; }

	public void initialize(URI uri, Configuration conf) throws IOException 
	{
		super.initialize(uri, conf);
		setConf(conf);

		String host = uri.getHost();
		if (host == null) {
			throw new IOException("Incomplete FileSystem URI, no host: "+ uri);
		}

		this.uri = URI.create(uri.getScheme()+"://"+uri.getAuthority());
		this.workingDir = getHomeDirectory();
	}

	private Path makeAbsolute(Path f)
	{
		if (f.isAbsolute()) {
			return f;
		} else {
			return new Path(workingDir, f);
		}
	}

	public Path getWorkingDirectory() 
	{
		return workingDir;
	}


	public void setWorkingDirectory(Path dir)
	{
		String result = makeAbsolute(dir).toUri().getPath();
		if (!DFSUtil.isValidName(result)) {
			throw new IllegalArgumentException("Invalid MDFS directory name " + 
					result);
		}
		workingDir = makeAbsolute(dir);
	}



	private String getPathName(Path file) 
	{
		checkPath(file);
		String result = makeAbsolute(file).toUri().getPath();
		if (!DFSUtil.isValidName(result)) {
			throw new IllegalArgumentException("Pathname " + result + " from " +
					file+" is not a valid MDFS filename.");
		}
		return result;
	}

	@Override
	public short getDefaultReplication() 
	{
		//TODO choose number ofbest locations
		LOG.error(" Not Implemented BlockSize");
		return; 
	}

	@Override
	public long getDefaultBlockSize()
	{
		//TODO choose default block Size
		LOG.error(" Not Implemented BlockSize");
		return;
	}

	@Override
	  public FSDataOutputStream create(Path path, FsPermission permission,
			        boolean overwrite, int bufferSize, short replication, long blockSize,
				      Progressable progress) throws IOException 
	  {
		statistics.incrementWriteOps(1);

		path = makeAbsolute(path);

		boolean exists = exists(path);

		if (progress != null) {
			progress.progress();
		}

		if (exists) {
			if (overwrite)
			{
				//TODO
			}
			else
				throw new FileAlreadyExistsException();
		} else {
			Path parent = path.getParent();
			if (parent != null)
				if (!mkdirs(parent, permission))
					throw new IOException("mkdirs failed for " + parent.toString());
		}

		if (progress != null) {
			progress.progress();
		}
		return new FSDataOutputStream
			(new MDFSOutputStream(getPathName(f), permission, 
				    overwrite, false, replication, blockSize, progress, bufferSize), 
			 statistics);
	}

	@Override
	public boolean mkdirs(Path path, FsPermission perms) throws IOException 
	{
		path = makeAbsolute(path);

		boolean result = false;
		try {
			mdfs.mkdirs(path, (int) perms.toShort());
			result = true;
		} catch (MDFSFileAlreadyExistsException e) {
			result = true;
		}

		return result;
	}

	public boolean delete(Path path, boolean recursive) throws IOException
       	{
		path = makeAbsolute(path);

		/* path exists? */
		FileStatus status;
		try {
			status = getFileStatus(path);
		} catch (FileNotFoundException e) {
			return false;
		}

		/* we're done if its a file */
		if (!status.isDir()) {
			//TODO how to delete file
			mdfs.rmfile(path);
			return true;
		}

		/* get directory contents */
		FileStatus[] dirlist = listStatus(path);
		if (dirlist == null)
			return false;

		if (!recursive && dirlist.length > 0)
			throw new IOException("Directory " + path.toString() + "is not empty.");

		for (FileStatus fs : dirlist) {
			if (!delete(fs.getPath(), recursive))
				return false;
		}

		mdfs.rmdir(path);
		return true;
	}

	public FileStatus getFileStatus(Path path) throws IOException 
	{
		path = makeAbsolute(path);

		MDFSFileStatus stat = new MDFSFileStatus();
		mdfs.lstat(path, stat);

		FileStatus status = new FileStatus(stat.size, stat.isDir(),
				mdfs.get_file_replication(path), stat.blksize, stat.m_time,
				stat.a_time, new FsPermission((short) stat.mode),
				System.getProperty("user.name"), null, path.makeQualified(this));

		return status;
	}
	   

	public FileStatus[] listStatus(Path path) throws IOException
	{
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
	public void setPermission(Path path, FsPermission permission) throws IOException
	{
		path = makeAbsolute(path);
		mdfs.chmod(path, permission.toShort());
	}

	@Override
	public void setTimes(Path path, long mtime, long atime) throws IOException 
	{
		path = makeAbsolute(path);

		MDFSFileStatus stat = new MDFSFileStatus();

		if (mtime != -1) {
			stat.m_time = mtime;
		}

		if (atime != -1) {
			stat.a_time = atime;
		}

		mdfs.setattr(path, stat);
	}

}
