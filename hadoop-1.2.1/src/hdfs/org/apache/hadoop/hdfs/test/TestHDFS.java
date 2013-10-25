package org.apache.hadoop.hdfs.test;

import org.apache.hadoop.hdfs.DistributedFileSystem;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import java.io.*;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.conf.Configuration;



class TestHDFS{
	public static void main(String[] args)  throws Exception
	{

		DistributedFileSystem fs = new DistributedFileSystem();
		Configuration conf = new Configuration();
		conf.addResource(new Path("/home/johnugeorge/workspace/Hadoop-mdfs/hadoop-1.2.1/conf/core-site.xml"));
		conf.addResource(new Path("/home/johnugeorge/workspace/Hadoop-mdfs/hadoop-1.2.1/conf/hdfs-site.xml"));
		//FileSystem fs = FileSystem.get(conf);

		fs.initialize(URI.create("hdfs://clusterNode1.local:9000"),conf);

		Path path=new Path("/dir0");
		fs.mkdirs(path,FsPermission.getDefault());
		System.out.println(" Created  dir");
		path=new Path("/dir0/data01");
		fs.mkdirs(path,FsPermission.getDefault());
		System.out.println(" Created ");
		path=new Path("/dir0/data02");
		fs.mkdirs(path,FsPermission.getDefault());
		System.out.println(" Created ");
		path=new Path("/dir0/data01/data012");
		fs.mkdirs(path,FsPermission.getDefault());
		System.out.println(" Created ");
		path=new Path("/dir0/data01/data013");
		fs.mkdirs(path,FsPermission.getDefault());
		//path=new Path("/dir0/data01");        //dir already exists
		//fs.mkdirs(path,FsPermission.getDefault());
		path=new Path("/dir1/");
		fs.mkdirs(path,FsPermission.getDefault());
		path=new Path("/dir2");
		fs.mkdirs(path,FsPermission.getDefault());
		path=new Path("/dir6");
		fs.mkdirs(path,FsPermission.getDefault());
		path=new Path("/dir7");
		fs.mkdirs(path,FsPermission.getDefault());
		path=new Path("/dir1/dir12");
		fs.mkdirs(path,FsPermission.getDefault());
		path=new Path("/dir3/dir31");
		fs.mkdirs(path,FsPermission.getDefault());
		path=new Path("/dir4/dir41");
		fs.mkdirs(path,FsPermission.getDefault());
		path=new Path("/dir3/dir31/dir311");
		fs.mkdirs(path,FsPermission.getDefault());
		path=new Path("/dir3/File31");
		fs.create(path,FsPermission.getDefault(),false,(int)512,(short)1,(long)4*1024*1024,null);
		System.out.println(" Created  File");
		path=new Path("/dir3/dir33/File331");
		fs.create(path,FsPermission.getDefault(),false,(int)0,(short)1,(long)0,null);
		System.out.println(" Created  File");
		path=new Path("/dir3/dir33/File332");
		fs.create(path,FsPermission.getDefault(),false,(int)0,(short)1,(long)0,null);
		path=new Path("/dir3/File32");
		System.out.println(" Created  File");
		fs.create(path,FsPermission.getDefault(),false,(int)0,(short)1,(long)0,null);
		path=new Path("/dir0/data01/File011");
		System.out.println(" Created  File");
		fs.create(path,FsPermission.getDefault(),false,(int)0,(short)1,(long)0,null);
		path=new Path("/dir0/File0");
		fs.create(path,FsPermission.getDefault(),false,(int)0,(short)1,(long)0,null);
		path=new Path("/dir3/File33");
		fs.create(path,FsPermission.getDefault(),false,(int)0,(short)1,(long)0,null);
		System.out.println(" Created  File");
		//path=new Path("/dir3/File33");//overwrite=true
		//fs.create(path,FsPermission.getDefault(),true,(int)0,(short)1,(long)0,null);
		path=new Path("/dir7/File71");
		fs.create(path,FsPermission.getDefault(),false,(int)0,(short)1,(long)0,null);
		System.out.println(" Created  File");
		//MDFSDirectory.printAllChildrenOfSubtrees();
		//path=new Path("/dir0/File0"); // File already exists
		//fs.create(path,FsPermission.getDefault(),false,(int)0,(short)1,(long)0,null);
		path=new Path("/dir1/");
		fs.delete(path,true);
		System.out.println(" Deleted ");
		path=new Path("/dir2/");
		fs.delete(path,true);
		path=new Path("/dir6/");
		fs.delete(path,false);
		//path=new Path("/dir5"); //dir not exist
		//fs.delete(path,true);
		path=new Path("/dir3/dir33/File331");
		fs.delete(path,true);
		path=new Path("/dir3/dir33/File332");
		fs.delete(path,false);
		path=new Path("/dir7/File71");
		fs.delete(path,false);
		path=new Path("/dir7/");
		fs.delete(path,false);
		//path=new Path("/dir3/");//directory not empty
		//fs.delete(path,false);
		//path=new Path("/dir0"); //dir already exist
		//fs.mkdirs(path,FsPermission.getDefault());
		//MDFSDirectory.printAllChildrenOfSubtrees();
		fs.listStatus(new Path("/"));
		System.out.println(" ");
		fs.listStatus(new Path("/dir3"));
		System.out.println(" ");
		fs.rename(new Path("/dir3/File33"),new Path("/dir3/File33_renamed"));
		//fs.rename(new Path("/dir3/File33"),new Path("/dir3/File33_new"));//no src file
		fs.rename(new Path("/dir3/File33_renamed"),new Path("/"));
		//fs.rename(new Path("/File33_renamed"),new Path("/"));//src is same as destination
		fs.rename(new Path("/File33_renamed"),new Path("/dir0"));
		fs.rename(new Path("/dir0/File33_renamed"),new Path("/dir3/File33_new"));
		//fs.printTree(new Path("/"), true);
		fs.rename(new Path("/dir3/File33_new"),new Path("/File7"));//dir doesn't exist
		//fs.rename(new Path("/dir0/File0"),new Path("/dir8/File0_new"));//destination parent dir doesn't exist
		fs.rename(new Path("/File7"),new Path("/dir3/"));
		fs.rename(new Path("/dir3"),new Path("/dir8/"));
		fs.rename(new Path("/dir8"),new Path("/dir3/"));
		System.out.println(" Renamed ");



	}

	static void printFileStatus (FileStatus fileStatus)throws IOException,UnsupportedEncodingException
	{
		ByteArrayOutputStream buffer = new ByteArrayOutputStream(64);
		DataInputBuffer in = new DataInputBuffer();

		buffer.reset();
		DataOutputStream out = new DataOutputStream(buffer);
		fileStatus.write(out);

		in.reset(buffer.toByteArray(), 0, buffer.size());

	}
}
