package org.apache.hadoop.mdfs.test;

import org.apache.hadoop.mdfs.MobileDistributedFileSystem;
import org.apache.hadoop.mdfs.MDFSDirectory;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;



class TestMDFS{
	public static void main(String[] args)  throws Exception
	{

		MobileDistributedFileSystem fs = new MobileDistributedFileSystem();
		fs.initialize(URI.create("mdfs://localhost:9000"),null);

		Path path=new Path("/dir0");
		fs.mkdirs(path,FsPermission.getDefault());
		path=new Path("/dir0/data01");
		fs.mkdirs(path,FsPermission.getDefault());
		path=new Path("/dir0/data02");
		fs.mkdirs(path,FsPermission.getDefault());
		path=new Path("/dir0/data01/data012");
		fs.mkdirs(path,FsPermission.getDefault());
		path=new Path("/dir0/data01/data013");
		fs.mkdirs(path,FsPermission.getDefault());
		path=new Path("/dir0/data01");
		fs.mkdirs(path,FsPermission.getDefault());
		path=new Path("/dir1/");
		fs.mkdirs(path,FsPermission.getDefault());
		path=new Path("/dir2");
		fs.mkdirs(path,FsPermission.getDefault());
		path=new Path("/dir1/dir12");
		fs.mkdirs(path,FsPermission.getDefault());
		path=new Path("/dir3/dir31");
		fs.mkdirs(path,FsPermission.getDefault());
		path=new Path("/dir3/dir31/dir311");
		fs.mkdirs(path,FsPermission.getDefault());
		path=new Path("/dir3/File31");
		fs.create(path,FsPermission.getDefault(),false,(int)0,(short)1,(long)0,null);
		path=new Path("/dir3/dir33/File331");
		fs.create(path,FsPermission.getDefault(),false,(int)0,(short)1,(long)0,null);
		path=new Path("/dir3/File32");
		fs.create(path,FsPermission.getDefault(),false,(int)0,(short)1,(long)0,null);
		path=new Path("/dir0/data01/File011");
		fs.create(path,FsPermission.getDefault(),false,(int)0,(short)1,(long)0,null);
		path=new Path("/dir0/File0");
		fs.create(path,FsPermission.getDefault(),false,(int)0,(short)1,(long)0,null);
		path=new Path("/dir3/File0");
		fs.create(path,FsPermission.getDefault(),false,(int)0,(short)1,(long)0,null);
		path=new Path("/dir3/File0");
		fs.create(path,FsPermission.getDefault(),true,(int)0,(short)1,(long)0,null);
		path=new Path("/dir1/");
		fs.delete(path,true);
		path=new Path("/dir5");
		fs.delete(path,true);
		path=new Path("/dir3/dir33/File331");
		fs.delete(path,true);
		path=new Path("/dir0");
		fs.mkdirs(path,FsPermission.getDefault());
		MDFSDirectory.printAllChildrenOfSubtrees();



	}
}
