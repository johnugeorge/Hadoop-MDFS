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
		fs.create(path,FsPermission.getDefault(),false,(int)0,(short)1,(long)0,null);
		path=new Path("/dir3/dir33/File331");
		fs.create(path,FsPermission.getDefault(),false,(int)0,(short)1,(long)0,null);
		path=new Path("/dir3/dir33/File332");
		fs.create(path,FsPermission.getDefault(),false,(int)0,(short)1,(long)0,null);
		path=new Path("/dir3/File32");
		fs.create(path,FsPermission.getDefault(),false,(int)0,(short)1,(long)0,null);
		path=new Path("/dir0/data01/File011");
		fs.create(path,FsPermission.getDefault(),false,(int)0,(short)1,(long)0,null);
		path=new Path("/dir0/File0");
		fs.create(path,FsPermission.getDefault(),false,(int)0,(short)1,(long)0,null);
		path=new Path("/dir3/File33");
		fs.create(path,FsPermission.getDefault(),false,(int)0,(short)1,(long)0,null);
		path=new Path("/dir3/File33");//overwrite=true
		fs.create(path,FsPermission.getDefault(),true,(int)0,(short)1,(long)0,null);
		path=new Path("/dir7/File71");
		fs.create(path,FsPermission.getDefault(),false,(int)0,(short)1,(long)0,null);
		MDFSDirectory.printAllChildrenOfSubtrees();
		//path=new Path("/dir0/File0"); // File already exists
		//fs.create(path,FsPermission.getDefault(),false,(int)0,(short)1,(long)0,null);
		path=new Path("/dir1/");
		fs.delete(path,true);
		MDFSDirectory.printAllChildrenOfSubtrees();
		path=new Path("/dir2/");
		fs.delete(path,true);
		MDFSDirectory.printAllChildrenOfSubtrees();
		path=new Path("/dir6/");
		fs.delete(path,false);
		MDFSDirectory.printAllChildrenOfSubtrees();
		//path=new Path("/dir5"); //dir not exist
		//fs.delete(path,true);
		path=new Path("/dir3/dir33/File331");
		fs.delete(path,true);
		MDFSDirectory.printAllChildrenOfSubtrees();
		path=new Path("/dir3/dir33/File332");
		fs.delete(path,false);
		path=new Path("/dir7/File71");
		fs.delete(path,false);
		path=new Path("/dir7/");//directory not empty
		fs.delete(path,false);
		//path=new Path("/dir3/");//directory not empty
		//fs.delete(path,false);
		//path=new Path("/dir0"); //dir already exist
		//fs.mkdirs(path,FsPermission.getDefault());
		MDFSDirectory.printAllChildrenOfSubtrees();



	}
}
