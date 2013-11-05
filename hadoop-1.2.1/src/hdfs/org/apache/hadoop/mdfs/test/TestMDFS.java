package org.apache.hadoop.mdfs.test;

import org.apache.hadoop.mdfs.MobileDistributedFileSystem;
import org.apache.hadoop.mdfs.protocol.MDFSDirectory;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import java.io.*;
import org.apache.hadoop.io.DataInputBuffer;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.conf.Configuration;


class TestMDFS{
	public static void main(String[] args)  throws Exception
	{

		MobileDistributedFileSystem fs = new MobileDistributedFileSystem();
		Configuration conf = new Configuration();
		conf.set("fs.default.name","mdfs://192.168.1.10:9000");

		fs.initialize(URI.create("mdfs://localhost:9000"),conf);

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
		//MDFSDirectory.printAllChildrenOfSubtrees();
		fs.printTree(new Path("/"),true);
		//path=new Path("/dir0/File0"); // File already exists
		//fs.create(path,FsPermission.getDefault(),false,(int)0,(short)1,(long)0,null);
		path=new Path("/dir1/");
		fs.delete(path,true);
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
		MDFSDirectory.printAllChildrenOfSubtrees();
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
		MDFSDirectory.printAllChildrenOfSubtrees();
		String line=" First String ";
		path=new Path("/dir3/FileTest.txt");
		System.out.println(" going to write");
		OutputStream out=fs.create(path,FsPermission.getDefault(),false,(int)4192,(short)1,(long)4192,null);
		//OutputStream appendout=fs.append(path,4192,null);//append
		BufferedWriter br = new BufferedWriter( new OutputStreamWriter( out, "UTF-8" ) );
		int i=0;
		/*while(i<10000){
		br.write("Hello World");
		br.write(" done ");
		i++;}
		br.close();
		OutputStream appendout1=fs.append(path,4192,null);//append


		line=" Second String ";
		path=new Path("/dir3/FileTest2.txt");
		System.out.println(" going to write");
		out=fs.create(path,FsPermission.getDefault(),false,(int)4192,(short)1,(long)4191,null);
		//OutputStream appendout=fs.append(path,4192,null);//append
		br = new BufferedWriter( new OutputStreamWriter( out, "UTF-8" ) );
		i=0;
		while(i<10000){
		br.write("HelloWorld");
		br.write(" done ");
		i++;}
		br.close();
		appendout1=fs.append(path,4192,null);//append

*/

		line=" Third String ";
		path=new Path("/dir3/FileTest3.txt");
		System.out.println(" going to write");
		out=fs.create(path,FsPermission.getDefault(),false,(int)4192,(short)1,(long)4192 ,null);
		//OutputStream appendout=fs.append(path,4192,null);//append
		br = new BufferedWriter( new OutputStreamWriter( out, "UTF-8" ) );
		i=0;
		while(i<1000){
		br.write("Hello World ");
		br.write(" done ");
		i++;}
		br.close();
		//OutputStream appendout2=fs.append(path,18,null);//append
		//br = new BufferedWriter( new OutputStreamWriter( appendout2, "UTF-8" ) );
		//i=0;
		//while(i<1000){
		//br.write("Append");
		//i++;}
		//br.close();



		System.out.println("Write Successful");
		fs.listStatus(new Path("/"));
		System.out.println(" ");
		fs.listStatus(new Path("/dir3"));
		System.out.println(" ");





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
