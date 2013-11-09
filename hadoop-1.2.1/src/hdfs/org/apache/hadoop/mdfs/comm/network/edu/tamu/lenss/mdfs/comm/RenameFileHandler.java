package edu.tamu.lenss.mdfs.comm;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Set;

import adhoc.etc.IOUtilities;
import edu.tamu.lenss.mdfs.Constants;
import edu.tamu.lenss.mdfs.MDFSDirectory;
import edu.tamu.lenss.mdfs.models.RenameFile;
import edu.tamu.lenss.mdfs.models.MDFSFileInfo;
import edu.tamu.lenss.mdfs.utils.AndroidIOUtils;

public class RenameFileHandler {
	
	public RenameFileHandler(){
	}

	public void processPacket(RenameFile rename){
		if(rename.getSrcFileName() == null || rename.getDestFileName() == null){
			System.out.println(" Src is null or dest is null src: "+rename.getSrcFileName() + " dest: " + rename.getDestFileName());
			return;
		}
		new RenameFileThread(rename.getSrcFileName(),rename.getDestFileName(),rename.getFileIds()).start();
	}

	public void sendFileRenamePacket(RenameFile rename){
		ServiceHelper.getInstance().getMyNode().sendAODVDataContainer(rename);
	}

	private class RenameFileThread extends Thread{
		private String src;
		private String dest;
		private List<Long> blockIds;


		private RenameFileThread(String src,String dest, List<Long> fIds){
			this.blockIds = fIds;
			this.src =src;
			this.dest = dest;
		}

		@Override
		public void run() {
			File rootDir = AndroidIOUtils.getExternalFile(Constants.DIR_ROOT);
			if(!rootDir.exists())
				return;


			for (int i = 0; i < blockIds.size(); i++) {
				String srcFileName=src+"/Blocks/Block-"+(new Long(blockIds.get(i))).toString();
				String destFileName=dest+"/Blocks/Block-"+(new Long(blockIds.get(i))).toString();
				long srcFileId = srcFileName.hashCode();
				long destFileId = destFileName.hashCode();
				System.out.println(" Rename Thread: "+srcFileName+" srcFileId "+srcFileId+" destFileName"+destFileName +" destFileId" +destFileId+" blockIds size "+ blockIds.size());


				File[] listOfFiles = rootDir.listFiles();

				for (int j = 0; j < listOfFiles.length; j++) {
					File file = listOfFiles[j];
					String fileName = file.getName();
					int index= fileName.indexOf(new Long(srcFileId).toString()) ;
					if(index != -1){

						System.out.println(" Src hashCode "+srcFileId+" matched in fileName "+fileName);
						String dest= fileName.substring(0,index)+ destFileId;
						System.out.println(" dest FileName "+destFileName);
						file.renameTo(new File(file.getParentFile(),dest));




						// Clean up the MDFSDirectory
						MDFSDirectory directory = ServiceHelper.getInstance().getDirectory();
						directory.removeDecryptedFile(srcFileId);
						directory.removeEncryptedFile(srcFileId);


						MDFSFileInfo fileInfo = new MDFSFileInfo(destFileName, destFileId ,true);
						directory.removeFile(srcFileId);
						directory.addFile(fileInfo);

						Set<Integer> filefrags= directory.getStoredFileIndex(srcFileId);
						directory.removeFileFragment(srcFileId);
						directory.addFileFragment(destFileId, filefrags);

						int keyfrag = directory.getStoredKeyIndex(srcFileId);
						directory.removeKeyFragment(srcFileId);
						directory.addKeyFragment(destFileId,keyfrag);

						// Remove Encrypted File
						file = new File(rootDir, "encrypted/" + MDFSFileInfo.getDirName(srcFileName, srcFileId));
						if(file.exists())
							file.delete();

						// Remove Decrypted File
						file = new File(rootDir, "decrypted/" + MDFSFileInfo.getDirName(srcFileName, srcFileId));
						if(file.exists())
							file.delete();
					}

				}
			}
			System.out.println(" Src "+src + " is renamed to dest "+ dest);

		}
	}
}
