package rice.p2p.projecto;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.NodeHandle;
import rice.pastry.commonapi.PastryIdFactory;

/**
 * 
 * @author GUREYA
 *
 */

public class FileOperations {

	private static PastryIdFactory pastryIdf;
	private static ContentHandler contentHandler;
	
	//storing all the Retrieved files
	private static HashMap<String, byte[]> returnedFiles = new HashMap<String, byte[]>();

	public static void setPastryIdFactory(PastryIdFactory idf) {
		pastryIdf = idf;
	}

	public static void setContentHandler(ContentHandler ct) {
		contentHandler = ct;
	}

	public static void sendData(String hashKey, byte[] data) {
		Id hashId = pastryIdf.buildIdFromToString(hashKey);
		contentHandler.sendData(hashId, data);
	}

	public static Id regenerateID(String hashKey) {
		return pastryIdf.buildIdFromToString(hashKey);
	}

	public static String generateHashKey(String rawKey) {
		return pastryIdf.buildId(rawKey).toString();
	}

	public static void RequestFile(String uniqVal, String[] hashKey) {
		for(int j=0; j<hashKey.length;++j)
		{
			System.out.println("Sending the following hashes for retrieval: " + hashKey[j]);
		}
		contentHandler.getFile(uniqVal, hashKey);
	}

	public static boolean fileAvailable() {
		boolean isAvailable = false;
		if (!returnedFiles.isEmpty())
			isAvailable = true;
		return isAvailable;
	}

	public static void addFiles(String uniqKey, byte[] data) {
		synchronized (returnedFiles) {
			returnedFiles.put(uniqKey, data);
			System.out
					.println("Retrieving file...");
		}
	}

	public static ReturnedFile[] getReturnedFiles() {
		synchronized (returnedFiles) {
			ArrayList<ReturnedFile> files = new ArrayList<ReturnedFile>();
			ArrayList<String> uniqKeys = new ArrayList<String>();
			ReturnedFile tempFile;
			for (Entry<String, byte[]> item : returnedFiles.entrySet()) {
				tempFile = new ReturnedFile();
				tempFile.setUniqKey(item.getKey());
				tempFile.setContent(item.getValue());
				files.add(tempFile);
				uniqKeys.add(item.getKey());
			}
			removeReturnedFilesList(uniqKeys.toArray(new String[uniqKeys
					.size()]));
			System.out
					.println("File has been retrieved....");
			return files.toArray(new ReturnedFile[files.size()]);
		}
	}

	public static void removeReturnedFilesList(String[] uniqKeys) {
		for (String uniqKey : uniqKeys) {
			if (returnedFiles.containsKey(uniqKey)) {
				returnedFiles.remove(uniqKey);
			}
		}
	}
}

class ReturnedFile {
	private String uniqKey;
	private byte[] content;

	public void setUniqKey(String uniqKey) {
		this.uniqKey = uniqKey;
	}

	public void setContent(byte[] content) {
		this.content = content;
	}

	public String getUniqKey() {
		return uniqKey;
	}

	public byte[] getContent() {
		return content;
	}
}

class FileTransferThread extends Thread {

	FileTransferApp filetransfer;
	String filepath;
	NodeHandle nh;

	public FileTransferThread(FileTransferApp filetransfer, NodeHandle nh,
			String filepath) {
		this.filetransfer = filetransfer;
		this.filepath = filepath;
		this.nh = nh;
	}

	@Override
	public void run() {
		try {
			filetransfer.SendFileDirect(nh, filepath);
		} catch (Exception e) {
			System.err.println("MyFunctions Thread error " + e.getMessage());
		}
	}

}
