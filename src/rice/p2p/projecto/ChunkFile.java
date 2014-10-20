package rice.p2p.projecto;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.math.*;

/**
 * 
 * @author GUREYA
 *Splitting the files into chunks
 */
public class ChunkFile {
	
	// Define the chunk size
	public static int chunkSize;
	/**
	 * split the file specified by filename into pieces, each of size chunkSize
	 * except for the last one, which may be smaller
	 */
	public ArrayList<String> Chunk(String filename)
			throws FileNotFoundException, IOException {
		
		BufferedInputStream lbins = new BufferedInputStream(
				new FileInputStream(filename));
		File f = new File(filename);
		long fileSize = f.length();
		
		chunkSize = (int) (fileSize/Math.log10(fileSize));
		System.out.println("Splitting the file - "
				+ fileSize + " FILE NAME:" + filename + " chunkSize:" + chunkSize);
		
		int chunk;
		byte[] buffer = new byte[(int) chunkSize];
		ArrayList<String> fSendToPeers = new ArrayList<>();
		
		//Compose key for each chunk
		String username = "gureya";
		String Key = username + filename;
		
		for (chunk = 0; chunk < fileSize / chunkSize; chunk++) {
			lbins.read(buffer);

			Date date = new Date();
			Timestamp timestamp = new Timestamp(date.getTime()); 
			Key += timestamp;
			String sFileHash = FileOperations.generateHashKey(Key);
			FileOperations.sendData(sFileHash, buffer);

			fSendToPeers.add(sFileHash);
			System.out.println("File hash value :" + sFileHash + "key value :"+Key+"\n");
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		if (fileSize != chunkSize * (chunk - 1)) {

			int iRestSize = (int) (fileSize - (chunkSize * chunk));
			byte[] lrestbuffer = new byte[iRestSize];
			lbins.read(lrestbuffer);
			
			Date date = new Date();
			Timestamp timestamp = new Timestamp(date.getTime()); 
			
			Key += timestamp;
			String sFileHash = FileOperations.generateHashKey(Key);
			FileOperations.sendData(sFileHash, buffer);

			fSendToPeers.add(sFileHash);
			System.out.println("File hash value :" + sFileHash + "\n");
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		lbins.close();

		return fSendToPeers;
	}

	public void join(String baseFilename) throws IOException {
		int numberParts = chunkNumber(baseFilename);

		BufferedOutputStream out = new BufferedOutputStream(
				new FileOutputStream(baseFilename));
		for (int part = 0; part < numberParts; part++) {
			BufferedInputStream in = new BufferedInputStream(
					new FileInputStream(baseFilename + "." + part));

			int b;
			while ((b = in.read()) != -1)
				out.write(b);

			in.close();
		}
		out.close();
	}

	public int chunkNumber(String baseFilename) throws IOException {
		// list all files in the same directory
		File directory = new File(baseFilename).getAbsoluteFile()
				.getParentFile();
		final String justFilename = new File(baseFilename).getName();
		String[] matchingFiles = directory.list(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.startsWith(justFilename)
						&& name.substring(justFilename.length()).matches(
								"^\\.\\d+$");
			}
		});
		return matchingFiles.length;
	}
}
