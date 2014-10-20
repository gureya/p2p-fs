package rice.p2p.projecto;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import rice.Continuation;
import rice.p2p.commonapi.Id;
import rice.p2p.past.PastContent;
import rice.p2p.past.PastImpl;


/**
 * 
 * @author GUREYA
 *Handles Insert and Lookup operations of the Pastry DHT
 */
public class ContentHandler {
	private PastImpl app;
	HashMap<String, HashMap<String, byte[]>> returnedFiles;

	public ContentHandler(PastImpl app) {
		this.app = app;
		returnedFiles = new HashMap<String, HashMap<String, byte[]>>();
		new ValidateReturnedFiles(this).start();
	}

	public void getFile(String uniqVal, String[] hashKey) {
		// TODO Auto-generated method stub
		UpdateRetrivalMap(uniqVal, hashKey);
		for (int i = 0; i < hashKey.length; i++) {
			final String key = hashKey[i];
			System.out.println("Looking up " + key + " at node "
					+ app.getLocalNodeHandle());
			app.lookup(FileOperations.regenerateID(hashKey[i]),
					new Continuation<PastContent, Exception>() {
						public void receiveResult(PastContent result) {
							try {
								MyContent obj = (MyContent) result;
								String resultKey = obj.getId().toString();
								byte[] data = obj.content;
								UpdateRetrivalMapData(resultKey, data);
							} catch (Exception ex) {
								System.err.println(ex.getMessage());
							}
							System.out.println("Successfully looked up "
									+ result + " for key " + key + ".");
						}

						public void receiveException(Exception result) {
							System.out.println("Error looking up " + key);
							result.printStackTrace();
						}
					});
		}

	}

	public void sendData(Id hashId, byte[] data) {
		// TODO Auto-generated method stub
		final PastContent mycontent = new MyContent(hashId, data);
		System.out.println("Inserting " + mycontent + " at node "
				+ app.getLocalNodeHandle());
		app.insert(mycontent, new Continuation<Boolean[], Exception>() {
			public void receiveResult(Boolean[] results) {
				int numSuccessfulStores = 0;
				for (int ctr = 0; ctr < results.length; ctr++) {
					if (results[ctr].booleanValue())
						numSuccessfulStores++;
				}
				System.out.println(mycontent + " successfully stored at "
						+ numSuccessfulStores + " locations.");
				System.out.println("Content Sent successfully");
			}

			public void receiveException(Exception result) {
				result.printStackTrace();
				System.err.println("Error inserting content");
			}
		});
	}
	
	public void CompleteFileRetrival()
	{
		synchronized (returnedFiles)
		{			
			if(returnedFiles.size()>0)
			{				
				ArrayList<String> completedFiles=new ArrayList<String>();
				for(Entry<String, HashMap<String, byte[]>>  temp:returnedFiles.entrySet())
				{					
					if(!temp.getValue().containsValue(null))
					{
						completedFiles.add(temp.getKey());
						FileOperations.addFiles(temp.getKey(), GetFullContent(temp.getValue()));
						System.out.println("Fully retrieved files");
					}
				}
				if(completedFiles.size()>0)
					RemoveMapItems(completedFiles.toArray(new String[completedFiles.size()]));
				
			}
		}
	}
	
	private void RemoveMapItems(String[] keys)
	{
		for(String uniqueKey: keys)
		{
			if(returnedFiles.containsKey(uniqueKey))
			{
				returnedFiles.remove(uniqueKey);
			}
		}
	}
	
	private byte[] GetFullContent(HashMap<String, byte[]> file)
	{
		int byteSize=0;
		for(byte[] b:file.values())
		{		
			byteSize+=b.length;
		}
		ByteBuffer merged = ByteBuffer.allocate(byteSize);
		for(byte[] b:file.values())
		{		
			merged.put(b);			
		}
		System.out.println("Content fully merged...");
		return (merged.compact()).array();
	}
	
	private void UpdateRetrivalMapData(String key, byte[] data)
	{
		synchronized (returnedFiles) 
		{			
			if(returnedFiles.size()>0)
			{
				
				for(HashMap<String, byte[]> temp:returnedFiles.values())
				{
					if(temp.containsKey(key))
					{
						temp.put(key, data);
						break;
					}				
				}
			}
		}
	}
	
	private void UpdateRetrivalMap(String uniqueValue, String[] hashKey )
	{
		synchronized (returnedFiles)
		{
			HashMap<String, byte[]> chunks=new HashMap<String, byte[]>();
			for(int i=0;i<hashKey.length;i++)
			{
				chunks.put(hashKey[i], null);
			}
			returnedFiles.put(uniqueValue, chunks);
		}
	}

}

class ValidateReturnedFiles extends Thread {
	ContentHandler dt;

	public ValidateReturnedFiles(ContentHandler dt) {
		this.dt = dt;
	}

	public void run() {
		while (true) {
			try {
				dt.CompleteFileRetrival();
				Thread.sleep(5000);
			} catch (Exception ex) {
				System.err
						.println(ex.getMessage() + "---" + ex.getStackTrace());
			}
		}
	}
}
