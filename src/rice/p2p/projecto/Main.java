package rice.p2p.projecto;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map.Entry;

import net.fusejna.FuseException;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.past.PastContent;
import rice.p2p.past.PastImpl;
import rice.pastry.NodeIdFactory;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeFactory;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.standard.RandomNodeIdFactory;
import rice.persistence.LRUCache;
import rice.persistence.MemoryStorage;
import rice.persistence.PersistentStorage;
import rice.persistence.Storage;
import rice.persistence.StorageManagerImpl;

/**
 * 
 * @author GUREYA Main class - launches the Pastry DHT and the FUSE filesystem
 */

public class Main {

	private static PastryIdFactory idf;
	private static PastImpl app;

	private static FileTransferApp fileTransfer;
	private static ContentHandler contentHandler;

	public static FileTransferApp getFileTransfer() {
		return fileTransfer;
	}

	public static PastryIdFactory getPastryIdFactory() {
		return idf;
	}

	public static PastImpl getPastImpl() {
		return app;
	}

	public static ContentHandler getContentTransfer() {
		return contentHandler;
	}

	/**
	 * This constructor sets up a PastryNode. It will bootstrap to an existing
	 * ring if it can find one at the specified location, otherwise it will
	 * start a new ring.
	 * 
	 * @param bindport
	 *            the local port to bind to
	 * @param bootaddress
	 *            the IP:port of the node to boot from
	 * @param env
	 *            the environment for these nodes
	 */
	public Main() {
		// TODO Auto-generated constructor stub
	}
	public Main(int bindport, InetSocketAddress bootaddress, Environment env)
			throws Exception {

		// Generate the NodeIds Randomly
		NodeIdFactory nidFactory = new RandomNodeIdFactory(env);

		// construct the PastryNodeFactory, this is how we use
		// rice.pastry.socket
		PastryNodeFactory factory = new SocketPastryNodeFactory(nidFactory,
				bindport, env);

		// construct a node
		PastryNode node = factory.newNode();

		// construct a new MyApp for messageRouting and FileTransfer
		fileTransfer = new FileTransferApp(node);

		// Start past
		idf = new rice.pastry.commonapi.PastryIdFactory(env);
		FileOperations.setPastryIdFactory(idf);
		// create a different storage root for each node
		String storageDirectory = "./storage" + node.getId().hashCode();

		// create the persistent part
		Storage stor = new PersistentStorage(idf, storageDirectory,
				4 * 1024 * 1024, node.getEnvironment());
		// Storage stor = new MemoryStorage(idf);
		app = new PastImpl(node, new StorageManagerImpl(idf, stor,
				new LRUCache(new MemoryStorage(idf), 512 * 1024,
						node.getEnvironment())), 3, "");
		contentHandler = new ContentHandler(app);
		FileOperations.setContentHandler(contentHandler);
		// End past

		node.boot(bootaddress);

		// the node may require sending several messages to fully boot into the
		// ring
		synchronized (node) {
			while (!node.isReady() && !node.joinFailed()) {
				// delay so we don't busy-wait
				node.wait(500);

				// abort if can't join
				if (node.joinFailed()) {
					throw new IOException(
							"Could not join the FreePastry ring.  Reason:"
									+ node.joinFailedReason());
				}
			}
		}

		System.out.println("Finished creating new node " + node);
		Main startThreads = new Main();
		//MemoryFs thread
		startThreads.new MemoryFsThread().start();
		//sleep
		Thread.sleep(15000);
		//Get Files thread
		startThreads.new GetFilesThread().start();

		// wait 10 seconds
		env.getTimeSource().sleep(5000);
		Id storedKey = null;
		while (true) {
			System.out.println("Enter PUT command");
			BufferedReader buffer = new BufferedReader(new InputStreamReader(
					System.in));
			String line = buffer.readLine();

			if (line.equalsIgnoreCase("PUT")) {

				String s = "example.txt";

				Path path = Paths.get(s);
				byte[] data = Files.readAllBytes(path);
				final PastContent myContent = new MyContent(idf.buildId(s),
						data);
				storedKey = myContent.getId();
				FileOperations.sendData(storedKey.toString(), data);
			} else if (line.equalsIgnoreCase("GET")) {
				final Id lookupKey = storedKey;
				String key = lookupKey.toString();
				FileOperations.RequestFile("uuuuuuu", new String[] { key });
				while (true) {
					System.out.println("Waiting.....");
					Thread.sleep(3000);
					if (FileOperations.fileAvailable())
						break;
				}
				System.out.println("OK");
				ReturnedFile[] files = FileOperations.getReturnedFiles();
				FileOutputStream fileOuputStream = new FileOutputStream(
						"newsss.txt");
				fileOuputStream.write(files[0].getContent());
				fileOuputStream.close();
				System.out.println("Found file");
			}
		}

	}

	/**
	 * Usage: java [-cp FreePastry-<version>.jar]
	 * rice.tutorial.lesson3.DistTutorial localbindport bootIP bootPort example
	 * java rice.tutorial.DistTutorial 9001 pokey.cs.almamater.edu 9001
	 */
	public static void main(String[] args) throws Exception {
		try {
			// Mount the file system
			MemoryFs.SetStartupFiles(args[3], args[4]);
		} catch (Exception e) {
			// remind user how to use
			System.out.println("Usage:");
			System.out
					.println("Specify the mount point and the filemetadata path as the 3rd and 4th params");
			throw e;
		}
		// Loads pastry settings
		Environment env = new Environment();

		// disable the UPnP setting (in case you are testing this on a NATted
		// LAN)
		env.getParameters().setString("nat_search_policy", "never");

		try {
			// the port to use locally
			int bindport = Integer.parseInt(args[0]);

			// build the bootaddress from the command line args
			InetAddress bootaddr = InetAddress.getByName(args[1]);
			int bootport = Integer.parseInt(args[2]);
			InetSocketAddress bootaddress = new InetSocketAddress(bootaddr,
					bootport);

			// launch our node!
			Main newnode = new Main(bindport, bootaddress, env);
		} catch (Exception e) {
			// remind user how to use
			System.out.println("Usage:");
			System.out
					.println("java [-cp FreePastry-<version>.jar] rice.tutorial.lesson3.DistTutorial localbindport bootIP bootPort");
			System.out
					.println("example java rice.tutorial.DistTutorial 9001 pokey.cs.almamater.edu 9001");
			throw e;
		}

	}

	// / MemoryFs Thread
	class MemoryFsThread extends Thread {

		public MemoryFsThread() {

		}

		public void run() {
			try {
				System.out.println("Mounting the MemoryFs...");
				MemoryFs.MountFs();
			} catch (FuseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	// Retrieve files thread!!
	class GetFilesThread extends Thread {

		public GetFilesThread() {

		}

		public void run() {

			try {
				RetrieveFilesThread.RequestFileFromPeers();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}
}

class RetrieveFilesThread {

	public RetrieveFilesThread() {

	}

	public static void RequestFileFromPeers() throws IOException {
		for (Entry<String, ArrayList<String>> item : MemoryFs.mapMetaMemyStore
				.entrySet()) {
			String[] l_sListofHashes = new String[item.getValue().size()];
			l_sListofHashes = item.getValue().toArray(l_sListofHashes);

			FileOperations.RequestFile(item.getKey(), l_sListofHashes);
		}
		while (true) {
			if (FileOperations.fileAvailable()) {

				ReturnedFile[] GetFiles = FileOperations.getReturnedFiles();
				if (GetFiles.length != 0) {
					System.out
							.println("Retrieving this users file from the available peers...");
					for (int i = 0; i < GetFiles.length; ++i) {
						System.out
								.println("Creating the file now...."
										+ GetFiles[i].getUniqKey());

						FileOutputStream fsFileRequester;

						fsFileRequester = new FileOutputStream(
								GetFiles[i].getUniqKey());
						BufferedOutputStream out = new BufferedOutputStream(
								fsFileRequester);
						out.write(GetFiles[i].getContent());

						out.close();

					}
				}
			}
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
	}
}
