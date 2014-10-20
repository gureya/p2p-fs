package rice.p2p.projecto;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.fusejna.DirectoryFiller;
import net.fusejna.ErrorCodes;
import net.fusejna.FuseException;
import net.fusejna.StructFuseFileInfo.FileInfoWrapper;
import net.fusejna.StructStat.StatWrapper;
import net.fusejna.types.TypeMode.ModeWrapper;
import net.fusejna.types.TypeMode.NodeType;
import net.fusejna.util.FuseFilesystemAdapterAssumeImplemented;

public class MemoryFs extends FuseFilesystemAdapterAssumeImplemented {
	
	
	private static ChunkFile chunkfile = new ChunkFile();
	public static final String METADATA = "filemetadata.txt";

	private static String mountpoint;
	private static String initialMetaData;
	public static HashMap<String, Integer> mapFileStore = new HashMap<String, Integer>();
	public static HashMap<String, ArrayList<String>> mapMetaMemyStore = new HashMap<String, ArrayList<String>>();
	public static HashMap<String, MemoryDirectory> DirMap = new HashMap<String, MemoryDirectory>();
	public static int g;
	public static HashMap<String, Long> memFileDetails = new HashMap<String, Long>();

	public class FileSizeMeta {
		long filesize;
		boolean isChanged;
	}

	public static HashMap<String, FileSizeMeta> FileContentSize = new HashMap<String, FileSizeMeta>();

	private final class MemoryDirectory extends MemoryPath {
		private final List<MemoryPath> contents = new ArrayList<MemoryPath>();

		private MemoryDirectory(final String name) {
			super(name);
		}

		private MemoryDirectory(final String name, final MemoryDirectory parent) {
			super(name, parent);
		}

		public void add(final MemoryPath p) {
			contents.add(p);
			p.parent = this;
		}

		@Override
		protected MemoryPath find(String path) {
			if (super.find(path) != null) {
				return super.find(path);
			}
			while (path.startsWith("/")) {
				path = path.substring(1);
			}
			if (!path.contains("/")) {
				for (final MemoryPath p : contents) {
					if (p.name.equals(path)) {
						return p;
					}
				}
				return null;
			}
			final String nextName = path.substring(0, path.indexOf("/"));
			final String rest = path.substring(path.indexOf("/"));
			for (final MemoryPath p : contents) {
				if (p.name.equals(nextName)) {
					return p.find(rest);
				}
			}
			return null;
		}

		@Override
		protected void getattr(final StatWrapper stat) {
			stat.setMode(NodeType.DIRECTORY);
		}

		private void mkdir(final String lastComponent) {
			contents.add(new MemoryDirectory(lastComponent, this));
		}

		public void mkfile(final String lastComponent) {
			contents.add(new MemoryFile(lastComponent, this));
		}

		public void read(final DirectoryFiller filler) {
			for (final MemoryPath p : contents) {
				filler.add(p.name);
			}
		}
	}

	private final class MemoryFile extends MemoryPath {
		private ByteBuffer contents = ByteBuffer.allocate(0);

		private MemoryFile(final String name) {
			super(name);
		}

		private MemoryFile(final String name, final MemoryDirectory parent) {
			super(name, parent);
		}

		public MemoryFile(final String name, final String text) {
			super(name);
			try {
				final byte[] contentBytes = text.getBytes("UTF-8");
				contents = ByteBuffer.wrap(contentBytes);
			} catch (final UnsupportedEncodingException e) {
				// Not going to happen
			}
		}

		@Override
		protected void getattr(final StatWrapper stat) {
			stat.setMode(NodeType.FILE);
			stat.size(contents.capacity());
		}

		private int read(final ByteBuffer buffer, final long size,
				final long offset) {
			final int bytesToRead = (int) Math.min(
					contents.capacity() - offset, size);
			final byte[] bytesRead = new byte[bytesToRead];
			contents.position((int) offset);
			contents.get(bytesRead, 0, bytesToRead);
			buffer.put(bytesRead);
			contents.position(0); // Rewind
			return bytesToRead;
		}

		private void truncate(final long size) {
			if (size < contents.capacity()) {
				// Need to create a new, smaller buffer
				final ByteBuffer newContents = ByteBuffer.allocate((int) size);
				final byte[] bytesRead = new byte[(int) size];
				contents.get(bytesRead);
				newContents.put(bytesRead);
				contents = newContents;
			}
		}

		private int write(final ByteBuffer buffer, final long bufSize,
				final long writeOffset) {
			final int maxWriteIndex = (int) (writeOffset + bufSize);
			if (maxWriteIndex > contents.capacity()) {
				// Need to create a new, larger buffer
				final ByteBuffer newContents = ByteBuffer
						.allocate(maxWriteIndex);
				newContents.put(contents);
				contents = newContents;
			}
			final byte[] bytesToWrite = new byte[(int) bufSize];
			buffer.get(bytesToWrite, 0, (int) bufSize);
			contents.position((int) writeOffset);
			contents.put(bytesToWrite);
			contents.position(0); // Rewind
			return (int) bufSize;
		}
	}

	private abstract class MemoryPath {
		private String name;
		private MemoryDirectory parent;

		private MemoryPath(final String name) {
			this(name, null);
		}

		private MemoryPath(final String name, final MemoryDirectory parent) {
			this.name = name;
			this.parent = parent;
		}

		private void delete() {
			if (parent != null) {
				parent.contents.remove(this);
				parent = null;
			}
		}

		protected MemoryPath find(String path) {
			while (path.startsWith("/")) {
				path = path.substring(1);
			}
			if (path.equals(name) || path.isEmpty()) {
				return this;
			}
			return null;
		}

		protected abstract void getattr(StatWrapper stat);

		private void rename(String newName) {
			while (newName.startsWith("/")) {
				newName = newName.substring(1);
			}
			name = newName;
		}
	}

	private final MemoryDirectory rootDirectory = new MemoryDirectory("");

	public MemoryFs() {

		CreateInmemoryMap();

	}

	@Override
	public int access(final String path, final int access) {
		return 0;
	}

	@Override
	public int create(final String path, final ModeWrapper mode,
			final FileInfoWrapper info) {
		if (getPath(path) != null) {
			return -ErrorCodes.EEXIST();
		}
		final MemoryPath parent = getParentPath(path);
		if (parent instanceof MemoryDirectory) {
			((MemoryDirectory) parent).mkfile(getLastComponent(path));
            // Never include the temporary files
			if (!(path.matches("^.+\\.sw[px]$"))) {
				long epoch = System.currentTimeMillis();
				String sFullath = mountpoint + path;
				memFileDetails.put(sFullath, epoch);
				FileSizeMeta fm = new FileSizeMeta();
				fm.filesize = 0;
				fm.isChanged = false;
				FileContentSize.put(sFullath, fm);
				System.out
						.println("Finished creating the new file..."
								+ sFullath);
			} else {
				System.out
						.println("Waiting until the file is saved....");
			}
			return 0;
		}
		return -ErrorCodes.ENOENT();
	}

	@Override
	public int getattr(final String path, final StatWrapper stat) {
		final MemoryPath p = getPath(path);
		String sFullath = mountpoint + path;
		if (p != null) {
			if (memFileDetails.get(sFullath) != null) {
				stat.setAllTimesMillis(memFileDetails.get(sFullath));
			}
			p.getattr(stat);
			return 0;
		}
		return -ErrorCodes.ENOENT();
	}

	private String getLastComponent(String path) {
		while (path.substring(path.length() - 1).equals("/")) {
			path = path.substring(0, path.length() - 1);
		}
		if (path.isEmpty()) {
			return "";
		}
		return path.substring(path.lastIndexOf("/") + 1);
	}

	private MemoryPath getParentPath(final String path) {
		return rootDirectory.find(path.substring(0, path.lastIndexOf("/")));
	}

	private MemoryPath getPath(final String path) {
		return rootDirectory.find(path);
	}

	@Override
	public int mkdir(final String path, final ModeWrapper mode) {
		if (getPath(path) != null) {
			return -ErrorCodes.EEXIST();
		}
		final MemoryPath parent = getParentPath(path);
		if (parent instanceof MemoryDirectory) {
			((MemoryDirectory) parent).mkdir(getLastComponent(path));
			return 0;
		}
		return -ErrorCodes.ENOENT();
	}

	@Override
	public int open(final String path, final FileInfoWrapper info) {
		return 0;
	}

	@Override
	public int read(final String path, final ByteBuffer buffer,
			final long size, final long offset, final FileInfoWrapper info) {
		final MemoryPath p = getPath(path);
		if (p == null) {
			return -ErrorCodes.ENOENT();
		}
		if (!(p instanceof MemoryFile)) {
			return -ErrorCodes.EISDIR();
		}

		return ((MemoryFile) p).read(buffer, size, offset);
	}

	@Override
	public int readdir(final String path, final DirectoryFiller filler) {
		final MemoryPath p = getPath(path);
		if (p == null) {
			return -ErrorCodes.ENOENT();
		}
		if (!(p instanceof MemoryDirectory)) {
			return -ErrorCodes.ENOTDIR();
		}
		((MemoryDirectory) p).read(filler);
		return 0;
	}

	@Override
	public int rename(final String path, final String newName) {
		final MemoryPath p = getPath(path);
		if (p == null) {
			return -ErrorCodes.ENOENT();
		}
		final MemoryPath newParent = getParentPath(newName);
		if (newParent == null) {
			return -ErrorCodes.ENOENT();
		}
		if (!(newParent instanceof MemoryDirectory)) {
			return -ErrorCodes.ENOTDIR();
		}
		p.delete();
		p.rename(newName.substring(newName.lastIndexOf("/")));
		((MemoryDirectory) newParent).add(p);
		return 0;
	}

	@Override
	public int rmdir(final String path) {
		final MemoryPath p = getPath(path);
		if (p == null) {
			return -ErrorCodes.ENOENT();
		}
		if (!(p instanceof MemoryDirectory)) {
			return -ErrorCodes.ENOTDIR();
		}
		p.delete();
		return 0;
	}

	@Override
	public int truncate(final String path, final long offset) {
		final MemoryPath p = getPath(path);
		if (p == null) {
			return -ErrorCodes.ENOENT();
		}
		if (!(p instanceof MemoryFile)) {
			return -ErrorCodes.EISDIR();
		}
		((MemoryFile) p).truncate(offset);
		return 0;
	}

	@Override
	public int unlink(final String path) {
		final MemoryPath p = getPath(path);
		if (p == null) {
			return -ErrorCodes.ENOENT();
		}
		p.delete();
		String sFullpath = mountpoint + path;
		System.out.println("Deleting file...: "
				+ sFullpath);
		return 0;
	}

	@Override
	public int write(final String path, final ByteBuffer buf,
			final long bufSize, final long writeOffset,
			final FileInfoWrapper wrapper) {
		final MemoryPath p = getPath(path);
		if (p == null) {
			return -ErrorCodes.ENOENT();
		}
		if (!(p instanceof MemoryFile)) {
			return -ErrorCodes.EISDIR();
		}
		return ((MemoryFile) p).write(buf, bufSize, writeOffset);
	}

	@Override
	public int release(final String path, final FileInfoWrapper info) {

		String sFullPath = mountpoint + path;
		File fs = new File(sFullPath);
		if (FileContentSize.get(sFullPath) != null) {
			long l_ipreviousSize = FileContentSize.get(sFullPath).filesize;
			if (fs.length() != l_ipreviousSize) {
				FileContentSize.get(sFullPath).filesize = fs.length();
				FileContentSize.get(sFullPath).isChanged = true;

				System.out
						.println("File has been modified,...sending it to other peers! :"
								+ sFullPath);
				try {
					WriteMetaData(chunkfile.Chunk(sFullPath), sFullPath);
				} catch (Exception ex) {
					System.out
							.println("Failed Sending File to other peers....");
					ex.printStackTrace();
				}

				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return 0;
	}

	public static ArrayList<String> HashArrayList(String sFullFileNmae) {
		return mapMetaMemyStore.get(sFullFileNmae);
	}

	// format file name (absolute path) and corresponding file hashes
	public void CreateInmemoryMap() {
		BufferedReader br = null;

		try {

			String sCurrentLine;

			br = new BufferedReader(new FileReader(initialMetaData));

			while ((sCurrentLine = br.readLine()) != null) {
				String[] lsParts = sCurrentLine.split("-");
				String[] sHashSeries = lsParts[1].split("\\|");
				ArrayList<String> lMetaHashArray = new ArrayList<String>();
				for (int i = 0; i < sHashSeries.length; ++i) {
					lMetaHashArray.add(sHashSeries[i]);
				}
				mapMetaMemyStore.put(lsParts[0], lMetaHashArray);
				CreateDirStructure(lsParts[0]);
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}

	private void CreateDirStructure(String sDirFileString)
			throws FileNotFoundException, IOException {

		int lOffset = mountpoint.length();
		String sRestString = sDirFileString.substring(lOffset + 1);
		String[] lsFileParts = sRestString.split("/");
		String sDirName = mountpoint;
		String sSearchPath = mountpoint;
		if (lsFileParts.length >= 2) // we have dir
		{
			for (int i = 0; i < lsFileParts.length - 1; ++i) {
				sDirName = sDirName + lsFileParts[i] + "/";
				if (i > 0) {
					sSearchPath += lsFileParts[i - 1] + "/";
				}
				if (DirMap.get(sDirName) == null) {
					final String sFilename = lsFileParts[i];
					final MemoryDirectory memDir = new MemoryDirectory(
							sFilename);

					if (i == 0) {
						rootDirectory.add(memDir);
						DirMap.put(sDirName, memDir);
					} else {
						System.out
								.println("Reading the metadata file...to setup the MemoryFs"
										+ sFilename);
						final MemoryDirectory memDir1 = new MemoryDirectory(
								sFilename);

						DirMap.get(sSearchPath).add(memDir1);
						DirMap.put(sDirName, memDir1);
					}

				}

			}

			DirMap.get(sDirName).add(
					new MemoryFile(lsFileParts[lsFileParts.length - 1], ""));

		} else {
			MemoryFile fs = new MemoryFile(lsFileParts[0], "");
			rootDirectory.add(fs);

		}

	}

	public void WriteMetaData(ArrayList<String> ArrayFileHash, String sFileName) {
		try {

			File file = new File(initialMetaData);
			System.out.println("Writing new metadata information  :"
					+ initialMetaData);
			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}

			FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
			BufferedWriter bw = new BufferedWriter(fw);
			String sUpdatehashline = "";
			for (int j = 0; j < ArrayFileHash.size(); ++j) {
				sUpdatehashline = sUpdatehashline + ArrayFileHash.get(j) + "|";

			}
			if (sUpdatehashline.charAt(sUpdatehashline.length() - 1) == '|') {
				sUpdatehashline = sUpdatehashline.replaceFirst(".$", "");
			}

			mapMetaMemyStore.put(sFileName, ArrayFileHash);
			sUpdatehashline = sFileName + "-" + sUpdatehashline;
			bw.write(sUpdatehashline);

			bw.write("\n");
			bw.close();
			System.out
					.println("Updating the metadata hashes:..."
							+ sUpdatehashline);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void SetStartupFiles(String MountPath, String sApplicationPath) {
		g = 0;
		mountpoint = MountPath;
		String sApplicatonPath = sApplicationPath;
		initialMetaData = sApplicatonPath + "/" + METADATA;
		System.out.println("MemoryFs mount point: " + mountpoint);
	}

	public static void MountFs() throws FuseException {
		new MemoryFs().log(false).mount(mountpoint);
	}

}
