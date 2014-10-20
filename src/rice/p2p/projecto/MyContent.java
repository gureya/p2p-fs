package rice.p2p.projecto;

import rice.p2p.commonapi.Id;
import rice.p2p.past.ContentHashPastContent;
	
public class MyContent extends ContentHashPastContent{
	byte[] content;

	public MyContent(Id id, byte[] content) {
		super(id);
		this.content = content;
	}

	public String toString() {
		return "MyPastContent [" + content + "]";
	}
}
