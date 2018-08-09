package sdfs.namenode;

public class CacheBlock {
	private LocatedBlock lb;
	private byte[] content;
	private boolean dirty;

	public CacheBlock(LocatedBlock lb, byte[] content, boolean dirty) {
		this.lb = lb;
		this.content = content;
		this.dirty = dirty;
	}

	public LocatedBlock getLocatedBlock() {
		return lb;
	}

	public byte[] getContent() {
		return content;
	}

	public void setContent(byte[] c) {
		content = c;
	}

	public boolean isDirty() {
		return dirty;
	}

	public void setDirty(boolean dirty) {
		this.dirty = dirty;
	}
}
