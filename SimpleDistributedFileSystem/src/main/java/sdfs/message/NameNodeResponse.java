package sdfs.message;

import java.io.Serializable;
import java.util.List;

import sdfs.namenode.LocatedBlock;
import sdfs.namenode.SDFSFileChannel;

public class NameNodeResponse extends AbstractMessage implements Serializable {
	private SDFSFileChannel sdfsFileChannel;
	private LocatedBlock locatedBlock;
	private List<LocatedBlock> blocks;
	private String[] content;

	public NameNodeResponse(Type type, SDFSFileChannel sdfsFileChannel, LocatedBlock locatedBlock,
			List<LocatedBlock> blocks, String[] content) {
		super(type);
		this.sdfsFileChannel = sdfsFileChannel;
		this.locatedBlock = locatedBlock;
		this.blocks = blocks;
		this.content = content;
	}

	public SDFSFileChannel getSdfsFileChannel() {
		return this.sdfsFileChannel;
	}

	public LocatedBlock getLocatedBlock() {
		return this.locatedBlock;
	}

	public List<LocatedBlock> getBlocks() {
		return this.blocks;
	}

	public String[] getContent() {
		return this.content;
	}
}
