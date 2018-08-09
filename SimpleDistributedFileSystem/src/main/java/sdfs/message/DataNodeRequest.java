package sdfs.message;

import java.io.Serializable;
import java.util.UUID;

public class DataNodeRequest extends AbstractMessage implements Serializable {
	private UUID uuid;
	private int blockNumber;
	private int offset;
	private int size;
	private byte[] b;

	public DataNodeRequest(Type type, UUID uuid, int blockNumber, int offset, int size, byte[] b) {
		super(type);
		this.uuid = uuid;
		this.blockNumber = blockNumber;
		this.offset = offset;
		this.size = size;
		this.b = b;
	}

	public UUID getUUID() {
		return this.uuid;
	}

	public int getBlockNumber() {
		return this.blockNumber;
	}

	public int getOffset() {
		return this.offset;
	}

	public int getSize() {
		return this.size;
	}

	public byte[] getByte() {
		return this.b;
	}

}
