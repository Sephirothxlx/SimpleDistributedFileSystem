package sdfs.message;

import java.io.Serializable;
import java.util.UUID;

public class DataNodeResponse extends AbstractMessage implements Serializable {
	private byte[] b;

	public DataNodeResponse(Type type, byte[] b) {
		super(type);
		this.b = b;
	}

	public byte[] getByte() {
		return this.b;
	}

}
