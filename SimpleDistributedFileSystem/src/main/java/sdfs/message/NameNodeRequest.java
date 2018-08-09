package sdfs.message;

import java.io.Serializable;
import java.util.UUID;

public class NameNodeRequest extends AbstractMessage implements Serializable {
	private String fileUri;
	private UUID fileUuid;
	private int number;

	public NameNodeRequest(Type type, String fileUri, UUID fileUuid, int number) {
		super(type);
		this.fileUri = fileUri;
		this.fileUuid = fileUuid;
		this.number = number;
	}

	public String getFileUri() {
		return fileUri;
	}

	public UUID getFileUUID() {
		return this.fileUuid;
	}

	public int getNumber() {
		return this.number;
	}
}
