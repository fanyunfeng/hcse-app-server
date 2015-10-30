package com.hcse.app.data.mysql;

public class DbDocument {
	private long key;
	private byte[] content;

	public DbDocument() {

	}

	public DbDocument(long key, byte[] content) {
		this.key = key;
		this.content = content;
	}

	public long getKey() {
		return key;
	}

	public void setKey(long key) {
		this.key = key;
	}

	public byte[] getContent() {
		return content;
	}

	public void setContent(byte[] content) {
		this.content = content;
	}
}
