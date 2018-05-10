package edu.umn.cs.spatialHadoop.indexing;

import org.apache.hadoop.io.Text;
import edu.umn.cs.spatialHadoop.io.TextSerializable;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

public class LSMComponent implements TextSerializable {
	
	int id;
	String name;
	long size;
	
	public LSMComponent(int id, String name) {
		this.id = id;
		this.name = name;
	}
	
	public LSMComponent(int id, String name, long size) {
		this.id = id;
		this.size = size;
		this.name = name;
	}
	
	public LSMComponent() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public Text toText(Text text) {
		TextSerializerHelper.serializeInt(this.id, text, ',');
		TextSerializerHelper.serializeLong(this.size, text, ',');
		byte[] temp = (this.name == null? "" : this.name).getBytes();
	    text.append(temp, 0, temp.length);
		return text;
	}

	@Override
	public void fromText(Text text) {
		this.id = TextSerializerHelper.consumeInt(text, ',');
		this.size = TextSerializerHelper.consumeLong(text, ',');
		this.name = text.toString();
	}
	
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public long getSize() {
		return size;
	}

	public void setSize(long size) {
		this.size = size;
	}
}
