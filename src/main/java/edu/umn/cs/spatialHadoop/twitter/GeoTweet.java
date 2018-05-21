package edu.umn.cs.spatialHadoop.twitter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.json.JSONException;
import org.json.JSONObject;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;
//import com.vividsolutions.jts.io.geojson.GeoJsonReader;
import com.vividsolutions.jts.io.geojson.GeoJsonReader;

import edu.umn.cs.spatialHadoop.core.OGCJTSShape;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.io.TextSerializable;

public class GeoTweet extends OGCJTSShape implements Writable, TextSerializable {

	String json;

	public GeoTweet() {
		// TODO Auto-generated constructor stub
		// System.out.println("Init GeoTweet");
	}

	public GeoTweet(Geometry geom) {
		super(geom);
		// TODO Auto-generated constructor stub
	}

	public GeoTweet(String json) {
		this.json = json;
		this.geom = parseGeometryFromJsonString(json);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Text toText(Text text) {
		// System.out.println("toText");
		byte[] temp = (json == null ? "" : json).getBytes();
		text.append(temp, 0, temp.length);
		return text;
	}

	@Override
	public void fromText(Text text) {
		// System.out.println("fromText");
		this.json = text.toString();
		this.geom = parseGeometryFromJsonString(json);
		// this.json = "{ \"type\": \"FeatureCollection\", \"features\": [ { \"type\":
		// \"Feature\", \"properties\": { \"name\": \"Van Dorn Street\",
		// \"marker-color\": \"#0000ff\", \"marker-symbol\": \"rail-metro\", \"line\":
		// \"blue\" }, \"geometry\": { \"type\": \"Point\", \"coordinates\": [
		// -77.12911152370515, 38.79930767201779 ] } }, { \"type\": \"Feature\",
		// \"properties\": { \"name\": \"Franconia-Springfield\", \"marker-color\":
		// \"#0000ff\", \"marker-symbol\": \"rail-metro\", \"line\": \"blue\" },
		// \"geometry\": { \"type\": \"Point\", \"coordinates\": [ -77.16797018042666,
		// 38.766521892689916 ] } }] }";
		// this.json = "{\"geometry\": { \"type\": \"Point\", \"coordinates\": [
		// -77.12911152370515, 38.79930767201779 ] }}";
		// this.json = "{ \"type\": \"Point\", \"coordinates\": [ -77.12911152370515,
		// 38.79930767201779 ] }";
//		JSONObject tweet = new JSONObject(json);
//		try {
//			JSONObject bbox = tweet.getJSONObject("place").getJSONObject("bounding_box");
//			GeoJsonReader reader = new GeoJsonReader();
//			try {
//				this.geom = reader.read(bbox.toString());
//				// this.geom =
//				// reader.read("{\"coordinates\":[[[-80.282775,26.089889],[-80.282775,26.105196],[-80.265788,26.105196],[-80.265788,26.089889],[-80.282775,26.089889]]],\"type\":\"Polygon\"}");
//			} catch (ParseException e2) {
//				// TODO Auto-generated catch block
//				this.geom = new GeometryFactory().createPoint(new Coordinate(0, 0));
//				System.out.println("ParseException");
//				// System.out.println(json);
//				System.out.println(bbox.toString());
//				e2.printStackTrace();
//			}
//		} catch (JSONException e1) {
//			// System.out.println("JSONException");
//			// System.out.println(json);
//			this.geom = new GeometryFactory().createPoint(new Coordinate(0, 0));
//			e1.printStackTrace();
//		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// System.out.println("public void write");
		super.write(out);
		out.writeUTF(this.json);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// System.out.println("public void readFields");
		super.readFields(in);
		this.json = in.readUTF();
	}

	@Override
	public Shape clone() {
		GeoTweet copy = new GeoTweet(this.json);
		return copy;
	}
	
	private Geometry parseGeometryFromJsonString(String json) {
		
		if(json != null) {
			Geometry geom;
			
			JSONObject tweet = new JSONObject(json);
			try {
				JSONObject bbox = tweet.getJSONObject("place").getJSONObject("bounding_box");
				GeoJsonReader reader = new GeoJsonReader();
				try {
					geom = reader.read(bbox.toString());
					// this.geom =
					// reader.read("{\"coordinates\":[[[-80.282775,26.089889],[-80.282775,26.105196],[-80.265788,26.105196],[-80.265788,26.089889],[-80.282775,26.089889]]],\"type\":\"Polygon\"}");
				} catch (ParseException e2) {
					// TODO Auto-generated catch block
					geom = new GeometryFactory().createPoint(new Coordinate(0, 0));
					System.out.println("ParseException");
					// System.out.println(json);
					System.out.println(bbox.toString());
					e2.printStackTrace();
				}
			} catch (JSONException e1) {
				 System.out.println("JSONException");
				// System.out.println(json);
				geom = new GeometryFactory().createPoint(new Coordinate(0, 0));
				e1.printStackTrace();
			}
			
			return geom;
		} else {
//			System.out.println("Json input is null");
			return null;
		}
	}
}
