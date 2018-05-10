package edu.umn.cs.spatialHadoop.twitter;

import org.apache.hadoop.io.Text;
import org.json.JSONObject;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.geojson.GeoJsonReader;

import edu.umn.cs.spatialHadoop.core.OGCJTSShape;

public class GeoTweetPolygon extends OGCJTSShape {
	
	String json;

	public GeoTweetPolygon() {
		// TODO Auto-generated constructor stub
	}

	public GeoTweetPolygon(Geometry geom) {
		super(geom);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Text toText(Text text) {
		byte[] temp = (json == null? "" : json).getBytes();
	    text.append(temp, 0, temp.length);
	    return text;
	}

	@Override
	public void fromText(Text text) {
		this.json = text.toString();
		try {
			JSONObject tweet = new JSONObject(json);
			JSONObject bbox = tweet.getJSONObject("place").getJSONObject("bounding_box");
			GeoJsonReader reader = new GeoJsonReader();
			this.geom = reader.read(bbox.toString());
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
}
