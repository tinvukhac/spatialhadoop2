package edu.umn.cs.spatialHadoop.twitter;

import org.json.JSONArray;
import org.json.JSONObject;

import edu.umn.cs.spatialHadoop.core.Polygon;

public class TWPolygon extends Polygon {

	/**
	 * 
	 */
	private static final long serialVersionUID = -883473529806644239L;

	private static final char SEPARATOR = '\t';
	public long id;

	public TWPolygon() {
		super();
	}

	public TWPolygon(String jsonTweet) {
		JSONObject tweet = new JSONObject(jsonTweet);
		this.id = tweet.getLong("id");
		JSONArray coordinates = tweet.getJSONObject("place").getJSONObject("bounding_box").getJSONArray("coordinates").getJSONArray(0);
		int npoints = coordinates.length();
		int[] xpoints = new int[npoints];
		int[] ypoints = new int[npoints];
		for (int i = 0; i < npoints; i++) {
			JSONArray point = coordinates.getJSONArray(i);
//			xpoints[i] = point.getFloat(0);
//			ypoints[i] = point.getFloat(1);
		}
	}
}
