package edu.umn.cs.spatialHadoop.operations;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.JSONArray;
import org.json.JSONObject;

public class LoadTwitterData {

	private static final Log LOG = LogFactory.getLog(LoadTwitterData.class);

	public static class Map extends Mapper<Object, Text, Text, NullWritable> {

		// public void setup(Context context) throws IOException {
		// Path pt = new
		// Path("hdfs://ec-hn.cs.ucr.edu:8040/user/tvu032/shadoop/datasets/TwitterData/geoTwitter18-03-20/geoTwitter18-03-20_00_05");//
		// Location of file in HDFS
		// FileSystem fs = FileSystem.get(new Configuration());
		// BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
		// String line;
		// line = br.readLine();
		// while (line != null) {
		// System.out.println(line);
		// line = br.readLine();
		// }
		// }

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				JSONObject tweet = new JSONObject(value.toString());
				String id = tweet.getString("id_str");
				String type = tweet.getJSONObject("place").getJSONObject("bounding_box").getString("type");
				JSONArray coordinates = tweet.getJSONObject("place").getJSONObject("bounding_box")
						.getJSONArray("coordinates");

				if (type.equals("Polygon")) {
					StringBuilder coordinateString = new StringBuilder();
					JSONArray polygonCoordinates = coordinates.getJSONArray(0);
					for (int i = 0; i < polygonCoordinates.length(); i++) {
						JSONArray point = polygonCoordinates.getJSONArray(i);
						coordinateString.append(String.format(",%f,%f", point.getFloat(0), point.getFloat(1)));
					}
					context.write(new Text(String.format("%s,%s%s", id, type, coordinateString.toString())),
							NullWritable.get());
				} else {
					// Type is Point
					context.write(new Text(
							String.format("%s,%s,%f,%f", id, type, coordinates.getFloat(0), coordinates.getFloat(1))),
							NullWritable.get());
				}
			} catch (Exception e) {

			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Read a File");

		FileSystem fs = FileSystem.get(conf);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		if (fs.exists(new Path(args[1])))
			fs.delete(new Path(args[1]), true);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setJarByClass(LoadTwitterData.class);
		job.waitForCompletion(true);
	}

}
