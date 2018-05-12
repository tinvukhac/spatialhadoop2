package edu.umn.cs.spatialHadoop.operations;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
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

import edu.umn.cs.spatialHadoop.util.FileUtil;

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
//				String id = tweet.getString("id_str");
				String type = tweet.getJSONObject("place").getJSONObject("bounding_box").getString("type");

				if (type.equals("Polygon")) {
					// Type is Polygon
//					StringBuilder coordinateString = new StringBuilder();
//					JSONArray polygonCoordinates = coordinates.getJSONArray(0);
//					for (int i = 0; i < polygonCoordinates.length(); i++) {
//						JSONArray point = polygonCoordinates.getJSONArray(i);
//						coordinateString.append(String.format(",%f,%f", point.getFloat(0), point.getFloat(1)));
//					}
//					context.write(new Text(String.format("%s,%s%s", id, type, coordinateString.toString())),
//							NullWritable.get());
					JSONArray coordinates = tweet.getJSONObject("place").getJSONObject("bounding_box")
							.getJSONArray("coordinates");
					JSONArray polygonCoordinates = coordinates.getJSONArray(0);
					JSONArray firstPoint = polygonCoordinates.getJSONArray(0);
					coordinates.getJSONArray(0).put(firstPoint);
					tweet.getJSONObject("place").getJSONObject("bounding_box").put("coordinates", coordinates);
					context.write(new Text(tweet.toString()), NullWritable.get());
				} else {
					// Type is Point
//					context.write(new Text(
//							String.format("%s,%s,%f,%f", id, type, coordinates.getFloat(0), coordinates.getFloat(1))),
//							NullWritable.get());
					context.write(value, NullWritable.get());
				}
			} catch (Exception e) {

			}
		}
	}
	
	private static Job cleanMapReduce(Path[] paths, Path dest) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Clean");
		
		FileSystem fs = FileSystem.get(conf);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
//		if (fs.exists(dest))
//			fs.delete(dest);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(12);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputDirRecursive(job, true);
		for(int i = 0; i < paths.length; i++) {
			FileInputFormat.addInputPath(job, paths[i]);
		}

		FileOutputFormat.setOutputPath(job, dest);
		job.setJarByClass(LoadTwitterData.class);
		job.waitForCompletion(true);
		
		return job;
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
//		Job job = new Job(conf, "Read a File");
//
		FileSystem fs = FileSystem.get(conf);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);
//		if (fs.exists(new Path(args[1])))
//			fs.delete(new Path(args[1]), true);
//		job.setMapperClass(Map.class);
//		job.setReducerClass(Reducer.class);
//		job.setNumReduceTasks(21);
//
//		job.setInputFormatClass(TextInputFormat.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(NullWritable.class);
//
//		FileInputFormat.setInputDirRecursive(job, true);
//		for(int i = 1; i <= 4; i++) {
//			Path path = new Path(String.format("%s/all_tweets_%d", args[0], i));
//			FileInputFormat.addInputPath(job, path);
//		}
////		FileInputFormat.addInputPath(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//		job.setJarByClass(LoadTwitterData.class);
//		job.waitForCompletion(true);
		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		System.out.println("Input: " + inputPath.toString());
		System.out.println("Output: " + outputPath.toString());
//		
//		if(fs.exists(outputPath)) 
//			fs.delete(outputPath);
//		fs.mkdirs(outputPath);
//		RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(inputPath, true);
//		while(fileStatusListIterator.hasNext()){
//			long t1 = System.currentTimeMillis();
//			
//	        LocatedFileStatus fileStatus = fileStatusListIterator.next();
//	        Path filePath = fileStatus.getPath();
//	        String filePathString = filePath.toString();
//	        String fileName = filePathString.substring(filePathString.lastIndexOf("/") + 1, filePathString.length());
//	        Path fileOutputPath = new Path(outputPath, fileName + ".clean");
//	        System.out.println("Filename: " + fileName);
//	        System.out.println("Output file path: " + fileOutputPath.toString());
//	        Path[] filePaths = new Path[1];
//	        filePaths[0] = filePath;
//	        Job job = cleanMapReduce(filePaths, fileOutputPath);
//	        long t2 = System.currentTimeMillis();
//			System.out.println("Total indexing time in millis " + (t2 - t1));
//	        if(job.isSuccessful()) {
//	        	System.out.println("Job succeed in millis: " + (t2 - t1));
//	        } else {
//	        	System.out.println("Job failed in millis: " + (t2 - t1));
//	        }
//	    }
		
		// Concat file
		if(fs.exists(outputPath)) 
			fs.delete(outputPath);
		
		ArrayList<Path> sourcePaths = new ArrayList<Path>();
		RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(inputPath, true);
		while(fileStatusListIterator.hasNext()){
			LocatedFileStatus fileStatus = fileStatusListIterator.next();
			Path filePath = fileStatus.getPath();
			if(!filePath.toString().contains("SUCCESS")) {
				sourcePaths.add(filePath);
			}
		}
		System.out.println("Number of inputs: " + sourcePaths.size());
		if(sourcePaths.size() > 0) {
			if(fs.exists(outputPath)) 
				fs.delete(outputPath);
			FSDataOutputStream out = fs.create(outputPath);
			out.close();
			Path[] sourcePathArray = sourcePaths.toArray(new Path[sourcePaths.size()]);
			FileUtil.concat(conf, fs, false, outputPath, sourcePathArray);
		}
	}

}
