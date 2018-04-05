package edu.umn.cs.spatialHadoop.indexing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.io.Text2;

public class RTreeInserter {
	
	private static final String TEMP_APPENDING_PATH = "temp";

	private static Job insertMapReduce(Path currentPath, Path insertPath, OperationsParams params) throws IOException,
			InterruptedException, ClassNotFoundException, InstantiationException, IllegalAccessException {
//		@SuppressWarnings("deprecation")
//		Job job = new Job(params, "RTreeInserter");
//		Configuration conf = job.getConfiguration();
//		job.setJarByClass(RTreeInserter.class);
//
//		// Set input file MBR if not already set
//		Rectangle inputMBR = (Rectangle) OperationsParams.getShape(conf, "mbr");
//		if (inputMBR == null) {
//			inputMBR = FileMBR.fileMBR(currentPath, new OperationsParams(conf));
//			OperationsParams.setShape(conf, "mbr", inputMBR);
//		}
//
//		// Load the partitioner from file
//		String index = conf.get("sindex");
//		if (index == null)
//			throw new RuntimeException("Index type is not set");
//		RTreeFilePartitioner partitioner = new RTreeFilePartitioner();
//		partitioner.createFromMasterFile(currentPath, params);
//		Partitioner.setPartitioner(conf, partitioner);
//
//		// Set mapper and reducer
//		Shape shape = OperationsParams.getShape(conf, "shape");
//		job.setMapperClass(Indexer.PartitionerMap.class);
//		job.setMapOutputKeyClass(IntWritable.class);
//		job.setMapOutputValueClass(shape.getClass());
//		job.setReducerClass(Indexer.PartitionerReduce.class);
//
//		// Set input and output
//		job.setInputFormatClass(SpatialInputFormat3.class);
//		SpatialInputFormat3.setInputPaths(job, insertPath);
//		job.setOutputFormatClass(IndexOutputFormat.class);
//		Path tempPath = new Path(currentPath, "temp");
//		IndexOutputFormat.setOutputPath(job, tempPath);
//
//		// Set number of reduce tasks according to cluster status
//		ClusterStatus clusterStatus = new JobClient(new JobConf()).getClusterStatus();
//		job.setNumReduceTasks(
//				Math.max(1, Math.min(partitioner.getPartitionCount(), (clusterStatus.getMaxReduceTasks() * 9) / 10)));
//
//		// Use multi-threading in case the job is running locally
//		conf.setInt(LocalJobRunner.LOCAL_MAX_MAPS, Runtime.getRuntime().availableProcessors());
//
//		// Start the job
//		if (conf.getBoolean("background", false)) {
//			// Run in background
//			job.submit();
//		} else {
//			job.waitForCompletion(conf.getBoolean("verbose", false));
//		}
//		return job;
		params.setBoolean("isAppending", true);
		params.set("currentPath", currentPath.toString());
		Path tempAppendingPath = new Path(currentPath, TEMP_APPENDING_PATH);
		return Indexer.index(insertPath, tempAppendingPath, params);
	}

	private static void appendNewFiles(Path currentPath, OperationsParams params)
			throws IOException, InterruptedException {
		// Read master file to get all file names
		final byte[] NewLine = new byte[] { '\n' };
		ArrayList<Partition> currentPartitions = MetadataUtil.getPartitions(currentPath, params);
		ArrayList<Partition> insertPartitions = MetadataUtil.getPartitions(new Path(currentPath, TEMP_APPENDING_PATH), params);

		// System.out.println("Insert partition size = " + insertPartitions.size());

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		String sindex = params.get("sindex");

		Path currentMasterPath = new Path(currentPath, "_master." + sindex);
//		Path insertMasterPath = new Path(currentPath, TEMP_APPENDING_PATH + "/_master." + sindex);

		ArrayList<Partition> partitionsToAppend = new ArrayList<Partition>();
		for (Partition insertPartition : insertPartitions) {
			for (Partition currentPartition : currentPartitions) {
				if (insertPartition.cellId == currentPartition.cellId) {
					currentPartition.expand(insertPartition);
					if (!MetadataUtil.isContainedPartition(partitionsToAppend, currentPartition)) {
						partitionsToAppend.add(currentPartition);
					}
				}
			}
		}

		// Append files in temp directory to corresponding files in current path
		for (Partition partition : partitionsToAppend) {
			// System.out.println("Appending to " + partition.filename);
			FSDataOutputStream out;
			Path filePath = new Path(currentPath, partition.filename);
			if (!fs.exists(filePath)) {
				out = fs.create(filePath);
			} else {
				out = fs.append(filePath);
			}
			BufferedReader br = new BufferedReader(
					new InputStreamReader(fs.open(new Path(currentPath, TEMP_APPENDING_PATH + "/" + partition.filename))));
			String line;
			PrintWriter writer = new PrintWriter(out);
			do {
				line = br.readLine();
				if (line != null) {
					writer.append("\n" + line);
				}
			} while (line != null);
			writer.close();
			out.close();
		}

		// Update master and wkt file
		Path currentWKTPath = new Path(currentPath, "_" + sindex + ".wkt");
		fs.delete(currentWKTPath);
		fs.delete(currentMasterPath);
		PrintStream wktOut = new PrintStream(fs.create(currentWKTPath));
		wktOut.println("ID\tBoundaries\tRecord Count\tSize\tFile name");
		OutputStream masterOut = fs.create(currentMasterPath);
		for (Partition currentPartition : currentPartitions) {
			Text masterLine = new Text2();
			currentPartition.toText(masterLine);
			masterOut.write(masterLine.getBytes(), 0, masterLine.getLength());
			masterOut.write(NewLine);
			wktOut.println(currentPartition.toWKT());
		}

		wktOut.close();
		masterOut.close();
		fs.delete(new Path(currentPath, TEMP_APPENDING_PATH));
		fs.close();
		System.out.println("Complete!");
	}

	private static void printUsage() {
		System.out.println(
				"Insert data from a file to another file with same type of shape, using RTree ChooseLeaf mechanism");
		System.out.println("Parameters (* marks required parameters):");
		System.out.println("<original file> - (*) Path to original file");
		System.out.println("<new file> - (*) Path to new file");
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}

	public static void append(Path currentPath, Path insertPath, OperationsParams params) throws ClassNotFoundException,
			InstantiationException, IllegalAccessException, IOException, InterruptedException {
		insertMapReduce(currentPath, insertPath, params);
//		appendNewFiles(currentPath, params);
	}

//	public static void append(Path currentPath, String localInsertPath, OperationsParams params) throws IOException {
//		long t1 = System.currentTimeMillis();
//		// Create partitioner
//		RTreeFilePartitioner partitioner = new RTreeFilePartitioner();
//		partitioner.createFromMasterFile(currentPath, params);
//
//		// Read the append file and assign record to a map of (cellId) -> {list of
//		// records}
//		HashMap<Integer, List<Shape>> map = new HashMap<Integer, List<Shape>>();
//		BufferedReader br = new BufferedReader(new FileReader(localInsertPath));
//		try {
//			String line = br.readLine();
//
//			while (line != null) {
//				// Convert line to shape
//				Text text = new Text(line);
//				OSMPoint shape = new OSMPoint();
//				shape.fromText(text);
//				// Find the corresponding cellId
//				int partitionId = partitioner.overlapPartition(shape);
//				if (map.get(partitionId) == null) {
//					map.put(partitionId, new ArrayList<Shape>());
//				}
//				map.get(partitionId).add(shape);
//
//				// Read next record
//				line = br.readLine();
//			}
//		} finally {
//			br.close();
//		}
//
//		// Append the mapped records to HDFS files
//
//		long t2 = System.currentTimeMillis();
//		System.out.println("Total appending time in millis " + (t2 - t1));
//	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException,
			InstantiationException, IllegalAccessException {
		// TODO Auto-generated method stub
		final OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
		// Path[] inputFiles = params.getPaths();
		//
		// if (!params.checkInput() || (inputFiles.length != 2)) {
		// printUsage();
		// System.exit(1);
		// }
		//
		// Path currentPath = inputFiles[0];
		// Path insertPath = inputFiles[1];
		// System.out.println("Current path: " + currentPath);
		// System.out.println("Insert path: " + insertPath);
		// insertMapReduce(currentPath, insertPath, params);
		// appendNewFiles(currentPath, params);

		params.setBoolean("isAppending", true);
		String currentPathString = params.get("current");
		String appendPathString = params.get("append");
		Path currentPath = new Path(currentPathString);
		Path appendPath = new Path(appendPathString);
		append(currentPath, appendPath, params);
	}
}
