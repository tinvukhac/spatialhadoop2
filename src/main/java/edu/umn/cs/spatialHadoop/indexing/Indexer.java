/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.indexing;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.indexing.IndexOutputFormat.IndexRecordWriter;
import edu.umn.cs.spatialHadoop.io.Text2;
import edu.umn.cs.spatialHadoop.mapreduce.RTreeRecordReader3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialRecordReader3;
import edu.umn.cs.spatialHadoop.nasa.HDFRecordReader;
import edu.umn.cs.spatialHadoop.operations.FileMBR;
import edu.umn.cs.spatialHadoop.operations.Sampler;
import edu.umn.cs.spatialHadoop.util.FileUtil;

/**
 * @author Ahmed Eldawy
 *
 */
public class Indexer {
	private static final Log LOG = LogFactory.getLog(Indexer.class);

	private static final Map<String, Class<? extends Partitioner>> PartitionerClasses;
	private static final Map<String, Class<? extends LocalIndexer>> LocalIndexes;
	private static final Map<String, Boolean> PartitionerReplicate;

	static {
		PartitionerClasses = new HashMap<String, Class<? extends Partitioner>>();
		PartitionerClasses.put("grid", GridPartitioner.class);
		PartitionerClasses.put("str", STRPartitioner.class);
		PartitionerClasses.put("str+", STRPartitioner.class);
		PartitionerClasses.put("rtree", RStarTreePartitioner.class);
		PartitionerClasses.put("r+tree", RStarTreePartitioner.class);
		PartitionerClasses.put("quadtree", QuadTreePartitioner.class);
		PartitionerClasses.put("zcurve", ZCurvePartitioner.class);
		PartitionerClasses.put("hilbert", HilbertCurvePartitioner.class);
		PartitionerClasses.put("kdtree", KdTreePartitioner.class);
		PartitionerClasses.put("r*tree", RStarTreePartitioner.class);
		PartitionerClasses.put("r*tree+", RStarTreePartitioner.class);

		PartitionerReplicate = new HashMap<String, Boolean>();
		PartitionerReplicate.put("grid", true);
		PartitionerReplicate.put("str", false);
		PartitionerReplicate.put("str+", true);
		PartitionerReplicate.put("rtree", false);
		PartitionerReplicate.put("r+tree", true);
		PartitionerReplicate.put("quadtree", true);
		PartitionerReplicate.put("zcurve", false);
		PartitionerReplicate.put("hilbert", false);
		PartitionerReplicate.put("kdtree", true);
		PartitionerReplicate.put("r*tree", false);
		PartitionerReplicate.put("r*tree+", true);

		LocalIndexes = new HashMap<String, Class<? extends LocalIndexer>>();
		LocalIndexes.put("rtree", RTreeLocalIndexer.class);
		LocalIndexes.put("r+tree", RTreeLocalIndexer.class);
	}

	/**
	 * The map function that partitions the data using the configured partitioner
	 * 
	 * @author Eldawy
	 *
	 */
	public static class PartitionerMap extends Mapper<Rectangle, Iterable<? extends Shape>, IntWritable, Shape> {

		/** The partitioner used to partitioner the data across reducers */
		private Partitioner partitioner;
		/**
		 * Whether to replicate a record to all overlapping partitions or to assign it
		 * to only one partition
		 */
		private boolean replicate;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.partitioner = Partitioner.getPartitioner(context.getConfiguration());
			this.replicate = context.getConfiguration().getBoolean("replicate", false);
		}

		@Override
		protected void map(Rectangle key, Iterable<? extends Shape> shapes, final Context context)
				throws IOException, InterruptedException {
			final IntWritable partitionID = new IntWritable();
			for (final Shape shape : shapes) {
				Rectangle shapeMBR = shape.getMBR();
				if (shapeMBR == null)
					continue;
				if (replicate) {
					partitioner.overlapPartitions(shape, new ResultCollector<Integer>() {
						@Override
						public void collect(Integer r) {
							partitionID.set(r);
							try {
								context.write(partitionID, shape);
							} catch (IOException e) {
								LOG.warn("Error checking overlapping partitions", e);
							} catch (InterruptedException e) {
								LOG.warn("Error checking overlapping partitions", e);
							}
						}
					});
				} else {
					partitionID.set(partitioner.overlapPartition(shape));
					if (partitionID.get() >= 0)
						context.write(partitionID, shape);
				}
				context.progress();
			}
		}
	}

	public static class PartitionerReduce<S extends Shape> extends Reducer<IntWritable, Shape, IntWritable, Shape> {

		@Override
		protected void reduce(IntWritable partitionID, Iterable<Shape> shapes, Context context)
				throws IOException, InterruptedException {
			LOG.info("Working on partition #" + partitionID);
			for (Shape shape : shapes) {
				context.write(partitionID, shape);
				context.progress();
			}
			// Indicate end of partition to close the file
			context.write(new IntWritable(-partitionID.get() - 1), null);
			LOG.info("Done with partition #" + partitionID);
		}
	}

	private static Job indexMapReduce(Path[] inPaths, Path outPath, OperationsParams paramss)
			throws IOException, InterruptedException, ClassNotFoundException {
		Job job = new Job(paramss, "Indexer");
		Configuration conf = job.getConfiguration();
		job.setJarByClass(Indexer.class);

		// Set input file MBR if not already set
		Rectangle inputMBR = (Rectangle) OperationsParams.getShape(conf, "mbr");
		if (inputMBR == null) {
			System.out.println("MBR is not set");
			inputMBR = FileMBR.fileMBR(inPaths, new OperationsParams(conf));
			OperationsParams.setShape(conf, "mbr", inputMBR);
		}

		// Set the correct partitioner according to index type
		String index = conf.get("sindex");
		if (index == null)
			throw new RuntimeException("Index type is not set");
		long t1 = System.currentTimeMillis();
		setLocalIndexer(conf, index);
		Partitioner partitioner;
		boolean isAppending = paramss.getBoolean("isAppending", false);
		if (isAppending) {
			Path currentPath = new Path(paramss.get("currentPath"));
			ArrayList<Partition> partitions = MetadataUtil.getPartitions(currentPath, paramss);
			CellInfo[] cells = new CellInfo[partitions.size()];
			for (int i = 0; i < cells.length; i++) {
				cells[i] = new CellInfo(partitions.get(i));
			}
			partitioner = new CellPartitioner(cells);
		} else {
			partitioner = createPartitioner(inPaths, outPath, conf, index);
		}
		Partitioner.setPartitioner(conf, partitioner);

		long t2 = System.currentTimeMillis();
		System.out.println("Total time for space subdivision in millis: " + (t2 - t1));

		// Set mapper and reducer
		Shape shape = OperationsParams.getShape(conf, "shape");
		job.setMapperClass(PartitionerMap.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(shape.getClass());
		job.setReducerClass(PartitionerReduce.class);
		// Set input and output
		job.setInputFormatClass(SpatialInputFormat3.class);
		SpatialInputFormat3.setInputPaths(job, inPaths);
		job.setOutputFormatClass(IndexOutputFormat.class);
		IndexOutputFormat.setOutputPath(job, outPath);
		// Set number of reduce tasks according to cluster status
		ClusterStatus clusterStatus = new JobClient(new JobConf()).getClusterStatus();
		job.setNumReduceTasks(
				Math.max(1, Math.min(partitioner.getPartitionCount(), (clusterStatus.getMaxReduceTasks() * 9) / 10)));

		// Use multithreading in case the job is running locally
		conf.setInt(LocalJobRunner.LOCAL_MAX_MAPS, Runtime.getRuntime().availableProcessors());

		// Start the job
		if (conf.getBoolean("background", false)) {
			// Run in background
			job.submit();
		} else {
			job.waitForCompletion(conf.getBoolean("verbose", false));
		}
		return job;
	}

	/**
	 * Set the local indexer for the given job configuration.
	 * 
	 * @param conf
	 * @param sindex
	 */
	private static void setLocalIndexer(Configuration conf, String sindex) {
		Class<? extends LocalIndexer> localIndexerClass = LocalIndexes.get(sindex);
		if (localIndexerClass != null)
			conf.setClass(LocalIndexer.LocalIndexerClass, localIndexerClass, LocalIndexer.class);
	}

	public static Partitioner createPartitioner(Path in, Path out, Configuration job, String partitionerName)
			throws IOException, ClassNotFoundException, InterruptedException {
		return createPartitioner(new Path[] { in }, out, job, partitionerName);
	}

	/**
	 * Create a partitioner for a particular job
	 * 
	 * @param ins
	 * @param out
	 * @param job
	 * @param partitionerName
	 * @return
	 * @throws IOException
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static Partitioner createPartitioner(Path[] ins, Path out, Configuration job, String partitionerName)
			throws IOException, ClassNotFoundException, InterruptedException {
		try {
			Partitioner partitioner;
			Class<? extends Partitioner> partitionerClass = PartitionerClasses.get(partitionerName.toLowerCase());
			if (partitionerClass == null) {
				// Try to parse the name as a class name
				try {
					partitionerClass = Class.forName(partitionerName).asSubclass(Partitioner.class);
				} catch (ClassNotFoundException e) {
					throw new RuntimeException("Unknown index type '" + partitionerName + "'");
				}
			}

			if (PartitionerReplicate.containsKey(partitionerName.toLowerCase())) {
				boolean replicate = PartitionerReplicate.get(partitionerName.toLowerCase());
				job.setBoolean("replicate", replicate);
			}
			partitioner = partitionerClass.newInstance();

			long t1 = System.currentTimeMillis();
			final Rectangle inMBR = (Rectangle) OperationsParams.getShape(job, "mbr");
			// Determine number of partitions
			long inSize = 0;
			for (Path in : ins) {
				inSize += FileUtil.getPathSize(in.getFileSystem(job), in);
			}
			long estimatedOutSize = (long) (inSize * (1.0 + job.getFloat(SpatialSite.INDEXING_OVERHEAD, 0.1f)));
			FileSystem outFS = out.getFileSystem(job);
			long outBlockSize = outFS.getDefaultBlockSize(out);

//			final List<Point> sample = new ArrayList<Point>();
//			float sample_ratio = job.getFloat(SpatialSite.SAMPLE_RATIO, 0.01f);
//			long sample_size = job.getLong(SpatialSite.SAMPLE_SIZE, 100 * 1024 * 1024);
//
//			LOG.info("Reading a sample of " + (int) Math.round(sample_ratio * 100) + "%");
//			ResultCollector<Point> resultCollector = new ResultCollector<Point>() {
//				@Override
//				public void collect(Point p) {
//					sample.add(p.clone());
//				}
//			};
//
//			OperationsParams params2 = new OperationsParams(job);
//			params2.setFloat("ratio", sample_ratio);
//			params2.setLong("size", sample_size);
//			if (job.get("shape") != null)
//				params2.set("shape", job.get("shape"));
//			if (job.get("local") != null)
//				params2.set("local", job.get("local"));
//			params2.setClass("outshape", Point.class, Shape.class);
//			Sampler.sample(ins, resultCollector, params2);
			OperationsParams sampleParams = new OperationsParams(job);
			sampleParams.setClass("outshape", Point.class, Shape.class);
			final String[] sample = Sampler.takeSample(ins, sampleParams);
			Point[] samplePoints = new Point[sample.length];
			for (int i = 0; i < sample.length; i++) {
				samplePoints[i] = new Point();
				samplePoints[i].fromText(new Text(sample[i]));
			}
			long t2 = System.currentTimeMillis();
			System.out.println("Total time for sampling in millis: " + (t2 - t1));
			LOG.info("Finished reading a sample of " + samplePoints.length + " records");

			int partitionCapacity = (int) Math.max(1,
					Math.floor((double) samplePoints.length * outBlockSize / estimatedOutSize));
			int numPartitions = Math.max(1, (int) Math.ceil((float) estimatedOutSize / outBlockSize));
			LOG.info("Partitioning the space into " + numPartitions + " partitions with capacity of "
					+ partitionCapacity);

			partitioner.createFromPoints(inMBR, samplePoints, partitionCapacity);

			return partitioner;
		} catch (InstantiationException e) {
			e.printStackTrace();
			return null;
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			return null;
		}
	}

	private static void indexLocal(Path[] inPaths, final Path outPath, Partitioner p, OperationsParams params)
			throws IOException, InterruptedException {
		Job job = Job.getInstance(params);
		final Configuration conf = job.getConfiguration();

		final boolean disjoint = params.getBoolean(Partitioner.PartitionerDisjoint, false);
		String globalIndexExtension = p.getClass().getAnnotation(Partitioner.GlobalIndexerMetadata.class).extension();

		// Start reading input file
		List<InputSplit> splits = new ArrayList<InputSplit>();
		final SpatialInputFormat3<Rectangle, Shape> inputFormat = new SpatialInputFormat3<Rectangle, Shape>();
		for (Path inPath : inPaths) {
			FileSystem inFs = inPath.getFileSystem(conf);
			FileStatus inFStatus = inFs.getFileStatus(inPath);
			if (inFStatus != null && !inFStatus.isDir()) {
				// One file, retrieve it immediately.
				// This is useful if the input is a hidden file which is automatically
				// skipped by FileInputFormat. We need to plot a hidden file for the case
				// of plotting partition boundaries of a spatial index
				splits.add(new FileSplit(inPath, 0, inFStatus.getLen(), new String[0]));
			} else {
				SpatialInputFormat3.addInputPath(job, inPath);
				for (InputSplit s : inputFormat.getSplits(job))
					splits.add(s);
			}
		}

		// Copy splits to a final array to be used in parallel
		final FileSplit[] fsplits = splits.toArray(new FileSplit[splits.size()]);

		// Set input file MBR if not already set
		Rectangle inputMBR = (Rectangle) OperationsParams.getShape(conf, "mbr");
		if (inputMBR == null) {
			inputMBR = new Rectangle(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY,
					Double.NEGATIVE_INFINITY);
			for (Path inPath : inPaths) {
				inputMBR.expand(FileMBR.fileMBR(inPath, new OperationsParams(conf)));
			}
			OperationsParams.setShape(conf, "mbr", inputMBR);
		}

		final IndexRecordWriter<Shape> recordWriter = new IndexRecordWriter<Shape>(p, null, outPath, conf);
		for (FileSplit fsplit : fsplits) {
			RecordReader<Rectangle, Iterable<Shape>> reader = inputFormat.createRecordReader(fsplit, null);
			if (reader instanceof SpatialRecordReader3) {
				((SpatialRecordReader3) reader).initialize(fsplit, conf);
			} else if (reader instanceof HDFRecordReader) {
				((HDFRecordReader) reader).initialize(fsplit, conf);
			} else {
				throw new RuntimeException("Unknown record reader");
			}

			final IntWritable partitionID = new IntWritable();

			while (reader.nextKeyValue()) {
				Iterable<Shape> shapes = reader.getCurrentValue();
				if (disjoint) {
					for (final Shape s : shapes) {
						if (s == null)
							continue;
						Rectangle mbr = s.getMBR();
						if (mbr == null)
							continue;
						p.overlapPartitions(mbr, new ResultCollector<Integer>() {
							@Override
							public void collect(Integer id) {
								partitionID.set(id);
								try {
									recordWriter.write(partitionID, s);
								} catch (IOException e) {
									throw new RuntimeException(e);
								}
							}
						});
					}
				} else {
					for (final Shape s : shapes) {
						if (s == null)
							continue;
						Rectangle mbr = s.getMBR();
						if (mbr == null)
							continue;
						int pid = p.overlapPartition(mbr);
						if (pid != -1) {
							partitionID.set(pid);
							recordWriter.write(partitionID, s);
						}
					}
				}
			}
			reader.close();
		}
		recordWriter.close(null);

		// Write the WKT-formatted master file
		FileSystem outFs = outPath.getFileSystem(params);
		Path masterPath = new Path(outPath, "_master." + globalIndexExtension);
		Partitioner.generateMasterWKT(outFs, masterPath);
	}

	public static Job index(Path inPath, Path outPath, OperationsParams params)
			throws IOException, InterruptedException, ClassNotFoundException {
		return index(new Path[] { inPath }, outPath, params);
	}

	public static Job index(Path[] inPaths, Path outPath, OperationsParams params)
			throws IOException, InterruptedException, ClassNotFoundException {
		// Partitioner p = initializeIndexers(inPaths, outPath, params);
		// if (OperationsParams.isLocal(new JobConf(params), inPaths)) {
		// indexLocal(inPaths, outPath, p, params);
		// return null;
		// } else {
		// Job job = indexMapReduce(inPaths, outPath, p, params);
		// return job;
		// }
//		System.out.println("initializeIndexers");
//		Partitioner p = initializeIndexers(inPaths, outPath, params);
		if (OperationsParams.isLocal(new JobConf(params), inPaths)) {
//			indexLocal(inPaths, outPath, p, params);
			return null;
		} else {
			Job job = indexMapReduce(inPaths, outPath, params);
			if (!job.isSuccessful())
				throw new RuntimeException("Failed job " + job);
			return job;
		}
	}

	/**
	 * Initialize the global and local indexers according to the input file, the
	 * output file, and the user-specified parameters
	 * 
	 * @param inPaths
	 * @param outPath
	 * @param conf
	 * @return the created {@link Partitioner}
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private static Partitioner initializeIndexers(Path[] inPaths, Path outPath, Configuration conf)
			throws IOException, InterruptedException {
		// Set the correct partitioner according to index type
		String sindex = conf.get("sindex");
		SpatialSite.SpatialIndex spatialIndex;
		if (sindex != null && SpatialSite.CommonSpatialIndex.containsKey(sindex))
			spatialIndex = SpatialSite.CommonSpatialIndex.get(sindex);
		else
			spatialIndex = new SpatialSite.SpatialIndex();
		if (conf.get("gindex") != null)
			spatialIndex.gindex = SpatialSite.getGlobalIndex(conf.get("gindex"));
		if (conf.get("lindex") != null)
			spatialIndex.lindex = SpatialSite.getLocalIndex(conf.get("lindex"));
		spatialIndex.disjoint = conf.getBoolean("disjoint", spatialIndex.disjoint);

		if (spatialIndex.lindex != null)
			conf.setClass(LocalIndex.LocalIndexClass, spatialIndex.lindex, LocalIndex.class);

		long t1 = System.currentTimeMillis();
		System.out.println("gindex = " + spatialIndex.gindex);
		Partitioner partitioner = initializeGlobalIndex(inPaths, outPath, conf, spatialIndex.gindex);
		Partitioner.setPartitioner(conf, partitioner);

		long t2 = System.currentTimeMillis();
		System.out.println("Total time for space subdivision in millis: " + (t2 - t1));
		return partitioner;
	}

	public static Partitioner initializeGlobalIndex(Path in, Path out, Configuration job,
			Class<? extends Partitioner> gindex) throws IOException {
		return initializeGlobalIndex(new Path[] { in }, out, job, gindex);
	}

	/**
	 * Create a partitioner for a particular job
	 * 
	 * @param ins
	 * @param out
	 * @param job
	 * @param partitionerClass
	 * @return
	 * @throws IOException
	 */
	public static Partitioner initializeGlobalIndex(Path[] ins, Path out, Configuration job,
			Class<? extends Partitioner> partitionerClass) throws IOException {

		// Determine number of partitions
		long inSize = 0;
		for (Path in : ins)
			inSize += FileUtil.getPathSize(in.getFileSystem(job), in);

		long estimatedOutSize = (long) (inSize * (1.0 + job.getFloat(SpatialSite.INDEXING_OVERHEAD, 0.1f)));
		FileSystem outFS = out.getFileSystem(job);
		long outBlockSize = outFS.getDefaultBlockSize(out);

		try {
			Partitioner partitioner;

			Partitioner.GlobalIndexerMetadata partitionerMetadata = partitionerClass
					.getAnnotation(Partitioner.GlobalIndexerMetadata.class);
			boolean disjointSupported = partitionerMetadata != null && partitionerMetadata.disjoint();

			if (job.getBoolean("disjoint", false) && !disjointSupported)
				throw new RuntimeException(
						"Partitioner " + partitionerClass.getName() + " does not support disjoint partitioning");

			try {
				Constructor<? extends Partitioner> c = partitionerClass.getConstructor(Rectangle.class, int.class);
				// Constructor needs an MBR and number of partitions
				final Rectangle inMBR = SpatialSite.getMBR(job, ins);
				int numOfPartitions = (int) Math.ceil((double) estimatedOutSize / outBlockSize);
				return (Partitioner) c.newInstance(inMBR, numOfPartitions);
			} catch (NoSuchMethodException e) {
				try {
					Constructor<? extends Partitioner> c = partitionerClass.getConstructor(Point[].class, int.class);
					// Constructor needs a sample and capacity (no MBR)
					OperationsParams sampleParams = new OperationsParams(job);
					sampleParams.setClass("outshape", Point.class, Shape.class);
					sampleParams.set("ratio", sampleParams.get(SpatialSite.SAMPLE_RATIO));
					final String[] sample = Sampler.takeSample(ins, sampleParams);
					Point[] samplePoints = new Point[sample.length];
					for (int i = 0; i < sample.length; i++) {
						samplePoints[i] = new Point();
						samplePoints[i].fromText(new Text(sample[i]));
					}
					int partitionCapacity = (int) Math.max(1,
							Math.floor((double) sample.length * outBlockSize / estimatedOutSize));
					return (Partitioner) c.newInstance(samplePoints, partitionCapacity);
				} catch (NoSuchMethodException e1) {
					try {
						Constructor<? extends Partitioner> c = partitionerClass.getConstructor(Rectangle.class,
								Point[].class, int.class);
						final Rectangle inMBR = SpatialSite.getMBR(job, ins);
						OperationsParams sampleParams = new OperationsParams(job);
						sampleParams.setClass("outshape", Point.class, Shape.class);
						final String[] sample = Sampler.takeSample(ins, sampleParams);
						Point[] samplePoints = new Point[sample.length];
						for (int i = 0; i < sample.length; i++) {
							samplePoints[i] = new Point();
							samplePoints[i].fromText(new Text(sample[i]));
						}
						int partitionCapacity = (int) Math.max(1,
								Math.floor((double) sample.length * outBlockSize / estimatedOutSize));
						return (Partitioner) c.newInstance(inMBR, samplePoints, partitionCapacity);
					} catch (NoSuchMethodException e2) {
						throw new RuntimeException("Could not find a suitable constructor for the partitioner "
								+ partitionerClass.getName());
					} catch (InterruptedException e2) {
						e2.printStackTrace();
						return null;
					} catch (ClassNotFoundException e2) {
						e2.printStackTrace();
						return null;
					}
				} catch (InterruptedException e1) {
					e1.printStackTrace();
					return null;
				} catch (ClassNotFoundException e1) {
					e1.printStackTrace();
					return null;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
				return null;
			}
		} catch (InstantiationException e) {
			e.printStackTrace();
			return null;
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			return null;
		} catch (InvocationTargetException e) {
			e.printStackTrace();
			return null;
		}
	}

	// public static Job index(Path inPath, Path outPath, OperationsParams params)
	// throws IOException, InterruptedException, ClassNotFoundException {
	// if (OperationsParams.isLocal(new JobConf(params), inPath)) {
	// indexLocal(inPath, outPath, params);
	// return null;
	// } else {
	// Job job = indexMapReduce(inPath, outPath, params);
	// if (!job.isSuccessful())
	// throw new RuntimeException("Failed job " + job);
	// return job;
	// }
	// }

	protected static void printUsage() {
		System.out.println("Builds a spatial index on an input file");
		System.out.println("Parameters (* marks required parameters):");
		System.out.println("<input file> - (*) Path to input file");
		System.out.println("<output file> - (*) Path to output file");
		System.out.println("shape:<point|rectangle|polygon> - (*) Type of shapes stored in input file");
		System.out.println("sindex:<index> - (*) Type of spatial index (grid|str|str+|quadtree|zcurve|kdtree)");
		System.out.println("-overwrite - Overwrite output file without noitce");
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}

	/**
	 * Entry point to the indexing operation.
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		OperationsParams params = new OperationsParams(new GenericOptionsParser(args));

		if (!params.checkInputOutput(true)) {
			printUsage();
			return;
		}
		if (params.get("sindex") == null) {
			System.err.println("Please specify type of index to build (grid, rtree, r+tree, str, str+)");
			printUsage();
			return;
		}
		Path inputPath = params.getInputPath();
		Path outputPath = params.getOutputPath();

		// The spatial index to use
		long t1 = System.currentTimeMillis();
		index(inputPath, outputPath, params);
		long t2 = System.currentTimeMillis();
		System.out.println("Total indexing time in millis " + (t2 - t1));
	}

}
