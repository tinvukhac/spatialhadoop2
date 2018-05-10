package edu.umn.cs.spatialHadoop.indexing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.io.Text2;
import edu.umn.cs.spatialHadoop.operations.FileMBR;

public class LSMRTreeIndexer {

	private static final int COMPACTION_MIN_COMPONENT = 3;
	private static final int COMPACTION_MAX_COMPONENT = 5;
	private static final double COMPACTION_RATIO = 1.0;
	private static final int BUFFER_LINES = 100000;
	private static final Log LOG = LogFactory.getLog(Indexer.class);

	static class RTreeComponent {
		private String name;
		private Double size;

		private RTreeComponent(String name, Double size) {
			this.setName(name);
			this.setSize(size);
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Double getSize() {
			return size;
		}

		public void setSize(Double size) {
			this.size = size;
		}
	}

	// Read the input by buffer then put them to a temporary input
	private static void insertByBuffer(Path inputPath, Path outputPath, OperationsParams params) throws IOException,
			ClassNotFoundException, InstantiationException, IllegalAccessException, InterruptedException {
		FileSystem fs = FileSystem.get(new Configuration());
		Path tempInputPath = new Path("./", "temp.input");
		FSDataOutputStream out = fs.create(tempInputPath, true);
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inputPath)));
		String line;
		PrintWriter writer = new PrintWriter(out);
		int count = 0;
		int componentId = 0;

		do {
			line = br.readLine();
			if (line != null) {
				count++;
				writer.append(line);
				if (count == BUFFER_LINES) {
					writer.close();
					out.close();
					// Build R-Tree index from temp file
					componentId++;
					Path componentPath = new Path(outputPath, String.format("%d", componentId));
					OperationsParams params2 = params;
					params2.set("sindex", "str");
					Rectangle tempInputMBR = FileMBR.fileMBR(tempInputPath, params);
					params2.set("mbr", String.format("%f,%f,%f,%f", tempInputMBR.x1, tempInputMBR.y1, tempInputMBR.x2,
							tempInputMBR.y2));
					params2.setBoolean("local", true);
					Indexer.index(tempInputPath, componentPath, params2);
					ArrayList<RTreeComponent> componentsToMerge = checkPolicy(outputPath);
					if (componentsToMerge.size() > 0) {
						merge(outputPath, componentsToMerge, params);
					}

					fs = FileSystem.get(new Configuration());
					out = fs.create(tempInputPath, true);
					writer = new PrintWriter(out);
					count = 0;
				} else {
					writer.append("\n");
				}
			} else {
				writer.close();
				out.close();
			}
		} while (line != null);
		fs.close();
	}

	private static void insertMapReduce(Path currentPath, Path appendPath, OperationsParams params)
			throws IOException, InterruptedException, ClassNotFoundException {
		// Create a new component with R-Tree index
		int maxComponentId = getMaxComponentId(currentPath);
		System.out.println("max compoment id = " + maxComponentId);
		int componentId = maxComponentId + 1;
		Path componentPath = new Path(currentPath, String.format("%d", componentId));
		OperationsParams params2 = params;
		params2.set("sindex", "rtree2");
		Rectangle tempInputMBR = FileMBR.fileMBR(appendPath, params);
		params2.set("mbr",
				String.format("%f,%f,%f,%f", tempInputMBR.x1, tempInputMBR.y1, tempInputMBR.x2, tempInputMBR.y2));
//		params2.setBoolean("local", false);
		Indexer.index(appendPath, componentPath, params2);

		// Check policy to merge
		ArrayList<RTreeComponent> componentsToMerge = checkPolicy(currentPath);
		if (componentsToMerge.size() > 0) {
			merge(currentPath, componentsToMerge, params);
		}
	}

	private static ArrayList<RTreeComponent> checkPolicy(Path path) throws IOException {
		ArrayList<RTreeComponent> componentsToMerge = new ArrayList<LSMRTreeIndexer.RTreeComponent>();
		ArrayList<RTreeComponent> rtreeComponents = getComponents(path);
		for (RTreeComponent component : rtreeComponents) {
			System.out.println("component " + component.name + " has size " + component.size);
		}

		if (rtreeComponents.size() < COMPACTION_MIN_COMPONENT || rtreeComponents.size() > COMPACTION_MAX_COMPONENT) {
			componentsToMerge.clear();
			return componentsToMerge;
		}

		Collections.sort(rtreeComponents, new Comparator<RTreeComponent>() {
			@Override
			public int compare(RTreeComponent o1, RTreeComponent o2) {
				return Integer.parseInt(o1.getName()) - Integer.parseInt(o2.getName());
			}
		});
		System.out.println("sorted components:");
		for (RTreeComponent component : rtreeComponents) {
			System.out.println("component " + component.name + " has size " + component.size);
		}

		for (int i = 0; i < rtreeComponents.size(); i++) {
			RTreeComponent currentComponent = rtreeComponents.get(i);
			double sum = 0;
			for (int j = i + 1; j < rtreeComponents.size(); j++) {
				sum += rtreeComponents.get(j).getSize();
			}
			if (currentComponent.getSize() < COMPACTION_RATIO * sum
					&& (rtreeComponents.size() - i >= COMPACTION_MIN_COMPONENT)
					&& (rtreeComponents.size() - i <= COMPACTION_MAX_COMPONENT)) {
				componentsToMerge.clear();
				for (int j = i; j < rtreeComponents.size(); j++) {
					componentsToMerge.add(rtreeComponents.get(j));
				}
			}
		}

		// for (int i = rtreeComponents.size() - 1; i >= 0; i--) {
		// RTreeComponent currentComponent = rtreeComponents.get(i);
		// double sum = 0;
		// if(i >= 1) {
		// for (int j = i - 1; j >= 0; j--) {
		// sum += rtreeComponents.get(j).getSize();
		// }
		// }
		// if (currentComponent.getSize() < COMPACTION_RATIO * sum) {
		// componentsToMerge.clear();
		// for (int k = i; k >= 0; k--) {
		// componentsToMerge.add(rtreeComponents.get(k));
		// }
		// break;
		// }
		// }

		return componentsToMerge;
	}

	private static ArrayList<RTreeComponent> getComponents(Path path) throws IOException {
		ArrayList<RTreeComponent> rtreeComponents = new ArrayList<LSMRTreeIndexer.RTreeComponent>();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		HashSet<String> components = new HashSet<String>();

		// the second boolean parameter here sets the recursion to true
		RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(path, true);
		while (fileStatusListIterator.hasNext()) {
			LocatedFileStatus fileStatus = fileStatusListIterator.next();
			String filePath = fileStatus.getPath().toString();
			String componentPath = filePath.substring(0, filePath.lastIndexOf("/"));
			String component = componentPath.substring(componentPath.lastIndexOf("/") + 1);
			components.add(component);
		}

		for (String component : components) {
			rtreeComponents.add(new RTreeComponent(component,
					new Double(fs.getContentSummary(new Path(path, component)).getSpaceConsumed())));
			// if(rtreeComponents.size() > COMPACTION_MAX_COMPONENT) {
			// break;
			// }
		}

		// if(rtreeComponents.size() < COMPACTION_MIN_COMPONENT) {
		// rtreeComponents.clear();
		// }
		//
		return rtreeComponents;
	}

	private static int getMaxComponentId(Path path) throws IOException {
		ArrayList<RTreeComponent> rtreeComponents = new ArrayList<LSMRTreeIndexer.RTreeComponent>();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		HashSet<String> components = new HashSet<String>();

		// the second boolean parameter here sets the recursion to true
		RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(path, true);
		while (fileStatusListIterator.hasNext()) {
			LocatedFileStatus fileStatus = fileStatusListIterator.next();
			String filePath = fileStatus.getPath().toString();
			String componentPath = filePath.substring(0, filePath.lastIndexOf("/"));
			String component = componentPath.substring(componentPath.lastIndexOf("/") + 1);
			components.add(component);
		}

		int maxComponentId = -1;
		for (String component : components) {
			int componentId = Integer.parseInt(component);
			if (maxComponentId < componentId) {
				maxComponentId = componentId;
			}
		}

		return maxComponentId;
	}

	public static HashSet<String> getComponentList(Path path) throws IOException {
		ArrayList<RTreeComponent> rtreeComponents = new ArrayList<LSMRTreeIndexer.RTreeComponent>();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		HashSet<String> components = new HashSet<String>();

		// the second boolean parameter here sets the recursion to true
		RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(path, true);
		while (fileStatusListIterator.hasNext()) {
			LocatedFileStatus fileStatus = fileStatusListIterator.next();
			String filePath = fileStatus.getPath().toString();
			String componentPath = filePath.substring(0, filePath.lastIndexOf("/"));
			String component = componentPath.substring(componentPath.lastIndexOf("/") + 1);
			components.add(component);
		}

		return components;
	}

	private static void merge(Path path, ArrayList<RTreeComponent> componentsToMerge, OperationsParams params)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		String sindex = params.get("sindex");

		System.out.println("merging following components:");
		double maxsize = componentsToMerge.get(0).getSize();
		String maxsizeComponent = componentsToMerge.get(0).getName();
		for (RTreeComponent component : componentsToMerge) {
			if (component.getSize() > maxsize) {
				maxsizeComponent = component.getName();
			}
		}

		Rectangle tempInputMBR = new Rectangle(0, 0, 0, 0);
		Path tempMergePath = new Path("./", "temp.merge");
		if(fs.exists(tempMergePath)) {
			fs.delete(tempMergePath);
		}
		fs.mkdirs(tempMergePath);
		for (RTreeComponent component : componentsToMerge) {
			FileStatus[] dataFiles = fs.listStatus(new Path(path, component.getName()), new PathFilter() {
				@Override
				public boolean accept(Path path) {
					return path.getName().contains("part-");
				}
			});
			
			// Read master file to get MBR
//			ArrayList<Partition> currentPartitions = new ArrayList<Partition>();
			Path currentMasterPath = new Path(new Path(path, component.getName()), "_master." + sindex);
			Text tempLine = new Text2();
			LineReader in = new LineReader(fs.open(currentMasterPath));
			while (in.readLine(tempLine) > 0) {
				Partition tempPartition = new Partition();
				tempPartition.fromText(tempLine);
				tempInputMBR.expand(tempPartition);
//				currentPartitions.add(tempPartition);
			}
			
			for (FileStatus file : dataFiles) {
				// Move file to temp merge directory
//				System.out.println(file.getPath());
				String filePath = file.getPath().toString();
				fs.rename(file.getPath(), new Path(tempMergePath,
						filePath.substring(filePath.lastIndexOf("/") + 1) + "-" + component.getName()));
			}
			fs.delete(new Path(path, component.getName()));
		}

		OperationsParams params2 = params;
		params2.set("sindex", "rtree2");
//		Rectangle tempInputMBR = FileMBR.fileMBR(tempMergePath, params);
		params2.set("mbr",
				String.format("%f,%f,%f,%f", tempInputMBR.x1, tempInputMBR.y1, tempInputMBR.x2, tempInputMBR.y2));
//		params2.setBoolean("local", true);
		Indexer.index(tempMergePath, new Path(path, maxsizeComponent), params2);

		fs.delete(tempMergePath);
	}

	private static void printUsage() {
		System.out.println("Index data using LSM with R-Tree component");
		System.out.println("Parameters (* marks required parameters):");
		System.out.println("<original file> - (*) Path to original file");
		System.out.println("<new file> - (*) Path to new file");
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException,
			InstantiationException, IllegalAccessException {

		OperationsParams params = new OperationsParams(new GenericOptionsParser(args));

		String currentPathString = params.get("current");
		String appendPathString = params.get("append");
		Path currentPath = new Path(currentPathString);
		Path appendPath = new Path(appendPathString);

		System.out.println("Current index path: " + currentPath);
		System.out.println("Append data path: " + appendPath);

		// The spatial index to use
		long t1 = System.currentTimeMillis();
		// Append new data to old data
		insertMapReduce(currentPath, appendPath, params);
		// checkPolicy(currentPath);
		long t2 = System.currentTimeMillis();

		long t3 = System.currentTimeMillis();
		System.out.println("Total appending time in millis " + (t2 - t1));
		System.out.println("Total repartitioning time in millis " + (t3 - t2));
		System.out.println("Total time in millis " + (t3 - t1));
	}
}
