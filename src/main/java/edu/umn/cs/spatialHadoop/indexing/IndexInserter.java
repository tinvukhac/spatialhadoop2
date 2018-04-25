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

public class IndexInserter {

	public enum InsertType {
		RTReeAppending, LSMTreeFlushing
	}

	// private static final String TEMP_APPENDING_PATH = "temp";
	//
	// private static Job insertMapReduce(Path currentPath, Path insertPath,
	// OperationsParams params) throws IOException,
	// InterruptedException, ClassNotFoundException, InstantiationException,
	// IllegalAccessException {
	// params.setBoolean("isAppending", true);
	// params.set("currentPath", currentPath.toString());
	// Path tempAppendingPath = new Path(currentPath, TEMP_APPENDING_PATH);
	// return Indexer.index(insertPath, tempAppendingPath, params);
	// }
	//
	// private static void appendNewFiles(Path currentPath, OperationsParams params)
	// throws IOException, InterruptedException {
	// // Read master file to get all file names
	// final byte[] NewLine = new byte[] { '\n' };
	// ArrayList<Partition> currentPartitions =
	// MetadataUtil.getPartitions(currentPath, params);
	// ArrayList<Partition> insertPartitions = MetadataUtil.getPartitions(new
	// Path(currentPath, TEMP_APPENDING_PATH),
	// params);
	//
	// // System.out.println("Insert partition size = " + insertPartitions.size());
	//
	// Configuration conf = new Configuration();
	// FileSystem fs = FileSystem.get(conf);
	// String sindex = params.get("sindex");
	//
	// Path currentMasterPath = new Path(currentPath, "_master." + sindex);
	// // Path insertMasterPath = new Path(currentPath, TEMP_APPENDING_PATH +
	// // "/_master." + sindex);
	//
	// ArrayList<Partition> partitionsToAppend = new ArrayList<Partition>();
	// for (Partition insertPartition : insertPartitions) {
	// for (Partition currentPartition : currentPartitions) {
	// if (insertPartition.cellId == currentPartition.cellId) {
	// currentPartition.expand(insertPartition);
	// if (!MetadataUtil.isContainedPartition(partitionsToAppend, currentPartition))
	// {
	// partitionsToAppend.add(currentPartition);
	// }
	// }
	// }
	// }
	//
	// // Append files in temp directory to corresponding files in current path
	// for (Partition partition : partitionsToAppend) {
	// // System.out.println("Appending to " + partition.filename);
	// FSDataOutputStream out;
	// Path filePath = new Path(currentPath, partition.filename);
	// if (!fs.exists(filePath)) {
	// out = fs.create(filePath);
	// } else {
	// out = fs.append(filePath);
	// }
	// BufferedReader br = new BufferedReader(new InputStreamReader(
	// fs.open(new Path(currentPath, TEMP_APPENDING_PATH + "/" +
	// partition.filename))));
	// String line;
	// PrintWriter writer = new PrintWriter(out);
	// do {
	// line = br.readLine();
	// if (line != null) {
	// writer.append("\n" + line);
	// }
	// } while (line != null);
	// writer.close();
	// out.close();
	// }
	//
	// // Update master and wkt file
	// Path currentWKTPath = new Path(currentPath, "_" + sindex + ".wkt");
	// fs.delete(currentWKTPath);
	// fs.delete(currentMasterPath);
	// PrintStream wktOut = new PrintStream(fs.create(currentWKTPath));
	// wktOut.println("ID\tBoundaries\tRecord Count\tSize\tFile name");
	// OutputStream masterOut = fs.create(currentMasterPath);
	// for (Partition currentPartition : currentPartitions) {
	// Text masterLine = new Text2();
	// currentPartition.toText(masterLine);
	// masterOut.write(masterLine.getBytes(), 0, masterLine.getLength());
	// masterOut.write(NewLine);
	// wktOut.println(currentPartition.toWKT());
	// }
	//
	// wktOut.close();
	// masterOut.close();
	// fs.delete(new Path(currentPath, TEMP_APPENDING_PATH));
	// fs.close();
	// System.out.println("Complete!");
	// }
	//

	private static void printUsage() {
		System.out.println("Insert data from a file to another file with same type of shape");
		System.out.println("Parameters (* marks required parameters):");
		System.out.println("current - (*) Path to original file");
		System.out.println("append - (*) Path to new file");
		System.out.println("inserttype - (*) Insert mechanism: rtreeappending or lsmtreeflushing");
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}

	public static void appendRTree(Path currentPath, Path insertPath, OperationsParams params)
			throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException,
			InterruptedException {
		// insertMapReduce(currentPath, insertPath, params);
		// appendNewFiles(currentPath, params);

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		String sindex = params.get("sindex");

		Path tempPath;
		do {
			tempPath = new Path(currentPath.getParent(), Integer.toString((int) (Math.random() * 1000000)));
		} while (fs.exists(tempPath));

		params.set("currentPath", currentPath.toString());
		Indexer.index(insertPath, tempPath, params);

		// Read master file to get all file names
		ArrayList<Partition> currentPartitions = MetadataUtil.getPartitions(currentPath, params);
		Path tempMasterPath = new Path(tempPath, "_master.cells");
		ArrayList<Partition> insertPartitions = MetadataUtil.getPartitions(tempMasterPath);

		ArrayList<Partition> partitionsToAppend = new ArrayList<Partition>();
		for (Partition insertPartition : insertPartitions) {
			for (Partition currentPartition : currentPartitions) {
				if (insertPartition.cellId == currentPartition.cellId) {
					insertPartition.expand(currentPartition);
					if (!MetadataUtil.isContainedPartition(partitionsToAppend, currentPartition)) {
						partitionsToAppend.add(insertPartition);
					}
				}
			}
		}

		// Append files in temp directory to corresponding files in current path
		for (Partition partition : partitionsToAppend) {
			FSDataOutputStream out;
			Path filePath = new Path(currentPath, partition.filename + "." + sindex);
			if (!fs.exists(filePath)) {
				out = fs.create(filePath);
			} else {
				out = fs.append(filePath);
			}
			BufferedReader br = new BufferedReader(
					new InputStreamReader(fs.open(new Path(tempPath, partition.filename))));
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
		Path currentMasterPath = new Path(currentPath, "_master." + sindex);
		Path currentWKTPath = new Path(currentPath, "_" + sindex + ".wkt");
		MetadataUtil.dumpToFile(currentPartitions, currentMasterPath);
		MetadataUtil.dumpToWKTFile(currentPartitions, currentWKTPath);
		fs.delete(tempPath);
		fs.close();
		System.out.println("Complete!");
	}

	public static void flushLSMTree(Path currentPath, Path insertPath, OperationsParams params) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		// If there is no component so far, pack the current index as the first component
		Path lsmMasterPath = new Path(currentPath, "_master.lsm");
		if(!fs.exists(lsmMasterPath)) {
			int firstComponentId = 1;
			String firstComponentName = "c" + firstComponentId;
			Path parentPath = currentPath.getParent();
			fs.rename(currentPath, new Path(parentPath, firstComponentName));
			fs.mkdirs(currentPath);
			fs.rename(new Path(parentPath, firstComponentName), new Path(currentPath, firstComponentName));
			LSMComponent component = new LSMComponent(firstComponentId, firstComponentName);
			ArrayList<LSMComponent> components = new ArrayList<>();
			components.add(component);
			MetadataUtil.dumpLSMComponentToFile(components, lsmMasterPath);
		}
		
		ArrayList<LSMComponent> components = MetadataUtil.getLSMComponents(lsmMasterPath);
		LSMComponent newComponent = MetadataUtil.createNewLSMComponent(components);
		Path newComponentPath = new Path(currentPath, newComponent.name);
		fs.mkdirs(newComponentPath);

		Indexer.index(insertPath, newComponentPath, params);
	}
	
	public static void insert(Path currentPath, Path insertPath, OperationsParams params) throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException, InterruptedException {
		String insertType = params.get("inserttype");
		
		if (insertType.equals("rtreeappending")) {
			params.setBoolean("isAppending", true);
			appendRTree(currentPath, insertPath, params);
			params.setBoolean("isAppending", false);
		} else if (insertType.equals("lsmtreeflushing")) {
			flushLSMTree(currentPath, insertPath, params);
		} else {
			printUsage();
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException,
			InstantiationException, IllegalAccessException {
		// TODO Auto-generated method stub
		final OperationsParams params = new OperationsParams(new GenericOptionsParser(args));

		String currentPathString = params.get("current");
		String insertPathString = params.get("insert");
		String insertType = params.get("inserttype");

		Path currentPath = new Path(currentPathString);
		Path insertPath = new Path(insertPathString);

		if (insertType.equals("rtreeappending")) {
			params.setBoolean("isAppending", true);
			params.set("currentPath", currentPath.toString());
			appendRTree(currentPath, insertPath, params);
		} else if (insertType.equals("lsmtreeflushing")) {
			flushLSMTree(currentPath, insertPath, params);
		} else {
			printUsage();
		}
	}
}
