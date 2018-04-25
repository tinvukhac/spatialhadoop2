package edu.umn.cs.spatialHadoop.indexing;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;

public class DynamicIndexer {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, InterruptedException {
		OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
		
		String currentPathString = params.get("current");
		String appendPathString = params.get("append");
		Path currentPath = new Path(currentPathString);
		Path appendPath = new Path(appendPathString);

		System.out.println("Current path: " + currentPath);
		System.out.println("Append path: " + appendPath);
		
		// Data flushing
		long t1 = System.currentTimeMillis();
//		IndexInserter.insert(currentPath, appendPath, params);
		long t2 = System.currentTimeMillis();
		System.out.println("Total appending time in millis " + (t2 - t1));
		
		// Partition selection
		ArrayList<ArrayList<Partition>> splitGroups = PartitionSelector.getSplitGroups(currentPath, params);
		long t3 = System.currentTimeMillis();
		System.out.println("Total optimization time in millis " + (t3 - t2));
		System.out.println("Number of groups = " + splitGroups.size());
		
		// Partition splitting
		PartitionSplitter.reorganize(currentPath, splitGroups, params);
		long t4 = System.currentTimeMillis();
		System.out.println("Total splitting time in millis " + (t4 - t3));
		System.out.println("Total dynamic indexing time in millis " + (t4 - t1));
		
//		if(splitType.equals("incrtree")) {
//			ArrayList<Partition> splitPartitions = PartitionSelector.getOverflowPartitions(currentPath, params);
//			long t3 = System.currentTimeMillis();
//			System.out.println("Total optimizing time in millis " + (t3 - t2));
//			MetadataUtil.printPartitionSummary(splitPartitions, blockSize);
//			
//			PartitionSplitter.reorganizePartitions(currentPath, splitPartitions, params);
//			
//			long t4 = System.currentTimeMillis();
//			System.out.println("Total reorganizing time in millis " + (t4 - t3));
//			System.out.println("Total dynamic indexing time in millis " + (t4 - t1));
//		} else {
//			ArrayList<ArrayList<Partition>> splitGroups = new ArrayList<>();
//			if(splitType.equals("greedyreducedcost")) {
//				splitGroups = PartitionSelector.getSplitGroups(currentPath, params, PartitionSelector.OptimizerType.MaximumReducedCost);
//			} else if(splitType.equals("greedyreducedarea")) {
//				splitGroups = PartitionSelector.getSplitGroups(currentPath, params, PartitionSelector.OptimizerType.MaximumReducedArea);
//			}
//			
//			long t3 = System.currentTimeMillis();
//			System.out.println("Total optimizing time in millis " + (t3 - t2));
//			MetadataUtil.printGroupSummary(splitGroups, blockSize);
//			
//			PartitionSplitter.reorganizeGroups(currentPath, splitGroups, params);
//			
//			long t4 = System.currentTimeMillis();
//			System.out.println("Total reorganizing time in millis " + (t4 - t3));
//			System.out.println("Total dynamic indexing time in millis " + (t4 - t1));
//		}
	}
}
