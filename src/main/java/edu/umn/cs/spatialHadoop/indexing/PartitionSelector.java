package edu.umn.cs.spatialHadoop.indexing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.indexing.LSMRTreeIndexer.RTreeComponent;

public class PartitionSelector {
	
	private static final int LSM_COMPACTION_MIN_COMPONENT = 3;
	private static final int LSM_COMPACTION_MAX_COMPONENT = 5;
	private static final double LSM_COMPACTION_RATIO = 1.0;
	
	public enum OptimizerType {
		MaximumReducedCost,
		MaximumReducedArea,
		IncrementalRTree,
		LSMCompaction
	}
	
	// Incremental RTree optimizer
	public static ArrayList<Partition> getOverflowPartitions(Path path, OperationsParams params) throws IOException {
		ArrayList<Partition> overflowPartitions = new ArrayList<Partition>();
		
		ArrayList<Partition> partitions = MetadataUtil.getPartitions(path, params);
		
		Configuration conf = new Configuration();
		int blockSize = Integer.parseInt(conf.get("dfs.blocksize"));
		double overflowRate = Double.parseDouble(params.get("overflow_rate"));
		double overflowSize = blockSize * overflowRate;
		
		for(Partition p: partitions) {
			if(p.size > overflowSize) {
				overflowPartitions.add(p);
			}
		}
		
		return overflowPartitions;
	}
	
	// Greedy algorithm that maximize the reduced range query cost
	@SuppressWarnings("unchecked")
	private static ArrayList<ArrayList<Partition>> getSplitGroupsWithMaximumReducedCost(ArrayList<Partition> partitions, OperationsParams params) throws IOException {
		ArrayList<ArrayList<Partition>> splitGroups = new ArrayList<>();
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		int blockSize = Integer.parseInt(conf.get("dfs.blocksize"));
		double overflowRate = Double.parseDouble(params.get("overflow_rate"));
		double overflowSize = blockSize * overflowRate;
		String sindex = params.get("sindex");
//		double budget = Double.parseDouble(params.get("budget")) * 1024 * 1024;
//		int budgetBlocks = (int) Math.ceil(budget / blockSize);
		
		long incrementalRTreeBudget = 0;
		for(Partition p: partitions) {
			if(p.size > overflowSize) {
				incrementalRTreeBudget += p.size;
			}
		}
		int budgetBlocks = (int) Math.ceil((float)incrementalRTreeBudget / (float)blockSize);
		
		Rectangle mbr = (Rectangle) OperationsParams.getShape(conf, "mbr");
		if (mbr == null) {
			mbr = new Rectangle(partitions.get(0));
			for (Partition p : partitions) {
				mbr.expand(p);
			}
		}
		double querySize = 0.000001 * Math.sqrt(mbr.area());
		
		ArrayList<Partition> remainingPartitions = new ArrayList<Partition>();
		ArrayList<Partition> splitPartitions = new ArrayList<Partition>();
		remainingPartitions = (ArrayList<Partition>) partitions.clone();

		while (budgetBlocks > 0) {
			Partition bestCandidatePartition = findBestCandidateToReduceCost(remainingPartitions, splitPartitions,
					querySize, blockSize);
			splitPartitions.add(bestCandidatePartition);
			remainingPartitions.remove(bestCandidatePartition);
			budgetBlocks -= bestCandidatePartition.getNumberOfBlock(blockSize);
		}
		
		splitGroups = MetadataUtil.groupByOverlappingPartitions(splitPartitions);
		return splitGroups;
	}
	
	private static double computeReducedCost(ArrayList<Partition> splittingPartitions, double querySize,
			int blockSize) {
		// System.out.println("Computing reduced cost of a set of partitions.");
		// Group splitting partitions by overlapping clusters
		ArrayList<ArrayList<Partition>> groups = MetadataUtil.groupByOverlappingPartitions(splittingPartitions);

		// System.out.println("Number of groups = " + groups.size());

		// Compute reduced cost
		double costBefore = 0;
		for (Partition p : splittingPartitions) {
			costBefore += (p.getWidth() + querySize) * (p.getHeight() + querySize) * p.getNumberOfBlock(blockSize);
		}

		double costAfter = 0;
		for (ArrayList<Partition> group : groups) {
			double groupBlocks = 0;
			Partition tempPartition = group.get(0).clone();
			for (Partition p : group) {
				groupBlocks += p.getNumberOfBlock(blockSize);
				tempPartition.expand(p);
			}
			double groupArea = tempPartition.area();

			costAfter += Math.pow((Math.sqrt(groupArea / groupBlocks) + querySize), 2) * groupBlocks;
		}

		// System.out.println("Reduced cost = " + Math.abs(costBefore - costAfter));
		return Math.abs(costBefore - costAfter);
	}
	
	private static Partition findBestCandidateToReduceCost(ArrayList<Partition> currentPartitions,
			ArrayList<Partition> splittingPartitions, double querySize, int blockSize) {
		Partition bestPartition = currentPartitions.get(0);
		double maxReducedCost = 0;
		for (Partition p : currentPartitions) {
			splittingPartitions.add(p);
			double splittingReducedCost = computeReducedCost(splittingPartitions, querySize, blockSize);
			if (maxReducedCost < splittingReducedCost) {
				bestPartition = p;
				maxReducedCost = splittingReducedCost;
			}
			splittingPartitions.remove(p);
		}

		return bestPartition;
	}
	
	// Greedy algorithm that maximize the reduced area
	private static ArrayList<ArrayList<Partition>> getSplitGroupsWithMaximumReducedArea(ArrayList<Partition> partitions, OperationsParams params) throws IOException {
		ArrayList<ArrayList<Partition>> splitGroups = new ArrayList<>();
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		int blockSize = Integer.parseInt(conf.get("dfs.blocksize"));
		double overflowRate = Double.parseDouble(params.get("overflow_rate"));
		double overflowSize = blockSize * overflowRate;
		String sindex = params.get("sindex");
//		double budget = Double.parseDouble(params.get("budget")) * 1024 * 1024;
//		int budgetBlocks = (int) Math.ceil(budget / blockSize);
		
		long incrementalRTreeBudget = 0;
		for(Partition p: partitions) {
			if(p.size > overflowSize) {
				incrementalRTreeBudget += p.size;
			}
		}
		int budgetBlocks = (int) Math.ceil((float)incrementalRTreeBudget / (float)blockSize);
		
		ArrayList<Partition> remainingPartitions = new ArrayList<Partition>();
		ArrayList<Partition> splitPartitions = new ArrayList<Partition>();
		remainingPartitions = (ArrayList<Partition>) partitions.clone();
		
		while (budgetBlocks > 0) {
			Partition bestCandidatePartition = findBestCandidateToReduceArea(remainingPartitions, splitPartitions);
			splitPartitions.add(bestCandidatePartition);
			remainingPartitions.remove(bestCandidatePartition);
			budgetBlocks -= bestCandidatePartition.getNumberOfBlock(blockSize);
		}
		
		splitGroups = MetadataUtil.groupByOverlappingPartitions(splitPartitions);
		return splitGroups;
	}
	
	private static double computeReducedArea(ArrayList<Partition> splittingPartitions, Partition partition) {
		double reducedArea = partition.area();
		for(Partition p: splittingPartitions) {
			if(p.isIntersected(partition)) {
				reducedArea += p.getIntersection(partition).area();
			}
		}
		return reducedArea;
 	}
	
	private static Partition findBestCandidateToReduceArea(ArrayList<Partition> currentPartitions,
			ArrayList<Partition> splittingPartitions) {
		Partition bestPartition = currentPartitions.get(0);
		double maxArea = 0;
		for (Partition p : currentPartitions) {
			splittingPartitions.add(p);
			double splittingReducedArea = computeReducedArea(splittingPartitions, p);
			if (maxArea < splittingReducedArea) {
				bestPartition = p;
				maxArea = splittingReducedArea;
			}
			splittingPartitions.remove(p);
		}
		
		return bestPartition;
	}
	
	/**
	 * Compaction policy: Trying to find the first component that has a size less than the total of 
	 * all smaller components multiplied by compaction ratio. 
	 * Once that file is found, compact that component with all subsequent components
	 * @param components
	 * @return
	 */
	private static ArrayList<LSMComponent> getComponentsToMerge(ArrayList<LSMComponent> components) {
		ArrayList<LSMComponent> componentsToMerge = new ArrayList<LSMComponent>();
		
		// Sort component by component ID (oldest to newest)
		Collections.sort(components, new Comparator<LSMComponent>() {
			@Override
			public int compare(LSMComponent o1, LSMComponent o2) {
				return o1.id - o2.id;
			}
		});
		
		for(int i = 0; i < components.size(); i++) {
			long sum = 0;
			LSMComponent currentComponent = components.get(i);
			for(LSMComponent c: components) {
				if(c.size < currentComponent.size) {
					sum += c.size;
				}
			}
			
			if(currentComponent.size < LSM_COMPACTION_RATIO * sum) {
				for(int j = i; j < components.size(); j++) {
					componentsToMerge.add(components.get(j));
				}
				break;
			}
		}
		
		System.out.println("Component to be merged:");
		for(LSMComponent c: componentsToMerge) {
			System.out.println(c.name);
		}
		
		return componentsToMerge;
	}
	
	public static ArrayList<LSMComponent> getComponentsToMerge(Path path, OperationsParams params) throws IOException {
		Path lsmMasterPath = new Path(path, IndexInserter.LSM_MASTER);
		ArrayList<LSMComponent> components = MetadataUtil.getLSMComponents(lsmMasterPath);
		return getComponentsToMerge(components);
	}
	
	public static ArrayList<ArrayList<Partition>> getSplitGroups(Path path, OperationsParams params) throws IOException {
		String splitType = params.get("splittype");
		if(splitType.equals("greedyreducedcost")) {
			return PartitionSelector.getSplitGroups(path, params, PartitionSelector.OptimizerType.MaximumReducedCost);
		} else if(splitType.equals("greedyreducedarea")) {
			return PartitionSelector.getSplitGroups(path, params, PartitionSelector.OptimizerType.MaximumReducedArea);
		} else if(splitType.equals("incrtree")) {
			return PartitionSelector.getSplitGroups(path, params, PartitionSelector.OptimizerType.IncrementalRTree);
		} 
//		else if(splitType.equals("lsmcompaction")) {
//			return PartitionSelector.getSplitGroups(path, params, PartitionSelector.OptimizerType.LSMCompaction);
//		}
		return null;
	}
	
	
	public static ArrayList<ArrayList<Partition>> getSplitGroups(Path path, OperationsParams params, OptimizerType type) throws IOException {
		ArrayList<ArrayList<Partition>> splitGroups = new ArrayList<>();
		
		ArrayList<Partition> partitions = MetadataUtil.getPartitions(path, params);
		
		if(type == OptimizerType.MaximumReducedCost) {
			splitGroups = getSplitGroupsWithMaximumReducedCost(partitions, params);
		} else if(type == OptimizerType.MaximumReducedArea) {
			splitGroups = getSplitGroupsWithMaximumReducedArea(partitions, params);
		} else if(type == OptimizerType.IncrementalRTree) {
			ArrayList<Partition> overflowPartitions = getOverflowPartitions(path, params);
			for(Partition partition: overflowPartitions) {
				ArrayList<Partition> group = new ArrayList<>();
				group.add(partition);
				splitGroups.add(group);
			}
		} 
//		else if(type == OptimizerType.LSMCompaction) {
//			ArrayList<Partition> partitionsToMerge = new ArrayList<Partition>();
//			Path lsmMasterPath = new Path(path, IndexInserter.LSM_MASTER);
//			ArrayList<LSMComponent> components = MetadataUtil.getLSMComponents(lsmMasterPath);
//			ArrayList<LSMComponent> componentsToMerge = getComponentsToMerge(components);
//			
//			if(componentsToMerge.size() > 1) {
//				for(Partition p: partitions) {
//					String componentName = p.filename.split("/")[0];
//					for(LSMComponent c: componentsToMerge) {
//						if(c.name.equals(componentName)) {
//							partitionsToMerge.add(p);
//						}
//					}
//				}
//				splitGroups.add(partitionsToMerge);
//			}
//		}
		
		return splitGroups;
	}
}
