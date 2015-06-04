package edu.pku.dblab.parallel;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

public class TestParallelSubISO {

	private static int BUFFER_SIZE = 500;
	private static BlockingQueue<Object> buffer = new ArrayBlockingQueue<Object>(BUFFER_SIZE);

	// private static final String IGRAPH_FILE =
	// "D:\\data\\example-full.igraph";
	// private static final String QUERY_FILE =
	// "D:\\data\\example-query.igraph";
	private static final String IGRAPH_FILE = "D:\\data\\yeast.igraph";
	private static final String QUERY_FILE = "D:\\data\\query-yeast-6.igraph";
	private static final Logger logger = Logger.getLogger(TestParallelSubISO.class);

	private static Map<Integer, Map<Integer, Set<int[]>>> inmemIndex = new TreeMap<Integer, Map<Integer, Set<int[]>>>();
	private static Map<Integer, Map<Integer, Integer>> freqPattern = new TreeMap<Integer, Map<Integer, Integer>>();
	private static Map<Integer, List<Integer>> adjIndex = new TreeMap<Integer, List<Integer>>();
	private static Map<Integer, Set<Integer>> dataLabelIndex = new TreeMap<Integer, Set<Integer>>();

	// private static Map<Integer,Map<Integer,List<Set<Integer>>>> queryGraph =
	// new TreeMap<Integer,Map<Integer,List<Set<Integer>>>>();
	private static Map<Integer, Map<Integer, List<Integer>>> queryGraph = new TreeMap<Integer, Map<Integer, List<Integer>>>();
	private static Map<Integer, Set<Integer>> queryLabelIndex = new TreeMap<Integer, Set<Integer>>();

	//private static List<String> R = new ArrayList<String>();
	private static int count = 0;

	public boolean IsLabelSetContained(Map<Integer, Set<Integer>> L_g, // V_g(dataLabelIndex)
			Map<Integer, Set<Integer>> L_q, // V_q(queryLabelIndex)
			Object u, Object v) {
		// public boolean IsLabelSetContained(LabeledGraph g, LabeledGraph q,
		// Object u, Object v) {

		// @SuppressWarnings("unchecked")
		// List<Object> L_v = g.getVertexLabel(v);
		// @SuppressWarnings("unchecked")
		// List<Object> L_u = q.getVertexLabel(u);
		Set<Integer> L_v = L_g.get(v);
		Set<Integer> L_u = L_q.get(u);

		List<Boolean> label_match = new ArrayList<Boolean>();

		// L(u) \in L(v)
		for (Object l_u : L_u) {
			for (Object l_v : L_v) {
				if (l_v.toString().equals(l_u.toString())) {
					label_match.add(new Boolean(true));
					continue;
				}
			}
		}
		if (label_match.size() == L_u.size()) {
			return true;
		} else {
			return false;
		}
	}

	public List<Object> FilterCandidate(Map<Integer, Set<Integer>> L_g, // V_g(dataLabelIndex)
			Map<Integer, Set<Integer>> L_q, // V_q(queryLabelIndex)
			Object u) {
		// public List<Object> FilterCandidate(LabeledGraph g, LabeledGraph q,
		// Object u) {

		List<Object> candidate = new ArrayList<Object>();

		// For backtrack algorithm, filtering is only graph label matching.
		// @SuppressWarnings("unchecked")
		// Set<Object> V_g = g.vertexSet();
		Set<Integer> V_g = L_g.keySet();

		for (Object v : V_g) {
			if (IsLabelSetContained(L_g, L_q, u, v)) {
				candidate.add(v);
			}
		}

		return candidate;
	}

	public List<List<Integer>> getParallelEdges(
			Map<Integer,Map<Integer,List<Integer>>> queryG,
			Map<Integer,Map<Integer,List<Integer>>> skeletonG
			){
		
		List<List<Integer>> parallelG = new ArrayList<List<Integer>>() ;
		Set<List<Integer>> tempG = new HashSet<List<Integer>>();
		
		for(Integer s:queryG.keySet()){
			if(skeletonG.containsKey(s)){
				for(Integer d:queryG.get(s).keySet()){
					if(! skeletonG.get(s).containsKey(d)){
						List<Integer> entry = new ArrayList<Integer>();
						if(s.intValue() > d.intValue()){
							entry.add(d);
							entry.add(s);
						}else{
							entry.add(s);
							entry.add(d);
						}
						for(Integer p:queryG.get(s).get(d)){
							entry.add(p);
						}
						tempG.add(entry);
					}
				}
			}else{
				for(Integer d:queryG.get(s).keySet()){
					//if(! skeletonG.containsKey(d)){
						List<Integer> entry = new ArrayList<Integer>();
						if(s.intValue() > d.intValue()){
							entry.add(d);
							entry.add(s);
						}else{
							entry.add(s);
							entry.add(d);
						}
						for(Integer p:queryG.get(s).get(d)){
							entry.add(p);
						}
						tempG.add(entry);
					//}
					
				}
			}
		}
		
		for(List<Integer> entry:tempG){
			parallelG.add(entry);
//			logger.info("Content of ParallelG : u" + entry.get(0)+ "-u" + entry.get(1) +", l"+entry.get(2)+"-l"+entry.get(3)+",Freq "+entry.get(4));
		}
		
		return parallelG;
	}
	
	/**
	 * @param args
	 */
	
	public Map<Integer,Map<Integer,List<Integer>>> getSkeletonQueryGraph(
			Map<Integer,Map<Integer,List<Integer>>> queryG
			){
		
		Map<Integer,Map<Integer,List<Integer>>> SkeletonQueryGraph = new TreeMap<Integer,Map<Integer,List<Integer>>>();

		int minFreq = Integer.MAX_VALUE;
		Integer s1 = null;
		Integer s2 = null;
		
		for(Integer source:queryG.keySet()){
			for(Integer dest:queryG.get(source).keySet()){
				logger.info("Edge u"+source.toString()+"-u"+dest.toString()+",freq "+queryG.get(source).get(dest).get(2).intValue());
				if(queryG.get(source).size()>1 && queryG.get(dest).size()>1) {
					if(queryG.get(source).get(dest).get(0).intValue() < minFreq  ){
						minFreq = queryG.get(source).get(dest).get(2).intValue();
						s1 = source;
						s2 = dest;
					}
				}
			}
		}
		
		logger.info("First Edge is u"+s1.toString()+"-u"+s2.toString()+",freq "+minFreq);
		
		Set<Integer> candidate = new HashSet<Integer>();
		
		for(Integer vertex:queryG.keySet()){
			if(queryG.get(vertex).size()>1){
				candidate.add(vertex);
			}
		}
		
//		for(Integer vertex:candidate){
//			logger.info("Candidate u"+vertex.toString());
//		}
		
		candidate.remove(s1);
		candidate.remove(s2);
		Map<Integer,List<Integer>> entry1 = new TreeMap<Integer,List<Integer>>();
		Map<Integer,List<Integer>> entry2 = new TreeMap<Integer,List<Integer>>();
		List<Integer> pattern1 = new ArrayList<Integer>();
		List<Integer> pattern2 = new ArrayList<Integer>();
		for(Integer p:queryG.get(s1).get(s2)){
			pattern1.add(p);
		}
		for(Integer p:queryG.get(s2).get(s1)){
			pattern2.add(p);
		}
		entry1.put(s2,pattern1);
		entry2.put(s1,pattern2);
		SkeletonQueryGraph.put(s1,entry1); 
		SkeletonQueryGraph.put(s2,entry2); 
		
		while(candidate.size()>0){
			Set<Integer[]> neighbors = new HashSet<Integer[]>();
			
			for(Integer m:SkeletonQueryGraph.keySet()){
				for(Integer c:queryG.get(m).keySet()){
					if((!SkeletonQueryGraph.containsKey(c)) && (queryG.get(c).size() > 1)){
						Integer[] entry = {m,c,queryG.get(m).get(c).get(2)};
						logger.info("===Add Entry: u"+m.toString()+"-u"+c.toString()+",freq "+queryG.get(m).get(c).get(2).intValue());
						neighbors.add(entry);
					}
				}
			}
			
			int min = Integer.MAX_VALUE;
			Integer[] newVertex = null;
			for(Integer[] entry: neighbors){
				if(entry[2].intValue() < min){
					min = entry[2].intValue();
					//get the lowest cost combination.
					newVertex = entry;
				}
			}
			
			Map<Integer, List<Integer>> entry = new TreeMap<Integer, List<Integer>>();
			List<Integer> newPattern1 = new ArrayList<Integer>();
			for(Integer p:queryG.get(newVertex[0]).get(newVertex[1])){
				newPattern1.add(p);
			}
			List<Integer> newPattern2 = new ArrayList<Integer>();
			for(Integer p:queryG.get(newVertex[1]).get(newVertex[0])){
				newPattern2.add(p);
			}
			entry.put(newVertex[0], newPattern1);
			SkeletonQueryGraph.put(newVertex[1],entry);
			SkeletonQueryGraph.get(newVertex[0]).put(newVertex[1],newPattern2);
			
			candidate.remove(newVertex[1]);
			logger.info("Add Entry: u"+newVertex[0].toString()+"->u"+newVertex[1].toString()+",Freq "+newVertex[2].toString());
			
		}
		
//		for(Integer ss:SkeletonQueryGraph.keySet()){
//			for(Integer d:SkeletonQueryGraph.get(ss).keySet()){
//				logger.info("SKeleton Edge u"+ss.toString()+"-u"+d.toString()+", Label "+ SkeletonQueryGraph.get(ss).get(d).get(0).intValue()
//						+ "-" + SkeletonQueryGraph.get(ss).get(d).get(1).intValue() + " ,freq "+SkeletonQueryGraph.get(ss).get(d).get(2).intValue());
//			}
//		}
	
		return SkeletonQueryGraph;
	}
	
	
	
	public static void loadDataGraph() {

		try {
			// Reading from iGraph file
			FileReader igraphFile = new FileReader(IGRAPH_FILE);
			BufferedReader br = new BufferedReader(igraphFile);

			String str = br.readLine();

			int num_label = 0;
			// Map<Integer,Set<Integer>> vertexIndex = new
			// TreeMap<Integer,Set<Integer>>();

			while (str != null) {
				String[] words = str.split(" ");
				switch (words[0]) {
				case "v":
					Integer nodeID = Integer.valueOf(words[1]);
					Set<Integer> labelSet = new HashSet<Integer>();
					for (int i = 2; i < words.length; i++) {
						if (Integer.valueOf(words[i]).intValue() == 0) {
							// Clear all content in existing labelList.
							// labelSet = new HashSet<Integer>();
							// labelSet.add(Integer.valueOf(0));
							labelSet = null;
							break;
						} else {
							labelSet.add(Integer.valueOf(words[i]));
							// num_label only used for counting total labels.
							if (Integer.valueOf(words[i]) > num_label) {
								num_label = Integer.valueOf(words[i]);
							}
						}// if
					}// for
						// LabeledVertex v = new LabeledVertex();
						// v.setId(nodeID.intValue());
					if (labelSet != null) {
						// v.setLabels(labelSet);
						// vertexIndex.put(nodeID, labelSet);
						dataLabelIndex.put(nodeID, labelSet);
					}
					// num_vertices only used for counting total labels.
					break;

				case "e":
					// read from file. sourceNode is the smaller.
					Integer sourceNode = null;
					Integer destNode = null;

					if (Integer.valueOf(words[1]).intValue() > Integer.valueOf(
							words[2]).intValue()) {
						sourceNode = Integer.valueOf(words[2]);
						destNode = Integer.valueOf(words[1]);
					} else {
						sourceNode = Integer.valueOf(words[1]);
						destNode = Integer.valueOf(words[2]);
					}

					// Set<Integer> sourceLabelSet =
					// vertexIndex.get(sourceNode);
					// Set<Integer> destLabelSet = vertexIndex.get(destNode);
					// System.out.println("node pairs: "+sourceNode.intValue()+"-"+destNode.intValue());
					try {
						Set<Integer> sourceLabelSet = dataLabelIndex
								.get(sourceNode);
						Set<Integer> destLabelSet = dataLabelIndex
								.get(destNode);

						if ((sourceLabelSet != null) && (destLabelSet != null)) {
							int[] newEdge = { sourceNode.intValue(),
									destNode.intValue() };

							for (Integer inSourceLabel : sourceLabelSet) {
								for (Integer inDestLabel : destLabelSet) {
									// System.out.println("Vertex:("+sourceNode.toString()+","+destNode.toString()+"), label<"+
									// sourceLabel.toString()+"-"+destLabel.toString()+">.");
									Integer sourceLabel = null;
									Integer destLabel = null;

									if (inSourceLabel.intValue() > inDestLabel
											.intValue()) {
										sourceLabel = inDestLabel;
										destLabel = inSourceLabel;
									} else {
										sourceLabel = inSourceLabel;
										destLabel = inDestLabel;
									}

									if (inmemIndex.containsKey(sourceLabel)) {
										if (inmemIndex.get(sourceLabel)
												.containsKey(destLabel)) {
											// pairs of source-dest have already
											// stored in in-memory index.
											Set<int[]> edgeSet = inmemIndex
													.get(sourceLabel).get(
															destLabel);
											edgeSet.add(newEdge);
											Integer freq = Integer
													.valueOf(freqPattern
															.get(sourceLabel)
															.get(destLabel)
															.intValue() + 1);
											freqPattern.get(sourceLabel).put(
													destLabel, freq);
										} else {
											Set<int[]> edgeSet = new HashSet<int[]>();
											edgeSet.add(newEdge);
											inmemIndex.get(sourceLabel).put(
													destLabel, edgeSet);
											freqPattern.get(sourceLabel).put(
													destLabel,
													Integer.valueOf(1));
										}
									} else {
										Set<int[]> edgeSet = new HashSet<int[]>();
										edgeSet.add(newEdge);
										Map<Integer, Set<int[]>> innerEntry = new TreeMap<Integer, Set<int[]>>();
										innerEntry.put(destLabel, edgeSet);
										inmemIndex.put(sourceLabel, innerEntry);
										Map<Integer, Integer> innerFreq = new TreeMap<Integer, Integer>();
										innerFreq.put(destLabel,
												Integer.valueOf(1));
										freqPattern.put(sourceLabel, innerFreq);

									}

								}

							}

							// add adjacent list
							if (adjIndex.containsKey(sourceNode)) {
								// System.out.print("contains sourceNode: "+sourceNode.toString()+",");
								adjIndex.get(sourceNode).add(destNode);
								if (adjIndex.containsKey(destNode)) {
									adjIndex.get(destNode).add(sourceNode);
									// System.out.print("contains destNode: "+destNode.toString());
								} else {
									List<Integer> newNodeList = new ArrayList<Integer>();
									newNodeList.add(sourceNode);
									adjIndex.put(destNode, newNodeList);
									// System.out.print("not contains destNode: "+destNode.toString());
								}
							} else {
								// System.out.print("Not contains sourceNode: "+sourceNode.toString()+",");
								List<Integer> newNodeList = new ArrayList<Integer>();
								newNodeList.add(destNode);
								adjIndex.put(sourceNode, newNodeList);

								if (adjIndex.containsKey(destNode)) {
									adjIndex.get(destNode).add(sourceNode);
									// System.out.print("contains destNode: "+destNode.toString()+",");
								} else {
									List<Integer> newNodeList2 = new ArrayList<Integer>();
									newNodeList2.add(sourceNode);
									adjIndex.put(destNode, newNodeList2);
									// System.out.print("not contains destNode: "+destNode.toString()+",");

								}
							}
						}
					} catch (Exception e) {
						break;
					}

					break;
				default:
					break;
				}// case
				str = br.readLine();
			}// while

			// @TEST log to logger.
			for (Integer sourceLabel : inmemIndex.keySet()) {
				// System.out.print("Label (" + sourceLabel.toString());
				for (Integer destLabel : inmemIndex.get(sourceLabel).keySet()) {
					// System.out.print("," + destLabel.toString() + "):");
					String log = "Label (" + sourceLabel.toString() + ","
							+ destLabel.toString() + "), ";
					int frequecy = freqPattern.get(sourceLabel).get(destLabel)
							.intValue();
					log = log + "Freq:" + frequecy + ", Edge list: ";
					Set<int[]> edgeList = inmemIndex.get(sourceLabel).get(
							destLabel);
					for (int[] edge : edgeList) {
						// System.out.print(" <" + edge[0]+","+edge[1]+">");
						log = log + " <" + edge[0] + "," + edge[1] + ">";
					}
					// System.out.println(";");
					// logger.info(log);
				}
			}

			for (Integer sourceNode : adjIndex.keySet()) {
				// System.out.print("Node "+sourceNode.toString()+" : ");
				String log = "Node " + sourceNode.toString() + " : ";
				for (Integer node : adjIndex.get(sourceNode)) {
					// System.out.print(","+node.toString());
					log = log + "," + node.toString();
				}
				// System.out.println(";");
				// logger.info(log);
			}

			br.close();
			igraphFile.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}// loadDataGraph()

	public static void loadQueryGraph() {

		try {
			FileReader queryFile = new FileReader(QUERY_FILE);
			BufferedReader br = new BufferedReader(queryFile);

			String str = br.readLine();

			int num_label = 0;

			while (str != null) {
				String[] words = str.split(" ");
				switch (words[0]) {
				case "v":
					Integer nodeID = Integer.valueOf(words[1]);
					Set<Integer> labelSet = new HashSet<Integer>();
					for (int i = 2; i < words.length; i++) {
						if (Integer.valueOf(words[i]).intValue() == 0) {
							// Clear all content in existing labelList.
							// labelSet = new HashSet<Integer>();
							// labelSet.add(Integer.valueOf(0));
							labelSet = null;
							break;
						} else {
							labelSet.add(Integer.valueOf(words[i]));
							// num_label only used for counting total labels.
							if (Integer.valueOf(words[i]) > num_label) {
								num_label = Integer.valueOf(words[i]);
							}
						}// if
					}// for
						// LabeledVertex v = new LabeledVertex();
						// v.setId(nodeID.intValue());
					if (labelSet != null) {
						// v.setLabels(labelSet);
						// vertexIndex.put(nodeID, labelSet);
						queryLabelIndex.put(nodeID, labelSet);
					}
					// num_vertices only used for counting total labels.
					break;

				case "e":

					Integer sourceNode = Integer.valueOf(words[1]);
					Integer destNode = Integer.valueOf(words[2]);

					if (sourceNode == null || destNode == null) {
						break;
					}

					Integer sourceLabel = null;
					Integer destLabel = null;

					// assume that each query node has only one label.
					// try{
					for (Integer label : queryLabelIndex.get(sourceNode)) {
						sourceLabel = label;
					}
					for (Integer label : queryLabelIndex.get(destNode)) {
						// destLabel = label;
						if (sourceLabel > label) {
							destLabel = sourceLabel;
							sourceLabel = label;
						} else {
							destLabel = label;
						}
					}

					// make sure that sourceLabel is not larger than destLabel.
					List<Integer> contentList = new ArrayList<Integer>();
					int freq = 0;
					if (freqPattern.get(sourceLabel).get(destLabel) == null) {
						freq = 0;
					} else {
						freq = freqPattern.get(sourceLabel).get(destLabel)
								.intValue();
					}
					contentList.add(sourceLabel);
					contentList.add(destLabel);
					contentList.add(freq);

					// read from file. sourceNode is the smaller.
					if (queryGraph.containsKey(sourceNode)) {
						// System.out.print("contains sourceNode: "+sourceNode.toString()+",");
						// queryGraph.get(sourceNode).add(destNode);
						// Map<Integer, List<Integer>> adjEntry = new
						// TreeMap<Integer, List<Integer>>();
						// adjEntry.put(destNode, contentList);
						queryGraph.get(sourceNode).put(destNode, contentList);

						if (queryGraph.containsKey(destNode)) {
							queryGraph.get(destNode).put(sourceNode,
									contentList);
							// System.out.print("contains destNode: "+destNode.toString());
						} else {
							Map<Integer, List<Integer>> adjEntry = new TreeMap<Integer, List<Integer>>();
							adjEntry.put(sourceNode, contentList);
							queryGraph.put(destNode, adjEntry);
							// System.out.print("not contains destNode: "+destNode.toString());
						}
					} else {
						// System.out.print("Not contains sourceNode: "+sourceNode.toString()+",");
						Map<Integer, List<Integer>> adjSourceEntry = new TreeMap<Integer, List<Integer>>();
						adjSourceEntry.put(destNode, contentList);
						queryGraph.put(sourceNode, adjSourceEntry);

						if (queryGraph.containsKey(destNode)) {
							queryGraph.get(destNode).put(sourceNode,
									contentList);
							// System.out.print("contains destNode: "+destNode.toString()+",");
						} else {
							Map<Integer, List<Integer>> adjDestEntry = new TreeMap<Integer, List<Integer>>();
							adjDestEntry.put(sourceNode, contentList);
							queryGraph.put(destNode, adjDestEntry);
							// System.out.print("not contains destNode: "+destNode.toString()+",");

						}
					}
					// System.out.println(".");

					break;


				default:
					break;
				}// case
				str = br.readLine();
			}// while

			// @TEST
			for (Integer sourceNode : queryGraph.keySet()) {
				for (Integer destNode : queryGraph.get(sourceNode).keySet()) {
					String log = "Query node (" + sourceNode.toString() + ","
							+ destNode.toString() + "), ";
					List<Integer> contentList = queryGraph.get(sourceNode).get(
							destNode);
					int frequecy = contentList.get(2).intValue();
					log = log + "Freq:" + frequecy + ", Label: "
							+ contentList.get(0) + "--" + contentList.get(1)
							+ ".";
					// System.out.println(log);
					// logger.info(log);
				}
			}

			br.close();
			queryFile.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}// loadQueryGraph

	/**
	 * @param args
	 */
	public static void main(String args[]) {
			
		long startTime = System.currentTimeMillis();

		ExecutorService exec = Executors.newCachedThreadPool();   

        loadDataGraph();
		
		loadQueryGraph();

		TestParallelSubISO tps = new TestParallelSubISO();

		List<Object[]> M = new ArrayList<Object[]>();
		List<Object> C_u = new ArrayList<Object>();
		Map<Object, Set<Object>> F = new TreeMap<Object, Set<Object>>();

		int min = Integer.MAX_VALUE;
		Integer s = null;
		Integer d = null;
		for (Integer source : queryGraph.keySet()) {
			for (Integer destination : queryGraph.get(source).keySet()) {
				for (Integer freq : queryGraph.get(source).get(destination)) {
					if (freq.intValue() < min) {
						min = freq.intValue();
						s = source;
						d = destination;
					}
				}
			}
		}

		Integer startVertex = null;
		if (queryGraph.get(s).size() > queryGraph.get(d).size()) {
			startVertex = s;
		} else {
			startVertex = d;
		}

		C_u = (ArrayList<Object>) tps.FilterCandidate(dataLabelIndex, queryLabelIndex, startVertex);
		Set<Object> visited = new HashSet<Object>();
		//System.out.println("root size "+C_u.size());

		for(int i = 0; i < C_u.size() ; i++) {   
			M = new ArrayList<Object[]>();
			// F = new HashSet<Object>();
			Object[] first_matched_pair = { startVertex, C_u.get(i) };
			M.add(first_matched_pair);
			visited.add(C_u.get(i));
			if (i == 0) {
				F.put(startVertex, visited);
			}

			exec.execute(new TestSubISOProducer(adjIndex,dataLabelIndex,queryGraph,queryLabelIndex,startVertex,C_u.get(i),buffer));
            //exec.execute(new TestConcurrentConsumer(i, buffer));
         }   

		// Print testing result.
		logger.info("#################################################################");
		 int total_matched = 0 ;
		 for(Object result:buffer){
			 logger.info(result);
			 total_matched ++;
		 }
		 logger.info("Total matched subgraphs::"+total_matched);
		logger.info("Total recursive counts::" + count);
		logger.info("################################################################");

		//End time
			long endTime = System.currentTimeMillis();
			long queryTime = endTime - startTime;

			exec.shutdown();       
			
			System.out.println("Total expense: "+queryTime);

	}



}
