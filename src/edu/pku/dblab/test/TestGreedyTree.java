package edu.pku.dblab.test;

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

import org.apache.log4j.Logger;

import edu.pku.dblab.parallel.GeneralBacktrack;
import edu.pku.dblab.parallel.PatternMatchingQuery;

public class TestGreedyTree {

	private static final String IGRAPH_FILE = "D:\\data\\yeast.igraph";
	private static final String QUERY_FILE = "D:\\data\\query-synthetic.igraph";
	private static final Logger logger = Logger.getLogger(TestGreedyTree.class);

	private static Map<Integer,Map<Integer,Set<int[]>>> inmemIndex = new TreeMap<Integer,Map<Integer,Set<int[]>>>();
	private static Map<Integer,Map<Integer,Integer>> freqPattern = new TreeMap<Integer,Map<Integer,Integer>>();
	private static Map<Integer,List<Integer>> adjIndex = new TreeMap<Integer,List<Integer>>();
	private static Map<Integer,Set<Integer>> dataLabelIndex = new TreeMap<Integer, Set<Integer>>();
	
	private static Map<Integer,Map<Integer,List<Integer>>> queryGraph = new TreeMap<Integer,Map<Integer,List<Integer>>>();
	private static Map<Integer,Set<Integer>> queryLabelIndex = new TreeMap<Integer,Set<Integer>>();

	public static void loadDataGraph() {

		try {
			// Reading from iGraph file
			FileReader igraphFile = new FileReader(IGRAPH_FILE);
			BufferedReader br = new BufferedReader(igraphFile);
			
			String str = br.readLine();
			
			int num_label = 0;
			//Map<Integer,Set<Integer>> vertexIndex = new TreeMap<Integer,Set<Integer>>();
			
			while (str != null) {
				String[] words = str.split(" ");
				switch(words[0]){
				case "v":
					Integer nodeID = Integer.valueOf(words[1]);
					Set<Integer> labelSet = new HashSet<Integer>();
					for(int i=2;i<words.length;i++){
						if( Integer.valueOf(words[i]).intValue() == 0){
							//Clear all content in existing labelList.
							labelSet = null;
							break;
						}else{
							labelSet.add(Integer.valueOf(words[i]));
							//num_label only used for counting total labels.
							if(Integer.valueOf(words[i])> num_label){
								num_label = Integer.valueOf(words[i]);
							}
						}//if
					}//for
					if(labelSet != null) {
						dataLabelIndex.put(nodeID,labelSet);
					}
					//num_vertices only used for counting total labels.
					break;
					
				case "e":
					//read from file. sourceNode is the smaller.
					Integer sourceNode = null;
					Integer destNode = null;
					
					if(Integer.valueOf(words[1]).intValue() > Integer.valueOf(words[2]).intValue()){
						sourceNode = Integer.valueOf(words[2]);
						destNode = Integer.valueOf(words[1]);
					}else{
						sourceNode = Integer.valueOf(words[1]);
						destNode = Integer.valueOf(words[2]);
					}
					
					try{
						Set<Integer> sourceLabelSet = dataLabelIndex.get(sourceNode);
					Set<Integer> destLabelSet = dataLabelIndex.get(destNode);
					
					if ((sourceLabelSet != null) && (destLabelSet != null)) {
						int[] newEdge = { sourceNode.intValue(),destNode.intValue() };

						for (Integer inSourceLabel : sourceLabelSet) {
							for (Integer inDestLabel : destLabelSet) {
								Integer sourceLabel = null;
								Integer destLabel = null;
								
								if(inSourceLabel.intValue() > inDestLabel.intValue()){
									sourceLabel = inDestLabel;
									destLabel = inSourceLabel;
								}else{
									sourceLabel = inSourceLabel;
									destLabel = inDestLabel;
								}
								
								if (inmemIndex.containsKey(sourceLabel)) {
									if (inmemIndex.get(sourceLabel).containsKey(destLabel)) {
										// pairs of source-dest have already
										// stored in in-memory index.
										Set<int[]> edgeSet = inmemIndex.get(sourceLabel).get(destLabel);
										edgeSet.add(newEdge);
										Integer freq = Integer.valueOf(freqPattern.get(sourceLabel).get(destLabel).intValue() + 1);
										freqPattern.get(sourceLabel).put(destLabel, freq);
									} else {
										Set<int[]> edgeSet = new HashSet<int[]>();
										edgeSet.add(newEdge);
										inmemIndex.get(sourceLabel).put(destLabel, edgeSet);
										freqPattern.get(sourceLabel).put(destLabel, Integer.valueOf(1));
									}
								} else {
									Set<int[]> edgeSet = new HashSet<int[]>();
									edgeSet.add(newEdge);
									Map<Integer, Set<int[]>> innerEntry = new TreeMap<Integer, Set<int[]>>();
									innerEntry.put(destLabel, edgeSet);
									inmemIndex.put(sourceLabel, innerEntry);
									Map<Integer, Integer> innerFreq = new TreeMap<Integer, Integer>();
									innerFreq.put(destLabel, Integer.valueOf(1));
									freqPattern.put(sourceLabel, innerFreq);

								}
								
								
								
							}

						}
						
						//add adjacent list
						if (adjIndex.containsKey(sourceNode)) {
							adjIndex.get(sourceNode).add(destNode);
							if(adjIndex.containsKey(destNode)){
								adjIndex.get(destNode).add(sourceNode);
							}else{
								List<Integer> newNodeList = new ArrayList<Integer>();
								newNodeList.add(sourceNode);
								adjIndex.put(destNode, newNodeList);
							}
						}else{
							List<Integer> newNodeList = new ArrayList<Integer>();
							newNodeList.add(destNode);
							adjIndex.put(sourceNode, newNodeList);
							
							if(adjIndex.containsKey(destNode)){
								adjIndex.get(destNode).add(sourceNode);
							}else{
								List<Integer> newNodeList2 = new ArrayList<Integer>();
								newNodeList2.add(sourceNode);
								adjIndex.put(destNode, newNodeList2);
							}
						}

					}					
					}catch(Exception e){
						break;
					}
						
					break;
				default:
					break;
				}//case
				str = br.readLine();
			}//while

			//@TEST log to logger.
			for (Integer sourceLabel : inmemIndex.keySet()) {
				for (Integer destLabel : inmemIndex.get(sourceLabel).keySet()) {
					String log = "Label (" + sourceLabel.toString() + "," + destLabel.toString() + "), ";
					int frequecy = freqPattern.get(sourceLabel).get(destLabel).intValue();
					log = log + "Freq:" + frequecy +", Edge list: ";
					Set<int[]> edgeList = inmemIndex.get(sourceLabel).get(destLabel);
					for(int[] edge:edgeList){
						log = log + " <" + edge[0]+","+edge[1]+">";
					}
				}
			}			
			
			for(Integer sourceNode: adjIndex.keySet()){
				String log = "Node "+sourceNode.toString()+" : ";
				for(Integer node:adjIndex.get(sourceNode)){
					log = log + ","+node.toString();
				}
			}

			br.close();
			igraphFile.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}//loadDataGraph()

	public static void loadQueryGraph(){
		
		try{
			FileReader queryFile = new FileReader(QUERY_FILE);
			BufferedReader br = new BufferedReader(queryFile);
			
			String str = br.readLine();
			
			int num_label = 0;
			
			while (str != null) {
				String[] words = str.split(" ");
				switch(words[0]){
				case "v":
					Integer nodeID = Integer.valueOf(words[1]);
					Set<Integer> labelSet = new HashSet<Integer>();
					for(int i=2;i<words.length;i++){
						if( Integer.valueOf(words[i]).intValue() == 0){
							//Clear all content in existing labelList.
							//labelSet = new HashSet<Integer>();
							//labelSet.add(Integer.valueOf(0));
							labelSet = null;
							break;
						}else{
							labelSet.add(Integer.valueOf(words[i]));
							//num_label only used for counting total labels.
							if(Integer.valueOf(words[i])> num_label){
								num_label = Integer.valueOf(words[i]);
							}
						}//if
					}//for

					if(labelSet != null) {
						queryLabelIndex.put(nodeID,labelSet);
					}
					//num_vertices only used for counting total labels.
					break;
					
				case "e":
					
					
					Integer sourceNode = Integer.valueOf(words[1]);
					Integer destNode = Integer.valueOf(words[2]);
					
					if( sourceNode==null || destNode == null){
						break;
					}
					
					Integer sourceLabel = null;
					Integer destLabel = null;
					
					//assume that each query node has only one label.
					for(Integer label:queryLabelIndex.get(sourceNode)){
						sourceLabel = label;
					}
					for(Integer label:queryLabelIndex.get(destNode)){
						//destLabel = label;
						if(sourceLabel > label){
							destLabel = sourceLabel;
							sourceLabel = label;
						}else{
							destLabel = label;
						}
					}
					//make sure that sourceLabel is not larger than destLabel.
					
					List<Integer> contentList = new ArrayList<Integer>();
					int freq = 0;
					if(freqPattern.get(sourceLabel).get(destLabel) == null){
						freq = 0;
					}else{
						freq = freqPattern.get(sourceLabel).get(destLabel).intValue();
					}
					contentList.add(sourceLabel);
					contentList.add(destLabel);
					contentList.add(freq);

					//read from file. sourceNode is the smaller.
					if (queryGraph.containsKey(sourceNode)) {
						queryGraph.get(sourceNode).put(destNode, contentList);
						
						if(queryGraph.containsKey(destNode)){
							queryGraph.get(destNode).put(sourceNode, contentList);
						}else{
							Map<Integer, List<Integer>> adjEntry = new TreeMap<Integer, List<Integer>>();
							adjEntry.put(sourceNode, contentList);
							queryGraph.put(destNode,adjEntry);
						}
					}else{
						Map<Integer, List<Integer>> adjSourceEntry = new TreeMap<Integer, List<Integer>>();
						adjSourceEntry.put(destNode, contentList);
						queryGraph.put(sourceNode,adjSourceEntry);
						
						if(queryGraph.containsKey(destNode)){
							queryGraph.get(destNode).put(sourceNode,contentList);
						}else{
							Map<Integer, List<Integer>> adjDestEntry = new TreeMap<Integer, List<Integer>>();
							adjDestEntry.put(sourceNode, contentList);
							queryGraph.put(destNode,adjDestEntry);
						}
					}

					break;

				default:
					break;
				}//case
				str = br.readLine();
			}//while
			
			//@TEST
			for (Integer sourceNode : queryGraph.keySet()) {
				for (Integer destNode : queryGraph.get(sourceNode).keySet()) {
					String log = "Query node (" + sourceNode.toString() + "," + destNode.toString() + "), ";
					List<Integer> contentList = queryGraph.get(sourceNode).get(destNode);
					int frequecy = contentList.get(2).intValue();
					log = log + "Freq:" + frequecy +", Label: "+contentList.get(0)+"--"+contentList.get(1)+".";
				}
			}	
			
			br.close();
			queryFile.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}//loadQueryGraph

	
	public boolean containEdge(Integer source, Integer dest, Map<Integer, Map<Integer,Integer>> graph){
		for(Integer s:graph.keySet()){
			for(Integer d:graph.get(s).keySet()){
				if((s.equals(source) && (d.equals(dest))) || ( (s.equals(dest)) && d.equals(source))){
					return true;
				}
			}
		}
		return false;
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
	
	
	public static void main(String args[]) {
		long startTime = System.currentTimeMillis();

		loadDataGraph();
		
		loadQueryGraph();
		
		long endTime = System.currentTimeMillis();
		long queryTime = endTime - startTime;

		String s = "Total index running time:"+queryTime;
		//logger.info(s);
		logger.info(s);
		
/*		int minFreq = Integer.MAX_VALUE;
		Integer s1 = null;
		Integer s2 = null;
		
		for(Integer source:queryGraph.keySet()){
			for(Integer dest:queryGraph.get(source).keySet()){
				logger.info("Edge u"+source.toString()+"-u"+dest.toString()+",freq "+queryGraph.get(source).get(dest).get(2).intValue());
				if(queryGraph.get(source).size()>1 && queryGraph.get(dest).size()>1) {
					if(queryGraph.get(source).get(dest).get(0).intValue() < minFreq  ){
						minFreq = queryGraph.get(source).get(dest).get(2).intValue();
						s1 = source;
						s2 = dest;
					}
				}
			}
		}
		
		logger.info("First Edge is u"+s1.toString()+"-u"+s2.toString()+",freq "+minFreq);
		
		Set<Integer> candidate = new HashSet<Integer>();
		Map<Integer,Map<Integer,List<Integer>>> SkeletonQueryGraph = new TreeMap<Integer,Map<Integer,List<Integer>>>();
		
		for(Integer vertex:queryGraph.keySet()){
			if(queryGraph.get(vertex).size()>1){
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
		for(Integer p:queryGraph.get(s1).get(s2)){
			pattern1.add(p);
		}
		for(Integer p:queryGraph.get(s2).get(s1)){
			pattern2.add(p);
		}
		entry1.put(s2,pattern1);
		entry2.put(s1,pattern2);
		SkeletonQueryGraph.put(s1,entry1); 
		SkeletonQueryGraph.put(s2,entry2); 
		
		while(candidate.size()>0){
			Set<Integer[]> neighbors = new HashSet<Integer[]>();
			
			for(Integer m:SkeletonQueryGraph.keySet()){
				for(Integer c:queryGraph.get(m).keySet()){
					if((!SkeletonQueryGraph.containsKey(c)) && (queryGraph.get(c).size() > 1)){
						Integer[] entry = {m,c,queryGraph.get(m).get(c).get(2)};
						logger.info("===Add Entry: u"+m.toString()+"-u"+c.toString()+",freq "+queryGraph.get(m).get(c).get(2).intValue());
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
			for(Integer p:queryGraph.get(newVertex[0]).get(newVertex[1])){
				newPattern1.add(p);
			}
			List<Integer> newPattern2 = new ArrayList<Integer>();
			for(Integer p:queryGraph.get(newVertex[1]).get(newVertex[0])){
				newPattern2.add(p);
			}
			entry.put(newVertex[0], newPattern1);
			SkeletonQueryGraph.put(newVertex[1],entry);
			SkeletonQueryGraph.get(newVertex[0]).put(newVertex[1],newPattern2);
			
			candidate.remove(newVertex[1]);
			logger.info("Add Entry: u"+newVertex[0].toString()+"->u"+newVertex[1].toString()+",Freq "+newVertex[2].toString());
			
		}
		
		for(Integer ss:SkeletonQueryGraph.keySet()){
			for(Integer d:SkeletonQueryGraph.get(ss).keySet()){
				logger.info("SKeleton Edge u"+ss.toString()+"-u"+d.toString()+", Label "+ SkeletonQueryGraph.get(ss).get(d).get(0).intValue()
						+ "-" + SkeletonQueryGraph.get(ss).get(d).get(1).intValue() + " ,freq "+SkeletonQueryGraph.get(ss).get(d).get(2).intValue());
			}
		}
		
*/		
		TestGreedyTree tgt = new TestGreedyTree();
		
		Map<Integer,Map<Integer,List<Integer>>> SkeletonQueryGraph = tgt.getSkeletonQueryGraph(queryGraph);
		
		for(Integer ss:SkeletonQueryGraph.keySet()){
			for(Integer d:SkeletonQueryGraph.get(ss).keySet()){
				logger.info("SKeleton Edge u"+ss.toString()+"-u"+d.toString()+", Label "+ SkeletonQueryGraph.get(ss).get(d).get(0).intValue()
						+ "-" + SkeletonQueryGraph.get(ss).get(d).get(1).intValue() + " ,freq "+SkeletonQueryGraph.get(ss).get(d).get(2).intValue());
			}
		}

		List<List<Integer>> parallelG = tgt.getParallelEdges(queryGraph,SkeletonQueryGraph);
		
		for(List<Integer> entry:parallelG){
			//parallelG.add(entry);
			logger.info("Content of ParallelG : u" + entry.get(0)+ "-u" + entry.get(1) +", l"+entry.get(2)+"-l"+entry.get(3)+",Freq "+entry.get(4));
		}
		
		
		//End time
		long endTime2 = System.currentTimeMillis();
		queryTime = endTime2 - endTime;

		s = "Total query running time:"+queryTime;
		logger.info(s);
	

	}


	
}
