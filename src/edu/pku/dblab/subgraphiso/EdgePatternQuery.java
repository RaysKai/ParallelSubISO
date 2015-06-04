package edu.pku.dblab.subgraphiso;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import edu.pku.dblab.binary.CheckBinaryPatternIndex;

public class EdgePatternQuery {

	private static final String IGRAPH_FILE = "D:\\data\\yeast.igraph";
	private static final String QUERY_FILE = "D:\\data\\query-yeast-1000.igraph";
	private static final Logger logger = Logger.getLogger(EdgePatternQuery.class);

	private static Map<Integer,Map<Integer,Set<int[]>>> inmemIndex = new TreeMap<Integer,Map<Integer,Set<int[]>>>();
	private static Map<Integer,Map<Integer,Integer>> freqPattern = new TreeMap<Integer,Map<Integer,Integer>>();
	private static Map<Integer,List<Integer>> adjIndex = new TreeMap<Integer,List<Integer>>();
	
	//private static Map<Integer,Map<Integer,List<Set<Integer>>>> queryGraph = new TreeMap<Integer,Map<Integer,List<Set<Integer>>>>();
	//private static Map<Integer,Map<Integer,List<Integer>>> queryGraph = new TreeMap<Integer,Map<Integer,List<Integer>>>();
	private static List<Map<Integer,Set<Integer>>> vertexIndex = new ArrayList<Map<Integer,Set<Integer>>>();
	private static List<Map<Integer,Map<Integer,List<Integer>>>> queryGraph = new ArrayList<Map<Integer,Map<Integer,List<Integer>>>>();


	public static void main(String[] args){
		
		//Record Start time of each query.
		long startTime = System.currentTimeMillis();

		loadDataGraph();
		
		loadQueryGraph();
		
		
		//End time
		long endTime = System.currentTimeMillis();
		long queryTime = endTime - startTime;

		String s = "Total running time:"+queryTime;
		logger.info(s);
	
		
	}
	
	public static void loadDataGraph() {

		try {
			// Reading from iGraph file
			FileReader igraphFile = new FileReader(IGRAPH_FILE);
			BufferedReader br = new BufferedReader(igraphFile);
			
			String str = br.readLine();
			
			int num_vertices = 0;
			int num_patterns = 0;
			int num_label = 0;
			boolean init_file = false;
			Map<Integer,Set<Integer>> vertexIndex = new TreeMap<Integer,Set<Integer>>();
			
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
						vertexIndex.put(nodeID, labelSet);
					}
					//num_vertices only used for counting total labels.
					num_vertices ++;
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
					
					Set<Integer> sourceLabelSet = vertexIndex.get(sourceNode);
					Set<Integer> destLabelSet = vertexIndex.get(destNode);
					
					if ((sourceLabelSet != null) && (destLabelSet != null)) {
						int[] newEdge = { sourceNode.intValue(),destNode.intValue() };

						for (Integer inSourceLabel : sourceLabelSet) {
							for (Integer inDestLabel : destLabelSet) {
//								System.out.println("Vertex:("+sourceNode.toString()+","+destNode.toString()+"), label<"+
//													sourceLabel.toString()+"-"+destLabel.toString()+">.");
								Integer sourceLabel = null;
								Integer destLabel = null;
								
								if(inSourceLabel.intValue() > inDestLabel.intValue()){
									sourceLabel = inDestLabel;
									destLabel = inSourceLabel;
								}else{
									sourceLabel = inSourceLabel;
									destLabel = inDestLabel;
								}
								
/*								if (inmemIndex.containsKey(sourceLabel)) {
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
								*/
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
							//System.out.print("contains sourceNode: "+sourceNode.toString()+",");
							adjIndex.get(sourceNode).add(destNode);
							if(adjIndex.containsKey(destNode)){
								adjIndex.get(destNode).add(sourceNode);
								//System.out.print("contains destNode: "+destNode.toString());
							}else{
								List<Integer> newNodeList = new ArrayList<Integer>();
								newNodeList.add(sourceNode);
								adjIndex.put(destNode, newNodeList);
								//System.out.print("not contains destNode: "+destNode.toString());
							}
						}else{
							//System.out.print("Not contains sourceNode: "+sourceNode.toString()+",");
							List<Integer> newNodeList = new ArrayList<Integer>();
							newNodeList.add(destNode);
							adjIndex.put(sourceNode, newNodeList);
							
							if(adjIndex.containsKey(destNode)){
								adjIndex.get(destNode).add(sourceNode);
								//System.out.print("contains destNode: "+destNode.toString()+",");
							}else{
								List<Integer> newNodeList2 = new ArrayList<Integer>();
								newNodeList2.add(sourceNode);
								adjIndex.put(destNode, newNodeList2);
								//System.out.print("not contains destNode: "+destNode.toString()+",");
								
							}
						}
/*						System.out.println(".");
						for(Integer sode: adjIndex.keySet()){
							//System.out.print("Node "+sourceNode.toString()+" : ");
							String log = "Node "+sode.toString()+" : ";
							for(Integer node:adjIndex.get(sode)){
								//System.out.print(","+node.toString());
								log = log + ","+node.toString();
							}
							System.out.println(log);
							//logger.info(log);
						}
*/						
					}
						
					break;
				default:
					break;
				}//case
				str = br.readLine();
			}//while

			//@TEST for v
//			for(Integer content:vertexIndex.keySet()){
//				System.out.print("Vertex "+content.toString()+":");
//				for(Integer label:vertexIndex.get(content)){
//					System.out.print("."+label.toString());
//				}
//				System.out.println(";");
//			}
			
			
			//@TEST log to logger.
			for (Integer sourceLabel : inmemIndex.keySet()) {
				//System.out.print("Label (" + sourceLabel.toString());
				for (Integer destLabel : inmemIndex.get(sourceLabel).keySet()) {
					//System.out.print("," + destLabel.toString() + "):");
					String log = "Label (" + sourceLabel.toString() + "," + destLabel.toString() + "), ";
					int frequecy = freqPattern.get(sourceLabel).get(destLabel).intValue();
					log = log + "Freq:" + frequecy +", Edge list: ";
					Set<int[]> edgeList = inmemIndex.get(sourceLabel).get(destLabel);
					for(int[] edge:edgeList){
						//System.out.print(" <" + edge[0]+","+edge[1]+">");
						log = log + " <" + edge[0]+","+edge[1]+">";
					}
					//System.out.println(";");
					logger.info(log);
				}
			}			
			
			for(Integer sourceNode: adjIndex.keySet()){
				//System.out.print("Node "+sourceNode.toString()+" : ");
				String log = "Node "+sourceNode.toString()+" : ";
				for(Integer node:adjIndex.get(sourceNode)){
					//System.out.print(","+node.toString());
					log = log + ","+node.toString();
				}
				//System.out.println(";");
				logger.info(log);
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

	}//loadDataGraph()
	

	
	
	
	public static void loadQueryGraph(){
		
		try{
			FileReader queryFile = new FileReader(QUERY_FILE);
			BufferedReader br = new BufferedReader(queryFile);
			
			//List<String> queryBlock = new ArrayList<String>();
			
			String str = br.readLine();
			
			int num_vertices = 0;
			int num_patterns = 0;
			int num_label = 0;
			boolean init_file = false;
			Map<Integer,Set<Integer>> vertexIndexEntry = new TreeMap<Integer,Set<Integer>>();
			Map<Integer,Map<Integer,List<Integer>>> queryGraphEntry  = new TreeMap<Integer,Map<Integer,List<Integer>>>();
			//Map<Integer,List<Integer>> querGraph = new TreeMap<Integer,List<Integer>>();
			
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
							//labelSet = null;
							labelSet.add(Integer.valueOf(1));
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
						vertexIndexEntry.put(nodeID, labelSet);
					}
					//num_vertices only used for counting total labels.
					num_vertices ++;
					break;
					
				case "e":
					
					Integer sourceNode = Integer.valueOf(words[1]);
					Integer destNode = Integer.valueOf(words[2]);
					
					Integer sourceLabel = null;
					Integer destLabel = null;
					
					//assume that each query node has only one label.
					for(Integer label:vertexIndexEntry.get(sourceNode)){
						sourceLabel = label;
					}
					for(Integer label:vertexIndexEntry.get(destNode)){
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
					if (queryGraphEntry.containsKey(sourceNode)) {
						//System.out.print("contains sourceNode: "+sourceNode.toString()+",");
						//queryGraph.get(sourceNode).add(destNode);
						//Map<Integer, List<Integer>> adjEntry = new TreeMap<Integer, List<Integer>>();
						//adjEntry.put(destNode, contentList);
						queryGraphEntry.get(sourceNode).put(destNode, contentList);
						
						if(queryGraphEntry.containsKey(destNode)){
							queryGraphEntry.get(destNode).put(sourceNode, contentList);
							//System.out.print("contains destNode: "+destNode.toString());
						}else{
							Map<Integer, List<Integer>> adjEntry = new TreeMap<Integer, List<Integer>>();
							adjEntry.put(sourceNode, contentList);
							queryGraphEntry.put(destNode,adjEntry);
							//System.out.print("not contains destNode: "+destNode.toString());
						}
					}else{
						//System.out.print("Not contains sourceNode: "+sourceNode.toString()+",");
						Map<Integer, List<Integer>> adjSourceEntry = new TreeMap<Integer, List<Integer>>();
						adjSourceEntry.put(destNode, contentList);
						queryGraphEntry.put(sourceNode,adjSourceEntry);
						
						if(queryGraphEntry.containsKey(destNode)){
							queryGraphEntry.get(destNode).put(sourceNode,contentList);
							//System.out.print("contains destNode: "+destNode.toString()+",");
						}else{
							Map<Integer, List<Integer>> adjDestEntry = new TreeMap<Integer, List<Integer>>();
							adjDestEntry.put(sourceNode, contentList);
							queryGraphEntry.put(destNode,adjDestEntry);
							//System.out.print("not contains destNode: "+destNode.toString()+",");
							
						}
					}
					//System.out.println(".");

					break;
				case "t":
					int blockId = Integer.valueOf(words[2]).intValue();

					if(blockId == 0){
						break;
					}else{
						queryGraph.add(queryGraphEntry);
						vertexIndex.add(vertexIndexEntry);
						vertexIndexEntry = new TreeMap<Integer,Set<Integer>>();
						queryGraphEntry  = new TreeMap<Integer,Map<Integer,List<Integer>>>();
					}
					logger.info("Query "+blockId+" is add to queryGraph.");

					if(blockId > 0){
					for (Integer sn : queryGraph.get(blockId-1).keySet()) {
						for (Integer dn : queryGraph.get(blockId-1).get(sn).keySet()) {
							String log = "Query node (" + sn.toString() + "," + dn.toString() + "), ";
							List<Integer> cl = queryGraph.get(blockId-1).get(sn).get(dn);
							int frequecy = cl.get(2).intValue();
							log = log + "Freq:" + frequecy +", Label: "+cl.get(0)+"--"+cl.get(1)+".";
							//System.out.println(log);
							logger.info(log);
						}
					}
					}

					break;

				default:
					break;
				}//case
				str = br.readLine();
			}//while
			
			//@TEST
/*			for (Integer sourceNode : queryGraph.keySet()) {
				for (Integer destNode : queryGraph.get(sourceNode).keySet()) {
					String log = "Query node (" + sourceNode.toString() + "," + destNode.toString() + "), ";
					List<Integer> contentList = queryGraph.get(sourceNode).get(destNode);
					int frequecy = contentList.get(2).intValue();
					log = log + "Freq:" + frequecy +", Label: "+contentList.get(0)+"--"+contentList.get(1)+".";
					//System.out.println(log);
					logger.info(log);
				}
			}	
*/			
			br.close();
			queryFile.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}//loadQueryGraph

}
