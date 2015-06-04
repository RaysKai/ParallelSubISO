package edu.pku.dblab.parallel;

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

public class PatternMatchingQuery {

	private static final String IGRAPH_FILE = "D:\\data\\yeast.igraph";
	private static final String QUERY_FILE = "D:\\data\\query.igraph";
	private static final Logger logger = Logger.getLogger(PatternMatchingQuery.class);

	private static Map<Integer,Map<Integer,Set<int[]>>> inmemIndex = new TreeMap<Integer,Map<Integer,Set<int[]>>>();
	private static Map<Integer,Map<Integer,Integer>> freqPattern = new TreeMap<Integer,Map<Integer,Integer>>();
	private static Map<Integer,List<Integer>> adjIndex = new TreeMap<Integer,List<Integer>>();
	private static Map<Integer,Set<Integer>> dataLabelIndex = new TreeMap<Integer, Set<Integer>>();
	
	//private static Map<Integer,Map<Integer,List<Set<Integer>>>> queryGraph = new TreeMap<Integer,Map<Integer,List<Set<Integer>>>>();
	private static Map<Integer,Map<Integer,List<Integer>>> queryGraph = new TreeMap<Integer,Map<Integer,List<Integer>>>();
	private static Map<Integer,Set<Integer>> queryLabelIndex = new TreeMap<Integer,Set<Integer>>();


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
					//LabeledVertex v = new LabeledVertex();
					//v.setId(nodeID.intValue());
					if(labelSet != null) {
						//v.setLabels(labelSet);
						//vertexIndex.put(nodeID, labelSet);
						dataLabelIndex.put(nodeID,labelSet);
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
					
					//Set<Integer> sourceLabelSet = vertexIndex.get(sourceNode);
					//Set<Integer> destLabelSet = vertexIndex.get(destNode);
					//System.out.println("node pairs: "+sourceNode.intValue()+"-"+destNode.intValue());
					try{
						Set<Integer> sourceLabelSet = dataLabelIndex.get(sourceNode);
					Set<Integer> destLabelSet = dataLabelIndex.get(destNode);
					
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
					}catch(Exception e){
						break;
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
			
			String str = br.readLine();
			
			int num_vertices = 0;
			int num_patterns = 0;
			int num_label = 0;
			boolean init_file = false;
			//Map<Integer,Set<Integer>> vertexIndex = new TreeMap<Integer,Set<Integer>>();
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
					//LabeledVertex v = new LabeledVertex();
					//v.setId(nodeID.intValue());
					if(labelSet != null) {
						//v.setLabels(labelSet);
						//vertexIndex.put(nodeID, labelSet);
						queryLabelIndex.put(nodeID,labelSet);
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
						//System.out.print("contains sourceNode: "+sourceNode.toString()+",");
						//queryGraph.get(sourceNode).add(destNode);
						//Map<Integer, List<Integer>> adjEntry = new TreeMap<Integer, List<Integer>>();
						//adjEntry.put(destNode, contentList);
						queryGraph.get(sourceNode).put(destNode, contentList);
						
						if(queryGraph.containsKey(destNode)){
							queryGraph.get(destNode).put(sourceNode, contentList);
							//System.out.print("contains destNode: "+destNode.toString());
						}else{
							Map<Integer, List<Integer>> adjEntry = new TreeMap<Integer, List<Integer>>();
							adjEntry.put(sourceNode, contentList);
							queryGraph.put(destNode,adjEntry);
							//System.out.print("not contains destNode: "+destNode.toString());
						}
					}else{
						//System.out.print("Not contains sourceNode: "+sourceNode.toString()+",");
						Map<Integer, List<Integer>> adjSourceEntry = new TreeMap<Integer, List<Integer>>();
						adjSourceEntry.put(destNode, contentList);
						queryGraph.put(sourceNode,adjSourceEntry);
						
						if(queryGraph.containsKey(destNode)){
							queryGraph.get(destNode).put(sourceNode,contentList);
							//System.out.print("contains destNode: "+destNode.toString()+",");
						}else{
							Map<Integer, List<Integer>> adjDestEntry = new TreeMap<Integer, List<Integer>>();
							adjDestEntry.put(sourceNode, contentList);
							queryGraph.put(destNode,adjDestEntry);
							//System.out.print("not contains destNode: "+destNode.toString()+",");
							
						}
					}
					//System.out.println(".");

					break;

/*					Integer sourceNode = null;
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
						for(Integer inSourceLabel: sourceLabelSet){
							for(Integer inDestLabel: destLabelSet){
								
								Integer sourceLabel = null;
								Integer destLabel = null;
								Integer sNode = null;
								Integer dNode = null;
								
								if(inSourceLabel.intValue() > inDestLabel.intValue()){
									sourceLabel = inDestLabel; sNode = destNode;
									destLabel = inSourceLabel; dNode = sourceNode;
								}else{
									sourceLabel = inSourceLabel; sNode = sourceNode;
									destLabel = inDestLabel; dNode = destNode;
								}

								if(queryGraph.containsKey(sourceLabel)){
									System.out.println("Contains sourceLabel: "+sourceLabel.toString()+". Node: "+sNode.toString()+";");
									if(queryGraph.get(sourceLabel).containsKey(destLabel)){
										System.out.println("Contains destLabel: "+destLabel.toString()+". Node: "+dNode.toString()+";");
										List<Set<Integer>> patternSet = queryGraph.get(sourceLabel).get(destLabel);
										for(Integer label :vertexIndex.get(sNode)){
											patternSet.get(0).add(label);
										}
										for(Integer label:patternSet.get(0)){
											System.out.print(":"+label.toString());
										}
										System.out.println(".");
										for(Integer label :vertexIndex.get(dNode)){
											patternSet.get(1).add(label);
										}
										for(Integer label:patternSet.get(1)){
											System.out.print(":"+label.toString());
										}
										System.out.println(".");
										
									}else{
										//Set<Integer>[] patternSet = {null,null};
										System.out.println("Not contains destLabel: "+destLabel.toString()+". Node: "+dNode.toString()+";");
										List<Set<Integer>> patternSet = new ArrayList<Set<Integer>>();
										Set<Integer> labelSet1 = new HashSet<Integer>();
										for(Integer label :vertexIndex.get(sNode)){
											labelSet1.add(label);
										}
										Set<Integer> labelSet2 = new HashSet<Integer>();
										for(Integer label :vertexIndex.get(dNode)){
											labelSet2.add(label);
										}
										patternSet.add(labelSet1);
										patternSet.add(labelSet2);
										queryGraph.get(sourceLabel).put(destLabel, patternSet);
									}
								}else{
									System.out.println("Not contains sourceLabel: "+sourceLabel.toString()+". Node: "+sNode.toString()+";");
									System.out.println("destLabel: "+destLabel.toString()+". Node: "+dNode.toString()+";");
									Map<Integer,List<Set<Integer>>> innerEntry = new TreeMap<Integer,List<Set<Integer>>>();
									List<Set<Integer>> patternSet = new ArrayList<Set<Integer>>();
									Set<Integer> labelSet1 = new HashSet<Integer>();
									for(Integer label :vertexIndex.get(sNode)){
										System.out.println("Label: "+label+". Node: "+sNode);
										labelSet1.add(label);
									}
									Set<Integer> labelSet2 = new HashSet<Integer>();
									for(Integer label :vertexIndex.get(dNode)){
										System.out.println("Label: "+label+". Node: "+dNode);
										labelSet2.add(label);
									}
									patternSet.add(labelSet1);
									patternSet.add(labelSet2);
									innerEntry.put(destLabel, patternSet);
									queryGraph.put(sourceLabel, innerEntry);
								}
							
							System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++");
							}
						}	
					
				}*/
					
					
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
					//System.out.println(log);
					logger.info(log);
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

	}//loadQueryGraph

}
