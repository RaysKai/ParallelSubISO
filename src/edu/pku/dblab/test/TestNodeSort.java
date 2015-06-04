package edu.pku.dblab.test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jgrapht.GraphPath;
import org.jgrapht.Graphs;
import org.jgrapht.alg.DijkstraShortestPath;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;
import org.jgrapht.graph.ListenableUndirectedWeightedGraph;

public class TestNodeSort {

	private static final String IGRAPH_FILE = "D:\\data\\human.igraph";
	//private static final String QUERY_FILE = "D:\\data\\query-human-clique-q7-496.igraph";
	private static final String QUERY_FILE = "D:\\data\\human_clique7.igraph";
	//private static final String QUERY_FILE = "F:\\iGraph\\iGraph20\\querysets\\human\\gs\\human_q10.igraph";
	private static final Logger logger = Logger.getLogger(TestNodeSort.class);

	private static Map<Integer,Map<Integer,Set<int[]>>> inmemIndex = new HashMap<Integer,Map<Integer,Set<int[]>>>();
	private static Map<Integer,Map<Integer,Integer>> freqPattern = new HashMap<Integer,Map<Integer,Integer>>();
	private static Map<Integer,List<Integer>> adjIndex = new HashMap<Integer,List<Integer>>();
	private static Map<Integer,Set<Integer>> adjLabelIndex = new HashMap<Integer,Set<Integer>>();
	private static Map<Integer,Set<Integer>> dataLabelIndex = new HashMap<Integer, Set<Integer>>();
	private static Map<Integer,Integer> LabelFrequency = new HashMap<Integer, Integer>();
	
	//private static Map<Integer,Map<Integer,List<Set<Integer>>>> queryGraph = new HashMap<Integer,Map<Integer,List<Set<Integer>>>>();
	//private static Map<Integer,Map<Integer,List<Integer>>> queryGraph = new HashMap<Integer,Map<Integer,List<Integer>>>();
	//private static Map<Integer,Set<Integer>> queryLabelIndex = new HashMap<Integer,Set<Integer>>();
	
	//in Key: nodeID. Content in ArrayList: (0)degree, (1)label (2)frequency.
	private static List<Map<Integer,List<Integer>>> queryLabelIndex = new ArrayList<Map<Integer,List<Integer>>>();
	//in Key: source nodeID. Key:dest nodeID. ArrayList: (0)source label ID, (1)dest label ID, (2)freq of label pairs.
	private static List<Map<Integer,Map<Integer,List<Integer>>>> queryGraph = new ArrayList<Map<Integer,Map<Integer,List<Integer>>>>();

	private static List<String> R = new ArrayList<String>();
	private static int count = 0;
	
	/**
     * Select a query vertex u \in V(q) which is not yet matched.
     *
     * @param q: LabeledGraph, for query graph.
     * @param M: List, a partial embedding, contains pairs of a query vertex and a corresponding data vertex.
     *
     * @return a vertex u \in V(q) which is not yet matched..
     *
     */
	/**
     * Select a query vertex u \in V(q) which is not yet matched.
     *
     * @param q: LabeledGraph, for query graph.
     * @param M: List, a partial embedding, contains pairs of a query vertex and a corresponding data vertex.
     *
     * @return a vertex u \in V(q) which is not yet matched..
     *
     */
	//@SuppressWarnings("unchecked")
	public List<Integer[]> getSearchOrder(
			Map<Integer,List<Integer>> g, 					//data graph(adjIndex)
			Map<Integer,Set<Integer>> L_g, 					//V_g(dataLabelIndex)
			Map<Integer,Map<Integer,List<Integer>>> q, 		//query graph(queryGraph)
			Map<Integer,List<Integer>>  L_q, 					//V_q(queryLabelIndex)
			int k ) {
		
		List<Integer[]> order = new ArrayList<Integer[]>();
		
		//sort query vertex based on label frequency.
		List<QueryNode> sortedVertex = new ArrayList<QueryNode>();
		
		for(Integer vertex:L_q.keySet()){
			//System.out.println("sort vertex u"+vertex.toString());
			QueryNode cVertex = new QueryNode(vertex,L_q.get(vertex).get(1),L_q.get(vertex).get(2));
			if(L_q.get(vertex).get(2)!= null){
				sortedVertex.add(cVertex);
			}else{
				System.out.println("sort vertx u"+vertex.toString()+",label:"+L_q.get(vertex).get(1).toString());
				//query graph is wrong, return null order.
				return null;
			}
		}
		
		
       Collections.sort(sortedVertex, new Comparator() {
            @Override
            public int compare(Object queryNodeOne, Object queryNodeTwo) {
                //use instanceof to verify the references are indeed of the type in question
                return ((QueryNode)queryNodeOne).getFreq()
                        .compareTo(((QueryNode)queryNodeTwo).getFreq());
            }
        }); 
        
        String sortlist = "getQueryOrder():: Query Vertex: ";
        for(QueryNode node:sortedVertex){
        	sortlist = sortlist + "u"+node.getId()+"["+node.getLabel()+"]:"+node.getFreq()+"|";
        }
        logger.info(sortlist);
        //logger.info(sortlist);
       
        if(k >= sortedVertex.size()){
        	QueryNode parent = null;
        	for(QueryNode n:sortedVertex){
        		if(parent == null){
        			Integer[] entry = {n.getId(),null};
        			order.add(entry);
           		}else{
        			Integer[] entry = {n.getId(),parent.getId()};
        			order.add(entry);
            		}
        		parent = n;
        	}
        }else{
	        
            //get top-K vertices from list.
            List<QueryNode> topkList = new ArrayList<QueryNode>();
            for(int i=0;i<k;i++){
            	topkList.add(sortedVertex.get(i));
            	//System.out.println("Top-k node "+topkList.get(i).getId().toString());
            }
           
			//Construct graph
	        ListenableUndirectedWeightedGraph<String, DefaultWeightedEdge> graph = new ListenableUndirectedWeightedGraph<String, DefaultWeightedEdge>(
					DefaultWeightedEdge.class);
			
			for(Integer vertex:q.keySet()){
				graph.addVertex(vertex.toString());
			}
			
			for(Integer source:q.keySet()){
				for(Integer dest:q.get(source).keySet()){
					if(source.intValue()<dest.intValue()){
						double weight = q.get(source).get(dest).get(2).doubleValue();
						graph.setEdgeWeight(graph.addEdge(source.toString(), dest.toString()),weight);
					}else{
						continue;
					}
				}
			}
			
	        //foreach other vertices in query, compute total cost of shortest path to top-k vertices.
			double minWeight = Double.MAX_VALUE;
			List<Integer[]> keyStructure = null;
			Integer rootVertex = null;
			
			for(int i=k;i<sortedVertex.size();i++){
				//System.out.println("Check vertex u"+sortedVertex.get(i).getId().toString());
				
				//Set<DefaultWeightedEdge> edgeSet = new HashSet<DefaultWeightedEdge>();
				Map<Integer,GraphPath<String, DefaultWeightedEdge>> shortestPathList = new HashMap<Integer,GraphPath<String, DefaultWeightedEdge>>();
				//double totalWeight = 0.0d;
				for(int j=0; j<k;j++){
					DijkstraShortestPath<String, DefaultWeightedEdge> dsp = new DijkstraShortestPath<String, DefaultWeightedEdge>(
						graph, sortedVertex.get(i).getId().toString(), topkList.get(j).getId().toString(), Double.POSITIVE_INFINITY);
					if(dsp!=null){
						GraphPath<String, DefaultWeightedEdge> gp = dsp.getPath();
						if (gp != null) {
							shortestPathList.put(topkList.get(j).getId(), gp);
						}
							//graph.getPathVertexList(GraphPath<V,E> path) 
					}
				}
				
				//Check whether there exist containment among all shortest paths.
				List<Integer[]> entry = new ArrayList<Integer[]>();
				for(GraphPath<String, DefaultWeightedEdge> cPath : shortestPathList.values()){
					List<String> pathVertexList = Graphs.getPathVertexList(cPath);
					
					for(int pos = 0;pos < pathVertexList.size()-1; pos++){
						Integer nodeOne = Integer.valueOf(pathVertexList.get(pos));
						Integer nodeTwo = Integer.valueOf(pathVertexList.get(pos+1));
						Integer[] nodePair = {null,null};
						if(nodeOne.intValue() > nodeTwo.intValue()){
							nodePair[0] = nodeTwo;
							nodePair[1] = nodeOne;
						}else{
							nodePair[0] = nodeOne;
							nodePair[1] = nodeTwo;
						}
						
						boolean isContained = false;
						for(Integer[] pair:entry){
							if((pair[0].equals(nodePair[0]))&&(pair[1].equals(nodePair[1]))){
								isContained = true;
								//System.out.println("already has u"+pair[0].toString()+"-u"+pair[1].toString());
								break;
							}
						}
						if(!isContained){
							entry.add(nodePair);
						}
					}
					
		        }
				
				double treeWeight = 0.0d;
				for(Integer[] nodePair:entry){
					//System.out.println("===Entry: u"+nodePair[0].toString()+"-u"+nodePair[1].toString());
					treeWeight = treeWeight + q.get(nodePair[0]).get(nodePair[1]).get(2).doubleValue();
				}
				
		        if(treeWeight<minWeight){
		        	minWeight = treeWeight;
		        	keyStructure = entry;
		        	rootVertex = sortedVertex.get(i).getId();
			    }
				
			}//for
				
	        String log = "getQueryOrder():: root vertex is u"+rootVertex.toString()+", Edge list: |";
	        for(Integer[] pairs:keyStructure){
	        	log = log + "u"+pairs[0].toString()+"-u"+pairs[1].toString()+"|";
	        }
	        logger.info(log);
	        //logger.info(log);
	        
	        //set up search order.
	        List<Integer> topkcan = new ArrayList<Integer>();
	        for(QueryNode topku:topkList){
	        	topkcan.add(topku.getId());
	        }
	        
	        Integer[] entry1 = {topkcan.get(0),null};
	        order.add(entry1);
	        
	        for(int i = 0;i<topkcan.size();i++){
		        Integer curNode = topkcan.get(i);
	        	//System.out.println("CurNode:["+curNode.toString()+"]");
		       
	        	/*	        	boolean isConnectedToRoot = false;
	        	for(Integer[] pair:order){
		        	if((pair[0].equals(order.get(order.size()-1)[0])) && (pair[1].equals(order.get(order.size()-1)[1]))){
		        		isConnectedToRoot = true;
		        		break;
		        	}else if((pair[1].equals(order.get(order.size()-1)[0])) && (pair[0].equals(order.get(order.size()-1)[1]))){
		        		isConnectedToRoot = true;
		        		break;
		        	}
	        	}
	        	if(isConnectedToRoot) break;
	        	 */		        
	        	if(order.get(order.size()-1)[1] != null){
	        		//System.out.println("judge whether is connected to root: u"+order.get(order.size()-1)[0].toString()
	        		//		+"[u"+order.get(order.size()-1)[1].toString()+"]");
	        		if(q.get(order.get(order.size()-1)[0]).keySet().contains(order.get(order.size()-1)[1])){
	        			break;
	        		}
	        	}
	        	
	        	
	        	for(int j=0;j<keyStructure.size();j++){
	    	        Integer[] entry = new Integer[2];
		        	for(Integer[] pair:keyStructure){
			        	//System.out.println("entry:["+curNode.toString()+"], pair:["+pair[0].toString()+"-"+pair[1].toString()+"],size "+order.size());
			        	if(pair[0].equals(curNode)){
			        		entry[1] = curNode;
			        		entry[0] = pair[1];
					        order.add(entry);
					        curNode = entry[0];
					        for(Integer topku:topkcan){
					        	//System.out.println("curNode: "+curNode.toString()+",topku: "+topku.toString());
					        	if(curNode.equals(topku)){
					        		topkcan.remove(curNode);
				        			//System.out.println("remove vertex : u"+curNode.toString()+",size: "+topkcan.size());
					        		break;
					        	}
					        }
					        //System.out.println("0 match");
			        		break;
			        	}else if(pair[1].equals(curNode)){
			        		entry[1] = curNode;
			        		entry[0] = pair[0];
					        order.add(entry);
					        curNode = entry[0];
					        for(Integer topku:topkcan){
					        	//System.out.println("curNode: "+curNode.toString()+",topku: "+topku.toString());
					        	if(curNode.equals(topku)){
					        		topkcan.remove(curNode);
				        			//System.out.println("remove vertex : u"+curNode.toString()+",size: "+topkcan.size());
					        		break;
					        	}
					        }
			        		//System.out.println("1 match");
			        		break;
			        	}
/*			        	else{
			        		System.out.println("No match");
			        	}
*/			        }
			        
		        	//System.out.println("new entry: u"+entry[0].toString()+"["+entry[1].toString()+"]");
			        
			        //if pass a top-k node.
			        if(curNode != null){
			        	//Integer[] entry = new Integer[2];
				        //if reach root node.
			        	if(curNode.equals(rootVertex)){
			        		if(i+1>=topkcan.size()){
			        			//System.out.println("Break:"+i+","+topkcan.size());
			        			break;
			        		}else{
			        			boolean isRootVertexContained = false;
			        			for(Integer pair[]:order){
			        				if(pair[0].equals(curNode)){
			        					isRootVertexContained = true;
			        					break;
			        				}
			        			}
			        			if(isRootVertexContained){
				        			entry = new Integer[2];
				        			entry[0] = topkcan.get(i+1);
					        		entry[1] = rootVertex;
				        			//System.out.println("match: u["+topkcan.get(i+1).toString()+"],size "+order.size());
					        		order.add(entry);
			        			}
				        		//topkcan.remove(entry[0]);
				        		//j=keyStructure.size();
				        		break;
			        		}
			        	}
			        }
			        else{
			        	System.out.println("NULL");
			        }

	        	}
	        	
/*		        log = "order: ";
		        for(Integer[] u:order){
		        	if(u[1] != null){
		        		log = log + "u"+u[0].toString()+"[u"+u[1].toString()+"] -> ";
		        	}else{
		        		log = log + "u"+u[0].toString()+"[null] -> ";
		        	}
		        }
		        System.out.println(log);
*/

	        }
	        
	        //order.remove(order.size()-1);
	        
/*	        log = "order: ";
	        for(Integer[] u:order){
	        	if(u[1] != null){
	        		log = log + "u"+u[0].toString()+"[u"+u[1].toString()+"] -> ";
	        	}else{
	        		log = log + "u"+u[0].toString()+"[null] -> ";
	        	}
	        }
	        System.out.println(log);
*/        
	        //after get the order of skeleton structure, arrange order of rest node.
	        //get the lowest-ranked rested vertices, check the traversable vertex.
	        
	        for(QueryNode n:sortedVertex){
	        	if(!isQueryNodeAssigned(n.getId(),order)){
	        		//System.out.println("Vertex u"+n.getId().toString()+" has been assigned to order list.");
 	        		Integer[] entry = new Integer[2];
    				entry[1] = getParent(n.getId(),order,q);
    				entry[0] = n.getId();
	        		order.add(entry);
	        		
	        		queryDFS(n.getId(),order,q);
	        	
	        	}
	        }
	        
	        order.get(0)[1]=rootVertex;

	        log = "getQueryOrder():: order: ";
	        for(Integer[] u:order){
	        	if(u[1] != null){
	        		log = log + "u"+u[0].toString()+"[u"+u[1].toString()+"] -> ";
	        	}else{
	        		log = log + "u"+u[0].toString()+"[null] -> ";
	        	}
	        }
	        logger.info(log);
        
        
        
        }//else

        return order;
	
	}
	
	public boolean isQueryNodeAssigned(Integer vertex, List<Integer[]> order){
		boolean isAssigned = false;
		
		for(Integer[] pair:order){
			if(pair[0].equals(vertex)){
				isAssigned = true;
				//System.out.println("Vertex u"+vertex.toString()+" is "+ isAssigned);
				break;
			}
		}
		
		return isAssigned;
	}
	
	public Integer getParent(Integer vertex, List<Integer[]> order, Map<Integer,Map<Integer,List<Integer>>> q){
		Integer parent = null;
		
/*		for(Integer[] pair:order){
			if(pair[0].equals(vertex)){
				parent = pair[1];
			}
		}
*/		
		for(Integer n:q.get(vertex).keySet()){
			if(isQueryNodeAssigned(n,order)){
				parent = n;
			}
		}
		return parent;
	}
	
	public void queryDFS(Integer vertex, List<Integer[]> order, Map<Integer,Map<Integer,List<Integer>>> q){
		//Integer nextNode = null;
		
		for(Integer neighbor:q.get(vertex).keySet()){
			if(!isQueryNodeAssigned(neighbor,order)){
				Integer[] entry = new Integer[2];
				entry[1] = getParent(vertex,order,q);
				entry[0] = neighbor; 
				order.add(entry);
				//System.out.println("Add order: u"+neighbor.toString()+"[u"+entry[1].toString()+"]");
				queryDFS(neighbor,order,q);
				break;
			}
		}
		
		//return nextNode;
	}
	
	
	
	public static void loadDataGraph() {

		try {
			// Reading from iGraph file
			FileReader igraphFile = new FileReader(IGRAPH_FILE);
			BufferedReader br = new BufferedReader(igraphFile);
			
			String str = br.readLine();
			
			int num_label = 0;
			//Map<Integer,Set<Integer>> vertexIndex = new HashMap<Integer,Set<Integer>>();
			
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
									Map<Integer, Set<int[]>> innerEntry = new HashMap<Integer, Set<int[]>>();
									innerEntry.put(destLabel, edgeSet);
									inmemIndex.put(sourceLabel, innerEntry);
									Map<Integer, Integer> innerFreq = new HashMap<Integer, Integer>();
									innerFreq.put(destLabel, Integer.valueOf(1));
									freqPattern.put(sourceLabel, innerFreq);

								}
								*/
								
								//Comment out all code related to immemIndex construction.
								
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
									Map<Integer, Set<int[]>> innerEntry = new HashMap<Integer, Set<int[]>>();
									innerEntry.put(destLabel, edgeSet);
									inmemIndex.put(sourceLabel, innerEntry);
									Map<Integer, Integer> innerFreq = new HashMap<Integer, Integer>();
									innerFreq.put(destLabel, Integer.valueOf(1));
									freqPattern.put(sourceLabel, innerFreq);

								}*/
								
								if (freqPattern.containsKey(sourceLabel)) {
									if (freqPattern.get(sourceLabel).containsKey(destLabel)) {
										// pairs of source-dest have already
										// stored in in-memory index.
										//Set<int[]> edgeSet = inmemIndex.get(sourceLabel).get(destLabel);
										//edgeSet.add(newEdge);
										Integer freq = Integer.valueOf(freqPattern.get(sourceLabel).get(destLabel).intValue() + 1);
										freqPattern.get(sourceLabel).put(destLabel, freq);
									} else {
										//Set<int[]> edgeSet = new HashSet<int[]>();
										//edgeSet.add(newEdge);
										//inmemIndex.get(sourceLabel).put(destLabel, edgeSet);
										freqPattern.get(sourceLabel).put(destLabel, Integer.valueOf(1));
									}
								} else {
									//Set<int[]> edgeSet = new HashSet<int[]>();
									//edgeSet.add(newEdge);
									//Map<Integer, Set<int[]>> innerEntry = new HashMap<Integer, Set<int[]>>();
									//innerEntry.put(destLabel, edgeSet);
									//inmemIndex.put(sourceLabel, innerEntry);
									Map<Integer, Integer> innerFreq = new HashMap<Integer, Integer>();
									innerFreq.put(destLabel, Integer.valueOf(1));
									freqPattern.put(sourceLabel, innerFreq);

								}
								
								
								
							}

						}
						
						//Construct adjacent list
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
					//logger.info(log);
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
				//logger.info(log);
			}
			
			for(Set<Integer> labels:dataLabelIndex.values()){
				for(Integer label:labels){
					if(LabelFrequency.containsKey(label)){
						Integer newFreq = Integer.valueOf(LabelFrequency.get(label).intValue()+1);
						LabelFrequency.put(label, newFreq);
					}else{
						LabelFrequency.put(label, Integer.valueOf(1));
					}
				}
			}
			
			//Construct adjacent label index. Using adjIndex and dataLabelIndex.
			for(Integer source:adjIndex.keySet()){
				Set<Integer> entry = new HashSet<Integer>();
				for(Integer dest:adjIndex.get(source)){
					for(Integer entryLabel:dataLabelIndex.get(dest)){
						entry.add(entryLabel);
					}
				}
				adjLabelIndex.put(source,entry);
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
			
			//List<String> queryBlock = new ArrayList<String>();
			
			String str = br.readLine();
			
			int num_label = 0;
			Map<Integer,List<Integer>> vertexIndexEntry = new HashMap<Integer,List<Integer>>();
			Map<Integer,Map<Integer,List<Integer>>> queryGraphEntry  = new HashMap<Integer,Map<Integer,List<Integer>>>();
			//Map<Integer,List<Integer>> querGraph = new HashMap<Integer,List<Integer>>();
			
			while (str != null) {
				String[] words = str.split(" ");
				switch(words[0]){
				case "v":
					Integer nodeID = Integer.valueOf(words[1]);
					List<Integer> labelSet = new ArrayList<Integer>();
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
						//Map<Integer, List<Integer>> adjEntry = new HashMap<Integer, List<Integer>>();
						//adjEntry.put(destNode, contentList);
						queryGraphEntry.get(sourceNode).put(destNode, contentList);
						
						if(queryGraphEntry.containsKey(destNode)){
							queryGraphEntry.get(destNode).put(sourceNode, contentList);
							//System.out.print("contains destNode: "+destNode.toString());
						}else{
							Map<Integer, List<Integer>> adjEntry = new HashMap<Integer, List<Integer>>();
							adjEntry.put(sourceNode, contentList);
							queryGraphEntry.put(destNode,adjEntry);
							//System.out.print("not contains destNode: "+destNode.toString());
						}
					}else{
						//System.out.print("Not contains sourceNode: "+sourceNode.toString()+",");
						Map<Integer, List<Integer>> adjSourceEntry = new HashMap<Integer, List<Integer>>();
						adjSourceEntry.put(destNode, contentList);
						queryGraphEntry.put(sourceNode,adjSourceEntry);
						
						if(queryGraphEntry.containsKey(destNode)){
							queryGraphEntry.get(destNode).put(sourceNode,contentList);
							//System.out.print("contains destNode: "+destNode.toString()+",");
						}else{
							Map<Integer, List<Integer>> adjDestEntry = new HashMap<Integer, List<Integer>>();
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
						String code = ("Query "+blockId+" : ");
						for(Integer vertex:queryGraphEntry.keySet()){
							//for(Integer label:vertexIndexEntry.keySet()){
								int degree = queryGraphEntry.get(vertex).size();
								vertexIndexEntry.get(vertex).add(0,Integer.valueOf(degree));
								Integer labelFreq = LabelFrequency.get(vertexIndexEntry.get(vertex).get(1));
								vertexIndexEntry.get(vertex).add(labelFreq);
								code = code + "u"+vertex.toString()+":"+vertexIndexEntry.get(vertex).get(0)+
										", label: "+ vertexIndexEntry.get(vertex).get(1)+ ", Freq: "+ vertexIndexEntry.get(vertex).get(2)+ "| ";
							//}
						}
						//Key
						//System.out.println(code);
						queryLabelIndex.add(vertexIndexEntry);
						vertexIndexEntry = new HashMap<Integer,List<Integer>>();
						queryGraphEntry  = new HashMap<Integer,Map<Integer,List<Integer>>>();
					}
					//logger.info("Query "+blockId+" is add to queryGraph.");

/*					if(blockId > 0){
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
*/
					break;

				default:
					break;
				}//case
				str = br.readLine();
			}//while
			
			
			br.close();
			queryFile.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}//loadQueryGraph

	
	
	/**
	 * @param args
	 */
	public static void main(String args[]) {

		//Record Start time of each query.

		long startTime = System.currentTimeMillis();

		loadDataGraph();
		
		loadQueryGraph();
		
		long endTime = System.currentTimeMillis();
		long queryTime = endTime - startTime;

		//String s = "Total index running time:"+queryTime;
		//logger.info(s);
		//logger.info(s);
		
		TestNodeSort tns = new TestNodeSort();

		for(int i=0;i<queryGraph.size();i++){
			//@TEST
			logger.info("====================== Query "+i+" ======================");
			for (Integer sourceNode : queryGraph.get(i).keySet()) {
				for (Integer destNode : queryGraph.get(i).get(sourceNode).keySet()) {
					//String log = "Query node (" + sourceNode.toString() + "," + destNode.toString() + "), ";
					List<Integer> contentList = queryGraph.get(i).get(sourceNode).get(destNode);
					int frequecy = contentList.get(2).intValue();
					//log = log + "Freq:" + frequecy +", Label: "+contentList.get(0)+"--"+contentList.get(1)+".";
					//System.out.println(log);
					//Key
					//System.out.println(log);
				}
			}	

			List<Integer[]> test = tns.getSearchOrder(adjIndex,dataLabelIndex, queryGraph.get(i),
				queryLabelIndex.get(i),	3);
			if(test == null) {
				logger.info("Error in query graph");
			}
		}


	}
	
	class QueryNode{
		Integer id;
		Integer label;
		Integer freq;
		
		QueryNode(Integer id,Integer label,Integer freq){
			this.id = id;
			this.label = label;
			this.freq = freq;
		}
		
		public Integer getId(){
			return id;
		}
		
		public Integer getLabel(){
			return label;
		}
		
		public Integer getFreq(){
			return freq;
		}
	}

	
}
