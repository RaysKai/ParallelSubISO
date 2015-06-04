package edu.pku.dblab.parallel;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
//import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.LinkedList;
//import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

//import org.jgrapht.alg.DijkstraShortestPath;
//import org.jgrapht.graph.DefaultWeightedEdge;
//import org.jgrapht.graph.SimpleDirectedWeightedGraph;
import org.jgrapht.alg.*;
import org.jgrapht.*;
import org.jgrapht.graph.*;
import org.apache.log4j.Logger;
//import org.jgrapht.graph.DefaultEdge;

public class OrderedBacktrackVerFour {

	//private static final String IGRAPH_FILE = "D:\\data\\example-full.igraph";
	//private static final String QUERY_FILE = "D:\\data\\example-query.igraph";
	private static float THRESHOLD = 1.0f;
	private static int TOPKV = 3;
	
	
	private static final String IGRAPH_FILE = "D:\\data\\human.igraph";
	//private static final String QUERY_FILE = "D:\\data\\human_q10.igraph";
	private static final String QUERY_FILE = "D:\\data\\error-query-human-q10-800.igraph";
	//private static final String QUERY_FILE = "D:\\data\\error-query-human-q10-101.igraph";
	
	//private static final String QUERY_FILE = "F:\\iGraph\\iGraph20\\querysets\\human\\gs\\human_q10.igraph";
	//private static final String QUERY_FILE = "F:\\iGraph\\iGraph20\\querysets\\human\\cliques\\human_clique7.igraph";
	
	private static final Logger logger = Logger.getLogger(OrderedBacktrack.class);

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
     * Generic subgraph isomorphism algorithm
     *
     * @param g: LabeledGraph, for data graph to be queried.
     * @param q: LabeledGraph, for query graph.
     * @param k: int, return when top-k isomorphism structures in g have been explored.
     *
     * @return void.
     *
     */
	//@SuppressWarnings("rawtypes")
	public void GenericQueryProc(
			Map<Integer,List<Integer>> g, 					//data graph(adjIndex)
			Map<Integer,Set<Integer>> L_g, 					//V_g(dataLabelIndex)
			Map<Integer,Map<Integer,List<Integer>>> q, 		//query graph(queryGraph)
			Map<Integer,List<Integer>>  L_q 					//V_q(queryLabelIndex)
			) {
//	public void GenericQueryProc(LabeledGraph g, LabeledGraph q ) {
		
		//M: a partial embedding, contains pairs of a query vertex and a corresponding data vertex.
		//data structure: pairs as array. the first element is query vertex, the second element is corresponding data vertex.
		//if |M|=|v(q)|, the algorithm finds a complete solution.
		List<Object[]> M = new ArrayList<Object[]>();
		
		//C(u): a set of candidate vertices.
		List<Object> C_u = new ArrayList<Object>();

		//Vector F denote which vertices of g have been used at an intermediate state of the match computation.
		//The data structure of F can be optimized to only use one bit for an ordered list of vertices. 
		//Map<Object, Set<Object>> F = new HashMap<Object,Set<Object>>();
		//unmatched refined candidate set.
		//Map<Object,Set<Object>> uF = new HashMap<Object,Set<Object>>();

		//2014-12-14 get search order.
		//for pre-determine candidate set of reverse ordered query node.
		Map<Integer,Set<Integer>> cR = new HashMap<Integer,Set<Integer>>();
		List<Integer[]> order = getSearchOrder(g, L_g, q,L_q, cR, TOPKV );
		

		C_u = (ArrayList<Object>) FilterCandidate(g,L_g,q,L_q,order.get(0)[0]);
		
/*		String scu = "GenericQueryProc:: Candidate for first vertex u"+order.get(0)[0].toString()+" : ";
		for(Object c:C_u){
			scu = scu +"|"+c.toString();
		}
		logger.info(scu);
*/		

		Set<Object> visited = new HashSet<Object>();

		
		for(int i=0;i<C_u.size();i++){
			//System.out.println("First Query Vertex: "+C_u.get(i));
			M = new ArrayList<Object[]>();
			//F = new HashSet<Object>();
			//Vector F denote which vertices of g have been used at an intermediate state of the match computation.
			//The data structure of F can be optimized to only use one bit for an ordered list of vertices. 
			Map<Object, Set<Object>> F = new HashMap<Object,Set<Object>>();
			//unmatched refined candidate set.
			Map<Object,Set<Object>> uF = new HashMap<Object,Set<Object>>();
			
			//get search order do the job.
			//Object[] first_matched_pair={startVertex,C_u.get(i)};
			Object[] first_matched_pair={order.get(0)[0],C_u.get(i)};
			
			M.add(first_matched_pair);
			visited.add(C_u.get(i));
			//logger.info("First query node: u"+order.get(0)[0].toString()+"(v"+C_u.get(i).toString()+")");

			if(i==0){
				//F.put(startVertex, visited);
				F.put(order.get(0)[0], visited);
			}
			//F.add(C_u.get(i));
			SubgraphSearch(g,L_g,q,L_q,M,F,uF,cR, order);
			//logger.info("#################################################################");
		
		}
		
		//Print testing result.
		//logger.info("#################################################################");
		int total_matched = 0 ;
		for(String result:R){
			//logger.info(result);
			total_matched ++;
		}
		logger.info("Total matched subgraphs::"+total_matched);

		logger.info("Total recursive counts::"+count);
		logger.info("################################################################");
		
		R = new ArrayList<String>();
		
	}

    /**
     * Main recursive function for subgraph isomorphism search.
     *
     * @param g: LabeledGraph, for data graph to be queried.
     * @param q: LabeledGraph, for query graph.
     * @param M: List, a partial embedding, contains pairs of a query vertex and a corresponding data vertex.
     * @param F: Map, a vector for indication of matched vertex in g. entry is vertex, value is boolean object.
     * @param d: int, top-k matched. 
     *
     * @return void.
     *
     */
	//@SuppressWarnings("rawtypes")
//	public void SubgraphSearch(LabeledGraph g, LabeledGraph q, List<Object[]> M, Map<Object,Boolean> F, int d) {
	public Object SubgraphSearch(
			Map<Integer,List<Integer>> g, 					//data graph(adjIndex)
			Map<Integer,Set<Integer>> L_g, 					//V_g(dataLabelIndex)
			Map<Integer,Map<Integer,List<Integer>>> q, 		//query graph(queryGraph)
			Map<Integer,List<Integer>>  L_q, 					//V_q(queryLabelIndex)
			List<Object[]> M, 
			Map<Object,Set<Object>> F,
			Map<Object,Set<Object>> uF,
			Map<Integer,Set<Integer>> cR,
			List<Integer[]> order) {
		
		//@TEST: define total recursive call times.		
		Object parentVertex = null;
		count++;
		//if(count>=10000) return null;
		if(R.size()>=1000) return null;
		//System.out.println("Start query.");
		
		//Have been backtracked to the first node.
		if(M.size()==0) return null;
		
		//main processing unit.
		if(M.size()<q.keySet().size() ){
			
			//not yet matched query vertex.
			Object[] u = NextQueryVertex(M,order);
			
			if(u == null) return null;
			//logger.info("NextQueryVertex():: get vertex q"+u[0].toString()+", prev vertex q"+u[1].toString());
			
			//get refined candidate list C(R) of vertex u. 
			//add code to deal with unusedCList.

			Set<Object> C_R = RefineCandidates(g,L_g,q,L_q,M,u,F,cR);
			
			if(C_R.size()==0){
				//parentVertex = ClearState(M,F,u);
				//logger.info("SubgraphSearch():: current query vertex u"+u[0].toString()+",v"+u[1].toString());
				if(u != null) {
					parentVertex = getBackableQueryVertex(q,M,F,uF,order,u);
					return parentVertex;
				}
			}

			if(!uF.containsKey(u[0])){
				//C_R = RefineCandidates(g,L_g,q,L_q,M,u,F, cR);
				Set<Object> uFentry = new HashSet<Object>();
				for(Object o:C_R){
					uFentry.add(o);
				}
				uF.put(u[0],uFentry);
			}
			
			
			//@TEST: print all elements in Candidate set.
//			for(Object e:C_R){
//				System.out.print("C(R)"+e.toString()+" ");
//			}
			logger.info("SubgraphSearch():: candidate u"+u[0]+" C(R) size: "+C_R.size());
			
			if(C_R.size()==0){
				//parentVertex = ClearState(M,F,u);
				//logger.info("SubgraphSearch():: current query vertex u"+u[0].toString()+",v"+u[1].toString());
				if(u != null) {
					parentVertex = getBackableQueryVertex(q,M,F,uF,order,u);
					return parentVertex;
				}
			}

/*			String content = "SubgraphSearch():: List candidates in C_R: ";
			for(Object candidate : C_R){
				content = content + "v"+candidate.toString()+"|";
			}
			logger.info(content);
*/

			for(Object v:C_R){
	        	boolean isMatched = false;
	        	//logger.info("Current query vertex is u"+u[0].toString());
				for(Object vertex:F.get(u[0])){
					if(vertex.equals(v)){
						isMatched = true;
						break;
					}
				}
				//logger.info("SubgraphSearch()::u"+u[0].toString()+"-v"+v.toString()+" isMatched in M: "+isMatched+". Parent u"+u[1].toString());

				//vertex v \in C(R) and is not yet matched.
				if(!isMatched) {
					F.get(u[0]).add(v);
					if(IsJoinable(g,q,L_g,L_q,M,u[0],v)){
					
						UpdateState(M,F,uF,u[0],v);
						Object parent = SubgraphSearch(g,L_g,q,L_q,M,F,uF,cR,order);
						//System.out.println("current vertex is u"+u[0].toString());
						if(parent == null) return null;
						if(parent.equals(u[0])){
							RestoreState(M,F,u,v);
						}else{
							return parent;
						}

					}//isJoinable()
				}

			}//looped all candidate in C_R.
			
			//parentVertex = ClearState(M,F,u);
			parentVertex = getBackableQueryVertex(q,M,F,uF,order,u);

		
			//System.out.println("TEST:size: M="+M.size()+",V(q)="+V_q.size()+",C(R)="+C_R.size());
		}else{
			//logger.info("M Size::"+M.size());
			
			String result = "SubgraphSearch():: Matched Result:: ";
			for(Object[] pairs:M){
				result = result + "V"+pairs[1].toString()+"(U"+pairs[0].toString()+"),";
			}
			R.add(result);
			logger.info("++++++++++++++++++++++++++++++++++++++++++++++++++++");
			logger.info(result+", count:"+R.size());
			logger.info("++++++++++++++++++++++++++++++++++++++++++++++++++++");
			
			return M.get(M.size()-1)[0];
		}
		
		return parentVertex;
		
		//return candidate;
	}

    /**
     *Check whether the label set of vertex u is contained in the label set of vertex v.
     *
     * @param g: LabeledGraph, for data graph to be queried.
     * @param q: LabeledGraph, for query graph.
     * @param u: Object, vertex in query to be matched.
     * @param v: Object, vertex in data graph.
     *
     * @return boolean: true is L(u) \in L(v).
     *
     */
	//@SuppressWarnings("rawtypes")
	public boolean IsLabelSetContained(
		Map<Integer,Set<Integer>> L_g, 					//V_g(dataLabelIndex)
		Map<Integer,List<Integer>>  L_q ,					//V_q(queryLabelIndex)
		Object u, Object v ){
		
	    Set<Integer> L_v = L_g.get(v);
	    Integer L_u = L_q.get(u).get(1);
	    
   
	    if(L_v.contains(L_u)){
	    	return true;
	    }
	    return false;
	}

	
	/**
     * Filtering candidates, based on g and q.
     *
     * @param g: LabeledGraph, for data graph to be queried.
     * @param q: LabeledGraph, for query graph.
     * @param u: Object, vertex in query to be matched.
     *
     * @return List<Object>: all matched vertex in graph for a given query vertex.
     *
     */
	//@SuppressWarnings("rawtypes")
	public List<Object> FilterCandidate(
			Map<Integer,List<Integer>> g, 					//data graph(adjIndex)
			Map<Integer,Set<Integer>> L_g, 					//V_g(dataLabelIndex)
			Map<Integer,Map<Integer,List<Integer>>> q, 		//query graph(queryGraph)
			Map<Integer,List<Integer>>  L_q, 					//V_q(queryLabelIndex)
			Object u ){
		
		List<Object> candidate=new ArrayList<Object>();

		//For backtrack algorithm, filtering is only graph label matching.
		Set<Integer> V_g = L_g.keySet(); 
	    
		//boolean isLabelMatch = false;
	    for(Object v:V_g){
	    	if(IsLabelSetContained(L_g,L_q,u,v)){
	    		//candidate.add(v);
	    		//degree in g must eg. than q.
	    		if(g.get(v).size()>=(q.get(u).size())){
		    		
	    			//get neighbor information in q.
	    			//check neighbor label containment. whether current vertex contains all neighbors's label set in q
	    			boolean isNeighborLabelContained = false;
	    			//get label set of vertex in g.
	    			//Set<Integer> gLabelSet = adjLabelIndex.get(candidate);
	    			//Set<Integer> gLabelSet = L_g.get(v);
//	    			String c = "FilterCandidate():: Labels for v"+v.toString()+":";
//	    			for(Integer i:gLabelSet){
//	    				c = c + i.toString() +"|";
//	    			}
//	    			logger.info(c);
	    			//get all neighbor of query vertex in q.
	    			Set<Object> matchedVertex = new HashSet<Object>();
	    			for(Integer neighbor:q.get(u).keySet()){
	    				Integer nLabel = L_q.get(neighbor).get(1);
	    				//logger.info("FilterCandidate():: query u"+neighbor.toString()+",label "+nLabel.toString());
	    				isNeighborLabelContained = false;
	    				for(Integer gNeighbor:g.get(v)){
	    	    			//logger.info("FilterCandidate():: data v"+v.toString()+" neighbor v"+gNeighbor.toString());
		    				if(L_g.get(gNeighbor).contains(nLabel)){
		    	    			if(matchedVertex.contains(gNeighbor)){
		    	    				continue;
		    	    			}else{
			    	    			//logger.info("FilterCandidate():: query u"+neighbor.toString()+",data v"+gNeighbor.toString()+",label "+nLabel.toString());
			    					matchedVertex.add(gNeighbor);
			    					isNeighborLabelContained = true;
			    					break;
		    		    		}
		    				}
		    			}
	    				if(!isNeighborLabelContained){
	    					break;
	    				}
	    				}
	    				
	    			
	    			if(isNeighborLabelContained) {
	    				candidate.add(v);
	    			}
		    		
    			
	    		}
	    	}
	    }
		
		return candidate;
	}
	
	/**
     * Filtering candidates, based on g and q, considering matched vector F.
     *
     * @param g: LabeledGraph, for data graph to be queried.
     * @param q: LabeledGraph, for query graph.
     * @param F: match indication vector.
     * @param u: Object, vertex in query to be matched.
     *
     * @return List<Object>: all matched vertex in graph for a given query vertex.
     *
     */

	//@SuppressWarnings("rawtypes")
	public List<Object> FilterCandidate(
			Map<Integer,Set<Integer>> L_g, 					//V_g(dataLabelIndex)
			Map<Integer,List<Integer>>  L_q, 				//V_q(queryLabelIndex)
			Object u,
			Map<Object,Set<Object>> F,
			List<Object[]> M ){
		List<Object> candidate=new ArrayList<Object>();

		//For Ullmann algorithm, filtering is only graph label matching.
		Set<Integer> V_g = L_g.keySet();
		
	    for(Object v:V_g){
	    	//if value in F is false(not been matched)
	    	if(!(F.get(u).contains(v))){
	    		boolean containedinM = false;
	    		for(Object[] pairs:M){
	    			if(pairs[1].equals(v)){
	    				containedinM = true;
	    				break;
	    			}
	    		}
	    		if((!containedinM) && IsLabelSetContained(L_g,L_q,u,v)){
		    		candidate.add(v);
		    	}
	    	}
	    }
		return candidate;
	}


	/**
     * Select a query vertex u \in V(q) which is not yet matched.
     *
     * @param q: LabeledGraph, for query graph.
     * @param M: List, a partial embedding, contains pairs of a query vertex and a corresponding data vertex.
     *
     * @return a vertex u \in V(q) which is not yet matched..
     *
     */

	public Integer[] NextQueryVertex(
			List<Object[]> M,
			List<Integer[]> order ){

		//List<Object[]> candidate = new ArrayList<Object[]>();
		Integer[] v = new Integer[2];

		//BFS based next query vertex.
		try{
			if(M.size()==0){
				return null;
			}else{
				Object u = M.get(M.size()-1)[0];
				for(int i = 0; i< order.size(); i++){
					if(order.get(i)[0].equals(u)){
						//M.add(order.get(i+1));
						v = order.get(i+1);
						
					}
				}
			}	
				
		}catch(Exception e){
			return null;
		}
		
		logger.info("NextQueryVertex():: get next query vertex u"+v[0].toString()+"[u"+v[1].toString()+"]");
		return v;

		/*Object u = M.get(M.size()-1)[0];
		for(int i = 0; i< order.size(); i++){
			if(order.get(i)[0].equals(u)){
				M.add(order.get(i+1));
				logger.info("NextQueryVertex():: get next query vertex u"+order.get(i+1)[0].toString()+"[u"+order.get(i+1)[1].toString()+"]");
				return order.get(i+1);
			}
		}
		return null;*/
		
	}
	
    /**
     * Obtain a refined candidate certex set C_R from C_u by using algorithm-specific pruning rules.
     * For Ullmann algorithm, it only uses the degree of data graph vertex.
     *
     * @param g: LabeledGraph, for data graph to be queried.
     * @param q: LabeledGraph, for query graph.
     * @param M: List, a partial embedding, contains pairs of a query vertex and a corresponding data vertex.
     * @param F: Map, a vector for indication of matched vertex in g. entry is vertex, value is boolean object.
     *
     * @return a vertex u \in V(q) which is not yet matched..
     *
     */
	
	//@SuppressWarnings({ "rawtypes", "unchecked" })
	public Set<Object> RefineCandidates(//LabeledGraph g, LabeledGraph q, 
			Map<Integer,List<Integer>> g, 					//data graph(adjIndex)
			Map<Integer,Set<Integer>> L_g, 					//V_g(dataLabelIndex)
			Map<Integer,Map<Integer,List<Integer>>> q, 		//query graph(queryGraph)
			Map<Integer,List<Integer>>  L_q, 					//V_q(queryLabelIndex)
			List<Object[]> M, 
			Object[] u,
			Map<Object,Set<Object>> F,
			Map<Integer,Set<Integer>> cR ){

		Set<Object> RefinedC_u = new HashSet<Object>();
		if(!F.containsKey(u[0])){
			Set<Object> visited = new HashSet<Object>();
			F.put(u[0], visited);
		}
		
		List<Object> C_u = FilterCandidate(L_g, L_q, u[0], F, M);
		//logger.info("RefineCandidates():: u"+u[0].toString()+",parent u"+u[1].toString()+". Latest matched in M, u"+M.get(M.size()-1)[0]+"(u"+M.get(M.size()-1)[1]+"). Candidate Size: "+C_u.size());

/*		for(Integer source:g.keySet()){
			for(Integer dest:g.get(source)){
				logger.info("adjecent list of v"+source.toString()+" is v"+dest.toString());
			}
		}
*/		
		
		List<Integer> preVertex = new ArrayList<Integer>();
		for(Object[] pairs: M){
			if(pairs[0].equals(u[1])){
				preVertex.add((Integer)pairs[1]);
			}
		}
		
		for (Object candidate : C_u) {
			// A Matrix of degree for g should be precomputed, instead of using
			// degreeOf function.
			//int g_degree = g.degreeOf(candidate);
			//int q_degree = q.degreeOf(u);
			int g_degree = 0;
			int q_degree = 0;
			try{
				g_degree = g.get(candidate).size();
			}catch(Exception e){
				g_degree = 0;
			}
			try{
				//q_degree = q.get(u[0]).size();
				q_degree = L_q.get(u[0]).get(0).intValue();
				//if(q_degree > 2){
				//}
			}catch(Exception e){
				q_degree = 0;
			}
			//logger.info("RefineCandidates():: u"+u[0].toString()+" degree "+q_degree + ", candidate v"+candidate.toString()+" degree is "+g_degree);
			
			
			//System.out.println("RefineCandidates()::g" + candidate.toString());
			if ((g_degree ==0)||(q_degree==0)||(g_degree < q_degree)) continue;
			
			//2014/12-19 check whether has reverse order candidate;
			if(cR.containsKey(u[0])){
				if((cR.get(u[0]).size()==0) || (!cR.get(u[0]).contains(candidate))){
					//logger.info("RefineCandidates() :: v"+candidate.toString() + "NOT IN REVERSE ORDER LIST of u"+u[0].toString());
					continue; 
				}
			}
			
			
			//check neighbor label containment. whether current vertex contains all neighbors's label set in q
			/*boolean isNeighborLabelContained = true;
			//get label set of vertex in g.
			//Set<Integer> gLabelSet = adjLabelIndex.get(candidate);
			Set<Integer> gLabelSet = L_g.get(candidate);
			//get all neighbor of query vertex in q.
			for(Integer neighbor:q.get(u[0]).keySet()){
				Integer nLabel = L_q.get(neighbor).get(1);
				if(!gLabelSet.contains(nLabel)){
					isNeighborLabelContained = false;
					break;
				}
			}
			
			if(!isNeighborLabelContained) continue;*/
			boolean isNeighborLabelContained = true;
			Set<Object> matchedVertex = new HashSet<Object>();
			for(Integer neighbor:q.get(u[0]).keySet()){
				Integer nLabel = L_q.get(neighbor).get(1);
				//logger.info("FilterCandidate():: query u"+neighbor.toString()+",label "+nLabel.toString());
				isNeighborLabelContained = false;
				for(Integer gNeighbor:g.get(candidate)){
	    			//logger.info("FilterCandidate():: data v"+v.toString()+" neighbor v"+gNeighbor.toString());
    				if(L_g.get(gNeighbor).contains(nLabel)){
    	    			if(matchedVertex.contains(gNeighbor)){
    	    				continue;
    	    			}else{
	    	    			//logger.info("FilterCandidate():: query u"+neighbor.toString()+",data v"+gNeighbor.toString()+",label "+nLabel.toString());
	    					matchedVertex.add(gNeighbor);
	    					isNeighborLabelContained = true;
	    					break;
    		    		}
    				}
    			}
				if(!isNeighborLabelContained){
					break;
				}
				}
				
			
			if(!isNeighborLabelContained) continue;
    		
			//logger.info("PreVertex of u"+u[0].toString()+" is :: u"+u[1].toString()+",v"+preVertex.toString()+".Candidate:v"+candidate.toString());
			//logger.info("Degree of candidate:"+g_degree+", degree of query:"+q_degree);
			
			//if ((g_degree!=0)&&(q_degree!=0)&&(g_degree >= q_degree)) {
			boolean isAllNeighbor = true;
			if(preVertex.size() != 0){
				for(Integer parentG : preVertex){
					if(g.containsKey(parentG)){
						if(!g.get(parentG).contains(candidate)) {
							isAllNeighbor = false;
							break;
						}
					}
				}
			}else{
				isAllNeighbor = false;
			}
					
			if(isAllNeighbor){
				RefinedC_u.add(candidate);
				//logger.info("RefineCandidates():: add v"+preVertex.toString()+"'s neighbor candidate v"+candidate.toString()+" to C_R");
			}
					
/*					if(g.containsKey(preVertex)){
					if(g.get(preVertex).contains(candidate)){
						RefinedC_u.add(candidate);
						//logger.info("RefineCandidates():: add v"+preVertex.toString()+"'s neighbor candidate v"+candidate.toString()+" to C_R");
						//logger.info("add candidate: "+candidate.toString());
					}
					}
				}else{
					RefinedC_u.add(candidate);
					//logger.info("RefineCandidates():: add candidate v"+candidate.toString()+" to C_R");
				}
*/
			//				RefinedC_u.add(candidate);
			//}
		}
		
/*		logger.info("RefineCandidates():: candidate u"+u[0]+" C(R) size: "+RefinedC_u.size());
		
		String content = "RefineCandidates():: List candidates in C_R: ";
		for(Object candidate : RefinedC_u){
			content = content + "v"+candidate.toString()+"|";
		}
		logger.info(content);
*/		
		return RefinedC_u;
		
	}
	
	
	/**
     * Check whether the edges between u and already matched query vertices of q have corresponding
     * edges between v and already matched query vertices of g.
     *
     * @param g: LabeledGraph, for data graph to be queried.
     * @param q: LabeledGraph, for query graph.
     * @param M: List, a partial embedding, contains pairs of a query vertex and a corresponding data vertex.
     * @param F: match indication vector.
     * @param u: Object, query vertex.
     * @param v: Object, data graph vertex.
     *
     * @return a vertex u \in V(q) which is not yet matched..
     *
     */
	
	//@SuppressWarnings({ "rawtypes", "unchecked" })
	public boolean IsJoinable( //LabeledGraph g, LabeledGraph q, 
			Map<Integer,List<Integer>> g, 					//data graph(adjIndex)
			Map<Integer,Map<Integer,List<Integer>>> q, 		//query graph(queryGraph)
			Map<Integer,Set<Integer>> L_g, 					//V_g(dataLabelIndex)
			Map<Integer,List<Integer>>  L_q, 				//V_q(queryLabelIndex)
			List<Object[]> M, Object u, Object v ){
//	public boolean IsJoinable(LabeledGraph g, LabeledGraph q, List<Object[]> M, Object u, Object v ){
		
		boolean isJoinable = false;
		
		if(M.size()==0){
			return true;
		}else{
			boolean allMatched = true;
			try{
			for(Object[] matched:M){
				if(q.get(u).containsKey(matched[0])){
					allMatched = allMatched && g.get(v).contains(matched[1]) ;
				}
				if(!allMatched){
					break;
				}
			}
			
			allMatched = allMatched && IsLabelSetContained(L_g,L_q,u,v);
			
			}catch(Exception e){
				return false;
			}

			isJoinable = allMatched;
		}
		//System.out.println("IsJoinable()::"+isJoinable);
		return isJoinable;
		
	}
		
	/**
     * update Matched status information.
     *
     * @param M: List, a partial embedding, contains pairs of a query vertex and a corresponding data vertex.
     * @param u: Object, query vertex.
     * @param v: Object, data graph vertex.
     *
     * @return void.
     *
     */
	
	public void UpdateState(List<Object[]> M,  Map<Object,Set<Object>> F, Map<Object,Set<Object>> uF, Object u, Object v ){
		
		Object[] match = {u,v};
		M.add(match);
		if(F.containsKey(u)){
			F.get(u).add(v);
		}else{
			Set<Object> visited = new HashSet<Object>();
			visited.add(v);
			F.put(u, visited);
		}
		
		//update unusedCList. remove matched element from list.
		uF.get(u).remove(v);

/*		String log = "UpdateState():: Content in M: ";
		for(Object[] pair:M){
			log = log + "u"+pair[0].toString()+"[v"+pair[1].toString()+"] | ";
		}
		logger.info(log);
		log = "UpdateState():: keySet in F: ";
		for(Object key:F.keySet()){
			log = log + "u"+key.toString()+",size "+F.get(key).size()+" | ";
		}
		logger.info(log);
*/		
		logger.info("UpdateState():: u"+match[0].toString()+",v"+match[1].toString());
	}

	/**
     * restore Matched status information.
     *
     * @param M: List, a partial embedding, contains pairs of a query vertex and a corresponding data vertex.
     * @param F: match indication vector.
     * @param u: Object, query vertex.
     * @param v: Object, data graph vertex.
     *
     * @return void.
     *
     */
	
	public void RestoreState(List<Object[]> M, Map<Object,Set<Object>> F,Object[] u, Object v){
			
		
		int pos = 0;
		for(pos=0;pos<M.size();pos++){
			if(M.get(pos)[0].equals(u[0])){
				break;
			}
		}
		
		int size = M.size();
		for(int i=pos;i<size;i++){
			//logger.info("RestoreState():: remove u"+M.get(M.size()-1)[0].toString()+" from M. till reach u"+u[0].toString());
			M.remove(M.size()-1);
		}
		//M.remove(M.size()-1);
		
		//add unmatched vertex back to unusedCList.
		
			
			String log = "RestoreState():: Content in M: ";
			for(Object[] pair:M){
				log = log + "u"+pair[0].toString()+"[v"+pair[1].toString()+"] | ";
			}
			logger.info(log);
			log = "RestoreState():: keySet in F: ";
			for(Object key:F.keySet()){
				log = log + "u"+key.toString()+",size "+F.get(key).size()+" | ";
			}
			logger.info(log);
			logger.info("RestoreState():: remove u"+u[0].toString());

	}	
	
	public Object ClearState(List<Object[]> M, Map<Object,Set<Object>> F,Object[] u){

		int index =0;
		int size = M.size();
		
		//logger.info("ClearState():: current vertex is u"+u[0].toString()+"(v"+u[1].toString()+")");

		//find parent data vertex in stack M.
		for(Object[] matchedPairs:M){
			if(matchedPairs[0].equals(u[1])){
				break;
			}
			index++;
		}
		//logger.info("ClearState():: backtack to index "+index);
		
		
		//clear all visited entry in F, and remove entry in M.
		for(int i=index+1;i<size;i++){
					F.remove(M.get(i)[0]);
					//logger.info("ClearState()::remove u"+M.get(i)[0].toString()+" in F");
//				}
//			}
		}
//		if(index ==0){
			F.remove(u[0]);
			//logger.info("ClearState()::remove u"+u[0].toString()+" in F");
//		}
		
		for(int i=index+1;i<size;i++){
			//logger.info("ClearState()::remove u"+M.get(M.size()-1)[0].toString()+" in M");
			M.remove(M.size()-1);
		}
		
		//logger.info("ClearState()::size of F "+F.size()+",size of M"+M.size());
		//for(Object vertex:F.keySet()){
				//logger.info("ClearState():: content in F-u"+vertex.toString());
		//}
/*		String log = "ClearState():: Content in M: ";
		for(Object[] pair:M){
			log = log + "u"+pair[0].toString()+"[v"+pair[1].toString()+"] | ";
		}
		logger.info(log);
		log = "ClearState():: keySet in F: ";
		for(Object key:F.keySet()){
			log = log + "u"+key.toString()+",size "+F.get(key).size()+" | ";
		}
		logger.info(log);
		logger.info("ClearState():: remove u"+u[0].toString()+",backtack to u"+u[1].toString());
*/		
		return u[1];

	}
	
	/*
	 *  getBackableQueryVertex()
	 */
	
	public Object getBackableQueryVertex(
			Map<Integer,Map<Integer,List<Integer>>> q,
			List<Object[]> M, 
			Map<Object,Set<Object>> F,
			Map<Object,Set<Object>> uF,
			List<Integer[]> order,
			Object[] u){
		
		//List<Object> intermediate = new ArrayList<Object>();
		//logger.info("getBackableQueryVertex()::====================================================");
/*    	for(Object i:F.keySet()){
			String content = "getBackableQueryVertex():: List candidates in F(u"+i.toString()+"): ";
			for(Object candidate : F.get(i)){
				content = content + "v"+candidate.toString()+"|";
			}
			logger.info(content);
		}
		for(Object i:uF.keySet()){
			String content = "getBackableQueryVertex():: List candidates in uF("+i.toString()+"): ";
			for(Object candidate : uF.get(i)){
				content = content + "v"+candidate.toString()+"|";
			}
			logger.info(content);
		}

*/		
		//determine the backtrack range in order.
		//String loginfo = "getBackableQueryVertex():: Input in M: ";
		//for(Object[] pair:M){
		//	loginfo = loginfo + "u"+pair[0].toString()+"[v"+pair[1].toString()+"] | ";
		//}
		//logger.info(loginfo);
		

		int start = 0;
		int end = 0;
		Object parent = null;
		//logger.info("getBackableQueryVertex():: current query u"+u[0].toString());
		for(Object[] pair:order){
			if(pair[0].equals(u[0])){
				parent = pair[1];
				break;
			}
			end++;
		}
		
		//logger.info("getBackableQueryVertex():: current query u"+u[0].toString()+", parent is u"+parent.toString());
		for(Object[] pair:order){
			if(pair[0].equals(parent)) break;
			start++;
		}
		
		//System.out.println("Start "+start+",end "+end+",parent u"+parent.toString());
		int i = end;
		//Object rmVertex = null;
		//Set<Object> removeable = new HashSet<Object>();
		
/*		String ee = "getBackableQueryVertexe():: M content: ";
		for(Object[] mee: M){
			ee = ee + "u"+mee[0].toString()+"(v"+mee[1].toString()+")";
		}
		logger.info(ee);
*/		
		for(i=end;i>start;i--){
			Object cur = order.get(i)[0];
			if(!q.get(u[0]).containsKey(cur)){
				//rmVertex = cur;
				continue;
			}
			if(!uF.containsKey(cur)) continue;
			
			if(uF.get(cur).size()!=0){
				//logger.info("getBackableQueryVertex():: vertex u"+cur.toString()+" has unmatched candidates.");
				int pos = 0;
				for(Object[] pair:M){
					if(pair[0].equals(cur)){
						break;
					}
					pos++;
				}
				for(int j=pos+1;j<M.size();j++){
					Object backVertex = M.get(j)[0];
										
					//to avoid java.util.ConcurrentModificationException
					if(!backVertex.equals(cur)){
							F.remove(backVertex);
							uF.remove(backVertex);
							//logger.info("getBackableQueryVertex():: remove vertex u"+backVertex.toString()+" from F and uF. parent u"+parent.toString());
					}
					
					//logger.info("getBackableQueryVertex():: add intermediate vertex u"+backVertex.toString());
					//removeable.add(backVertex);
					//M.remove(j);
				}
				
				int msize = M.size();
				for(int j=pos+1;j<msize;j++){
					//logger.info("getBackableQueryVertex():: remove vertex u"+M.get(M.size()-1)[0].toString()+" from M");
					M.remove(M.size()-1);
				}
				
				parent = cur;
				break;
			}
		}
		
		//go to the parent vertex.
		if(i<start){
			int pos = 0;
			for(Object[] pair:M){
				if(pair[0].equals(parent)){
					break;
				}
				pos++;
			}
			//rmVertex = parent;
			for(int j=pos;j<M.size();j++){
				Object backVertex = M.get(j)[0];
				
				F.remove(backVertex);
				uF.remove(backVertex);
				//removeable.add(backVertex);
				//logger.info("getBackableQueryVertex():: remove parent vertex u"+backVertex.toString());
				//logger.info("getBackableQueryVertex():: remove vertex u"+backVertex.toString()+" from F and uF.");
				//M.remove(j);
			}
			
			int msize = M.size();
			for(int j=pos+1;j<msize;j++){
				//logger.info("getBackableQueryVertex():: remove vertex u"+M.get(M.size()-1)[0].toString()+" from M");
				M.remove(M.size()-1);
			}

		}
		
		//after picked out all removeable vertex in F and uF, check if none of the appears in the parent place in the order list.
/*		removeable.add(u[0]);
		logger.info("getBackableQueryVertex():: add start vertex u"+u[0].toString());
		int rmPos = 0;
		for(rmPos = 0;rmPos<order.size();rmPos++){
			if(order.get(rmPos)[0]==rmVertex) break;
		}
		for(Object o:removeable){
			boolean isRemoveable = true;
			for(int rmp=0;rmp<rmPos;rmp++){
				if(order.get(rmp)[1].equals(o)) {
					isRemoveable = false;
					break;
				}
			}
			if(isRemoveable){
				F.remove(o);
				uF.remove(o);
				logger.info("getBackableQueryVertex():: remove vertex u"+o.toString()+" from F and uF.");
			}
		}
		
*/		
		F.remove(u[0]);
		uF.remove(u[0]);
		
		logger.info("getBackableQueryVertex():: remove start vertex u"+u[0].toString());

		logger.info("getBackableQueryVertex():: get new parent u"+parent.toString());

		String log = "getBackableQueryVertex():: Content in M: ";
		for(Object[] pair:M){
			log = log + "u"+pair[0].toString()+"[v"+pair[1].toString()+"] | ";
		}
		logger.info(log);
		log = "getBackableQueryVertex():: keySet in F: ";
		for(Object key:F.keySet()){
			log = log + "u"+key.toString()+",size "+F.get(key).size()+" | ";
		}
		logger.info(log);
		log = "getBackableQueryVertex():: keySet in uF: ";
		for(Object key:uF.keySet()){
			log = log + "u"+key.toString()+",size "+uF.get(key).size()+" | ";
		}
		logger.info(log);
		logger.info("getBackableQueryVertexe():: remove u"+u[0].toString()+",backtack to u"+parent.toString());
		
		logger.info("getBackableQueryVertex()::====================================================");
		
		return parent;

	}
	
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
	
	/**
	 * @param args
	 */
	
	public List<Integer[]> getSearchOrder(
			Map<Integer,List<Integer>> g, 					//data graph(adjIndex)
			Map<Integer,Set<Integer>> L_g, 					//V_g(dataLabelIndex)
			Map<Integer,Map<Integer,List<Integer>>> q, 		//query graph(queryGraph)
			Map<Integer,List<Integer>>  L_q, 					//V_q(queryLabelIndex)
			Map<Integer,Set<Integer>> cR,					//Struct for reverse ordered query vertex.
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
				//System.out.println("sort vertx u"+vertex.toString()+",label:"+L_q.get(vertex).get(1).toString());
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
       
       //write too much extra code there.
        
       String sortlist = "getQueryOrder():: Query Vertex: ";
        for(QueryNode node:sortedVertex){
        	sortlist = sortlist + "u"+node.getId()+"["+node.getLabel()+"]:"+node.getFreq()+"|";
        }
        logger.info(sortlist);
       
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
            Map<Integer,Integer> allPathLength = new HashMap<Integer,Integer>();
            //sorted topk map
            /*SortedMap<Integer,Integer> sortedTopk = new TreeMap<Integer,Integer>(new Comparator<Map.Entry<Integer,Integer>>() {
            	@Override
                public int compare (Map.Entry<Integer,Integer> distOne, Map.Entry<Integer,Integer> distTwo) {
                    //use instanceof to verify the references are indeed of the type in question
                    //return (distOne.compareTo(distTwo));
                    int res = distOne.getValue().compareTo(d.getValue());
                    return res != 0 ? res : 1; // Special fix to preserve items with equal values
                }
            });*/
            Map<Integer,Integer> sortedTopk = new TreeMap<Integer,Integer>();
            
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
			Map<Integer,List<Integer>> pathSeq = null;
			
			//store shortest path length.
/*			List<QueryNode> pathLength = null;
			for(int j=0; j<k;j++){
				pathLength.add(new QueryNode(topkList.get(j).getId(),topkList.get(j).getLabel(),Integer.valueOf(0)));
			}
*/			
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
						//System.out.println(dsp.getPathLength());
						//long rounded = (myDouble == null)? 0L: Math.round(dsp.getPathLength());
						Integer pathLength = Integer.valueOf((int)Math.round(dsp.getPathLength()));
						//Integer pathLength = Integer.valueOf(Double.valueOf(dsp.getPathLength()).intValue());
						
						if (gp != null) {
							shortestPathList.put(topkList.get(j).getId(), gp);
							//Map.replace is only defined in JDK 1.8
							if(allPathLength.containsKey(topkList.get(j).getId())){
								allPathLength.remove(topkList.get(j).getId());
							}
							allPathLength.put(topkList.get(j).getId(), pathLength);
							
						}
							//graph.getPathVertexList(GraphPath<V,E> path) 
					}
				}
				
				//Check whether there exist containment among all shortest paths.
				List<Integer[]> entry = new ArrayList<Integer[]>();
				Map<Integer,List<Integer>> seq = new HashMap<Integer,List<Integer>>();

				for(GraphPath<String, DefaultWeightedEdge> cPath : shortestPathList.values()){
					List<String> pathVertexList = Graphs.getPathVertexList(cPath);
					List<Integer> pathList = new ArrayList<Integer>();
					Integer startInPath = Integer.valueOf(cPath.getStartVertex() );
					Integer endInPath = Integer.valueOf(cPath.getEndVertex());
					//logger.info("getQueryOrder():: path start vertex u"+startInPath.toString()+", end vertex u"+endInPath.toString());
					
/*					if(startInPath.equals(rootVertex)){
						seq.put(endInPath, pathList);
					}
					if(endInPath.equals(rootVertex)){
						seq.put(startInPath, pathList);
					}
*/					
					for(int pos = 0;pos < pathVertexList.size()-1; pos++){
						Integer nodeOne = Integer.valueOf(pathVertexList.get(pos));
						Integer nodeTwo = Integer.valueOf(pathVertexList.get(pos+1));
						
						if(!pathList.contains(nodeOne)) pathList.add(nodeOne);
						if(!pathList.contains(nodeTwo)) pathList.add(nodeTwo);
						//logger.info("getQueryOrder():: get path vertex u"+nodeOne.toString()+", end vertex u"+nodeTwo.toString());
						
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
					
					seq.put(endInPath, pathList);
					
		        }
				

/*				String log2 = "getQueryOrder():: temp Seq list: |";
		        for(Integer key:seq.keySet()){
		        	log2 = log2 + "u"+key.toString()+"[";
		        	for(Integer cc:seq.get(key)){
		        		log2 = log2 + "u"+cc.toString()+",";
		        	}
		        	log2 = log2 + "] | ";
		        }
		        logger.info(log2);
*/		        
		        double treeWeight = 0.0d;
				for(Integer[] nodePair:entry){
					//System.out.println("===Entry: u"+nodePair[0].toString()+"-u"+nodePair[1].toString());
					treeWeight = treeWeight + q.get(nodePair[0]).get(nodePair[1]).get(2).doubleValue();
				}
				
/*		        String log = "getQueryOrder():: root vertex is u"+sortedVertex.get(i).getId().toString()+", Edge list: |";
		        for(Integer[] pairs:entry){
		        	log = log + "u"+pairs[0].toString()+"-u"+pairs[1].toString()+"|";
		        }
		        logger.info(log);
*/
		        if(treeWeight<minWeight){
		        	minWeight = treeWeight;
		        	keyStructure = entry;
		        	rootVertex = sortedVertex.get(i).getId();
		        	for(Integer key:allPathLength.keySet()){
		        		sortedTopk.put(key, allPathLength.get(key));
		        	}
		        	pathSeq = seq;
			    }
				
			}//for
			
			//sort descending order, farest vertex first.
			sortedTopk = sortByValue(sortedTopk);
			
	        String log = "getQueryOrder():: root vertex is u"+rootVertex.toString()+", key Structure: |";
	        for(Integer[] pairs:keyStructure){
	        	log = log + "u"+pairs[0].toString()+"-u"+pairs[1].toString()+"|";
	        }
	        logger.info(log);
	        
/*	        log = "getQueryOrder():: sorted top-K vertex list: |";
	        for(Integer key:sortedTopk.keySet()){
	        	log = log + "u"+key.toString()+"("+sortedTopk.get(key).toString()+")|";
	        }
	        logger.info(log);
	        
	        log = "getQueryOrder():: pathSeq list: |";
	        for(Integer key:pathSeq.keySet()){
	        	log = log + "u"+key.toString()+"[";
	        	for(Integer cc:pathSeq.get(key)){
	        		log = log + "u"+cc.toString()+",";
	        	}
	        	log = log + "] | ";
	        }
	        logger.info(log);
*/	        
	        /* ****************************************
	         *
	         * Arrange search order for top-k vertices.
	         * 2014-12-16 revised for new strategy.
	         * 
	         * ****************************************/
	        //top-k path length.
	        List<Integer> topkPL = new ArrayList<Integer>();
	        for(Integer topku:sortedTopk.keySet()){
	        	topkPL.add(topku);
	        }
	        
	        //top-k label frequency.
	        List<Integer> topkLF = new ArrayList<Integer>();
	        for(QueryNode topku:topkList){
	        	topkLF.add(topku.getId());
	        }
	        
	        //check path containment.
	        //Map<Integer,List<Integer>> inPath = getPathContainment(rootVertex,keyStructure,topkLF);
	        
	        
	        for(Integer orderNode : topkLF){
	        	
/*		        String newlog = "orderNode u"+orderNode.toString()+", Content in topkPL: ";
	        	for(Integer newnode:topkPL){
	        		newlog = newlog + "u"+newnode.toString()+" | ";
	        	}
	        	logger.info(newlog);

	        	newlog = "Content in topkLF: ";
	        	for(Integer newnode:topkLF){
	        		newlog = newlog + "u"+newnode.toString()+" | ";
	        	}
	        	logger.info(newlog);

	        	if(topkPL.size() == 0) break;
	        	
*/	        	//if(order.size()!=1){
	        	//	if(!topkPL.contains(orderNode)) break;
	        	//}
	        	
	        	boolean hasAssigned = false;
	        	
		        for(Integer[] pair:order){
		        	if(pair[0].equals(orderNode)){
		        			hasAssigned = true;
		        			break;
		        		}
		        	}
		        	if(hasAssigned){
		        		//logger.info("CONTINUE!!!");
		        		continue;
		        	}
	        	
        		Integer nextNode = null;
	        	//if orderNode is contained in the path from rootVertex to other top-k node.
	        	if(pathSeq.containsKey(orderNode)){
	        		//get the less frequent node in topkLF
	        		for(Integer nn:topkLF){
        				hasAssigned = false;
	        			if(pathSeq.get(orderNode).contains(nn)){
	        				nextNode = nn;
	        	        	//nextNode is not in order list.
	        				hasAssigned = false;
	        	        	for(Integer[] pair:order){
	        	        		if(pair[0].equals(nn)){
	        	        			hasAssigned = true;
	        	        			break;
	        	        		}
	        	        	}
	        	        	if(!hasAssigned) break;
	        			}
	        		}
	        		if(nextNode == null) nextNode = rootVertex;
	        	}else{
	        		nextNode = rootVertex;
	        	}
	        	if(hasAssigned){
	        		//logger.info("CONTINUE!!!");
	        		continue;
	        	}

	        	//logger.info("Get nextNode u"+nextNode.toString());
	        	
				hasAssigned = false;
	        	for(Integer[] pair:order){
	        		if(pair[0].equals(nextNode)){
	        			hasAssigned = true;
	        			break;
	        		}
	        	}
	        	
	        	if(hasAssigned){
	        	if(pathSeq.containsKey(orderNode)){
	        		if(pathSeq.get(orderNode).size()>2){
	        			boolean hasQueryNode = false;
	        			for(int i=pathSeq.get(orderNode).size();i>1;i--){
	        				Integer mNode = pathSeq.get(orderNode).get(i-2);
	        				for(Integer[] pair: order){
	        					if(pair[0].equals(mNode)){
	        						nextNode = mNode;
	        						hasQueryNode = true;
	        						break;
	        					}
	        				}
	        				if(hasQueryNode) break;
	        			}
	        		}else{
	        			nextNode = rootVertex;
	        		}
	        	}
	        	}
	        	
		        if(order.size()==0){
		        	Integer[] entry1 = {topkLF.get(0),null};
		        	order.add(entry1);
		        	topkPL.remove(topkLF.get(0));
		        }else{
		        	Integer[] entry1 = {orderNode,rootVertex};
		        	order.add(entry1);
		        	topkPL.remove(orderNode);
		        }
		        
	        	//logger.info("Get nextNode u"+nextNode.toString());
	        	
	        	//If not to rootVertex, first to find path from orderNode to nextNode in keyStructure.
        		//List<Integer> seq = new ArrayList<Integer>();
	        	if(!nextNode.equals(rootVertex)){
	        		//logger.info("go to query node u"+nextNode.toString());
	        		List<Integer> cSeq = new ArrayList<Integer>();
	        		//get vertices that is in the path of nextNode->rootVertex,but not in the path of orderNode->rootVertex.
	        		for(Integer cu:pathSeq.get(nextNode)){
	        			if(!pathSeq.get(orderNode).contains(cu)){
	        				cSeq.add(cu);
	        			}
	        		}
		        
	        		Integer intermediate = orderNode;
			        while(!intermediate.equals(nextNode)){
			        	for(Integer mNode:cSeq){
			        		//add intermediate vertex to order list.
							Integer[] newEntry = {mNode,intermediate};
			        		order.add(newEntry);
			        		intermediate = mNode;
			        		
			        		if(topkPL.contains(intermediate)){
								topkPL.remove(intermediate);
							}
								
				        }
			        }//while
	        	}// if not rootVertex.
	        	
	        	//go to rootVertex.
        		Integer intermediate = orderNode;
		        while(!intermediate.equals(rootVertex)){
		        	for(int i=pathSeq.get(orderNode).size();i>1;i--){
		        		//add intermediate vertex to order list.
						Integer[] newEntry = {pathSeq.get(orderNode).get(i-2),intermediate};
		        		order.add(newEntry);
		        		intermediate = pathSeq.get(orderNode).get(i-2);
		        		
		        		if(topkPL.contains(intermediate)){
							topkPL.remove(intermediate);
						}
							
			        }
		        }//while
	        	
	        }//for
      
        	int occurance = 0;
//	        Set<Integer> assignedNeighbors = new HashSet<Integer>();
        	
        	//remove duplicated root vertex in order list.
	        Iterator<Integer[]> iter = order.iterator();
    		while(iter.hasNext()){
    			Integer vertex = iter.next()[0];
    			if(vertex.equals(rootVertex)){
    				occurance++;
    			}
	        	if(occurance>1){
					iter.remove();
	        	}
    		}
    		//2014-12-16 finish arrange top-k vertices and root in order list.

    		//order.remove(order.size()-1);
	        
/*	        log = "temporary order: ";
	        for(Integer[] u:order){
	        	if(u[1] != null){
	        		log = log + "u"+u[0].toString()+"[u"+u[1].toString()+"] -> ";
	        	}else{
	        		log = log + "u"+u[0].toString()+"[null] -> ";
	        	}
	        }
	        logger.info(log);
*/        
	        //after get the order of skeleton structure, arrange order of rest node.
	        //get the lowest-ranked rested vertices, check the traversable vertex.
	        
    		
	        while(order.size() < q.size()){
	    		//construct neighbors set for vertex in order.
	        	Set<Integer> assignedNeighbors = new HashSet<Integer>();
		        for(Integer[] p:order){
			        for(Integer neigb:q.get(p[0]).keySet()){
			        	boolean inOrder = false;
	    				for(Integer pair[]:order){
		    				//logger.info("order u"+p[0].toString()+",neigbhor u"+neigb.toString()+",to match u"+pair[0].toString());
	    					if(neigb.equals(pair[0])){
	    						inOrder = true;
	    						//logger.info("Matched: u"+neigb.toString()+"- u"+pair[0].toString());
	    						break;
	    					}
	    				}
	    	    			
	    				if(!inOrder){
	    					assignedNeighbors.add(neigb);
	    	    			//logger.info("add neighbor u"+neigb.toString()+" to set.");
	    				}
	    			}  
		        }

		        //check sorted vertex for vertex that is not yet put in order.
		        for(QueryNode n:sortedVertex){
	
	    			if(isQueryNodeInNeighbors(n.getId(),assignedNeighbors) && (!isQueryNodeAssigned(n.getId(),order))){
		        		//System.out.println("Vertex u"+n.getId().toString()+" has been assigned to order list.");
	 	        		Integer[] entry = new Integer[2];
	    				entry[1] = getParent(n.getId(),order,q);
	    				entry[0] = n.getId();
		        		order.add(entry);
		        		
		        		queryDFS(n.getId(),order,q);
		        		//logger.info("Out");
		        		break;
		        	
		        	}
		        }
	        }
	        
	        order.get(0)[1]=rootVertex;

	        //log the query order.
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
        
        //2014-12-19 Code to deal with reverse order.
		//for reverse ordered query node.
		List<Integer> sortedList = new ArrayList<Integer>();
		Map<Integer,Set<Integer>> roCR = new HashMap<Integer,Set<Integer>>();
		
	    for(QueryNode n:sortedVertex){
	    	sortedList.add(n.getId());
	    }
	    
	    //@TEST
	    String logu = "GET_REVERSE_CANDIDATE:: Content in sortedList: ";
	    for(Integer u:sortedList){
	    	logu = logu + u.toString() + "|";
	    }
	    logger.info(logu);
	    
	    
	    //check if reverse order exist.
	    //Integer traversalOne = order.get(0)[0];
	    //int i = 0;
	    for(Integer[] pair:order){
	    	//if(i==0) continue;
	    	//Integer traversalTwo = pair[0];
	    	
	    	int posOne = 0;
	    	for(Integer n:sortedList){
	    		if(n.equals(pair[0])) break;
	    		posOne++;
	    	}
	    	
			Set<Integer> roSet = new HashSet<Integer>();
	    	for(Integer neighborU:q.get(pair[0]).keySet()){

				int posTwo = 0;
		    	for(Integer n:sortedList){
		    		if(n.equals(neighborU)) break;
		    		posTwo++;
		    	}
		    	logger.info("GET_REVERSE_CANDIDATE:: prev u"+pair[0].toString()+"(pos_"+posOne+") | next u"+neighborU.toString()+"(pos_"+posTwo+")");
		    	
		    	if(posOne > posTwo){
		    		//if(L_q.get(neighborU).get(1)<100)
		    		//if(L_q.get(pair[0]).get(1)/L_q.get(neighborU).get(1) > 10)
			    	logger.info("GET_REVERSE_CANDIDATE:: prev u"+pair[0].toString()+"(labelfrq_"+L_q.get(pair[0]).get(2).toString() +") | next u"+neighborU.toString()+"(labelfreq_"+L_q.get(neighborU).get(2).toString()+")");
		    		if((float)(L_q.get(pair[0]).get(2)/L_q.get(neighborU).get(2)) >= THRESHOLD){
		    			List<Object> allV = FilterCandidate(g,L_g,q,L_q,neighborU);
	    				
		    			for(Object cV:allV){
		    				//cV:current parent vertx in G.
		    				//result added to cR.
		    				for(Object cVN:g.get(cV)){
		    					if(IsLabelSetContained(L_g,L_q ,pair[0], cVN )){
		    						roSet.add((Integer)cVN);
		    				    	//logger.info("GET_REVERSE_CANDIDATE:: add u"+cVN.toString()+" to Map entry u"+pair[0].toString());
		    					}
		    				}//for child
		    			}//for parent(all candidates)
		    			
		    			//roCR.put(neighborU,roSet);
		    		}//if THRESHOLD
		    	}
		    	
 	    	}
	    	
	    	//if(roCR.size()!=0){
	    	//@ERROR If check containsKey first, Map will create this key.
	    	//if((!roCR.keySet().contains(pair[0])) && (roSet.size()!=0) ){
	    	if((roSet.size()!=0) && (!roCR.containsKey(pair[0])) ){
	    		cR.put(pair[0],roSet);
	    	  	logger.info("GET_REVERSE_CANDIDATE:: MAP entry u"+pair[0].toString()+", size "+ cR.get(pair[0]).size());
	    	}
	    	
	    	//traversalOne = traversalTwo;
	    	//i++;
	    }

        //log the query order.
        logger.info("getQueryOrder():: To display reverse candidate map");
	    for(Integer u:cR.keySet()){
            String log = "getQueryOrder():: Reverse candidate map: u"+ u.toString() +":";
         	for( Integer c:cR.get(u)){
        		log = log + " u"+c.toString()+"|";
        	}
            logger.info(log);
       }
 
        return order;
	
	}
		
	
	//Obsoleted.
	public Map<Integer,List<Integer>> getPathContainment(
			Integer rootVertex,
			List<Integer[]> keyStructure,
			List<Integer> topk
			){
		
		Map<Integer,List<Integer>> inPath = new HashMap<Integer,List<Integer>>();
		
		for(Integer u:topk){
			List<Integer> list = new ArrayList<Integer>();
			
			Set<Integer> contains = new HashSet<Integer>();
			Integer toVertex = u;
			while(!toVertex.equals(rootVertex)){
				for(Integer[] pair:keyStructure){
					if(pair[1].equals(toVertex)){
						toVertex = pair[0];
						contains.add(toVertex);
						break;
					}
					if(pair[0].equals(toVertex)){
						toVertex = pair[1];
						contains.add(toVertex);
						break;
					}
				}
			}//while
			
			for(Integer k:topk){
				if(contains.contains(k)){
					if(!inPath.containsKey(k)){
						inPath.put(k, list);
					}
					inPath.get(k).add(u);
				}
			}//for
			
		}
		
		//combine duplicate containments in inPath.
		boolean hasCombinable = true;
		while(hasCombinable){
			hasCombinable = false;
			for(Integer u:inPath.keySet()){
				for(Integer vv:inPath.get(u)){
					if(inPath.containsKey(vv)){
						for(Integer v:inPath.keySet()){
							for(Integer l:inPath.get(v)){
								if(inPath.get(u).contains(l)){
									inPath.get(u).remove(v);
									hasCombinable = true;
								}
							}
						}
					}
				}
			}
		}
		
		//@TEST print results.
/*		String log = "getPathContainment():: Content in inPath: ";
		for(Integer u:inPath.keySet()){
			log = log + "u" + u.toString()+ "[";
			for(Integer list:inPath.get(u)){
				log = log + "u" + list.toString() +"|";
			}
			log = log + "]; " ;
		}
		logger.info(log);
*/		
		return inPath;
		
	}
	
/*	public List<Integer> getPathInKeyStructure(
			Integer sVertex,
			Integer eVertex,
			List<Integer[]> keyStructure,
			Map<Integer,List<Integer>> pathSeq
			){
		
		List<Integer> seq = new ArrayList<Integer>();
		
		
		return seq;
	}
		
*/	public <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
		List<Map.Entry<K, V>> list = new LinkedList<>(map.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
			@Override
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
				//return (-(o1.getValue()).compareTo(o2.getValue()));
				return (o1.getValue()).compareTo(o2.getValue());
			}
		});

		Map<K, V> result = new LinkedHashMap<>();
		for (Map.Entry<K, V> entry : list) {
			result.put(entry.getKey(), entry.getValue());
		}
		return result;
	}

	public boolean isQueryNodeInNeighbors(Integer vertex, Set<Integer> assignedNeighbors){
		boolean isInNeighbor = false;
		
		for(Integer assigned:assignedNeighbors){
			if(assigned.equals(vertex)){
				isInNeighbor = true;
				break;
			}
		}
		
		return isInNeighbor;
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
				//logger.info("Add order: u"+neighbor.toString()+"[u"+entry[1].toString()+"]");

/*				String log = "getQueryOrder():: order: ";
		        for(Integer[] u:order){
		        	if(u[1] != null){
		        		log = log + "u"+u[0].toString()+"[u"+u[1].toString()+"] -> ";
		        	}else{
		        		log = log + "u"+u[0].toString()+"[null] -> ";
		        	}
		        }
		        logger.info(log);
*/
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
						//int[] newEdge = { sourceNode.intValue(),destNode.intValue() };

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
							labelSet.add(Integer.valueOf(89));
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
						//logger.info(code);
						queryLabelIndex.add(vertexIndexEntry);
						vertexIndexEntry = new HashMap<Integer,List<Integer>>();
						queryGraphEntry  = new HashMap<Integer,Map<Integer,List<Integer>>>();
					}
/*					logger.info("Query "+blockId+" is add to queryGraph.");

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
*/
					break;

				default:
					break;
				}//case
				str = br.readLine();
			}//while
			
			//@TEST
			for(int i=0;i<queryGraph.size();i++){
			for (Integer sourceNode : queryGraph.get(i).keySet()) {
				for (Integer destNode : queryGraph.get(i).get(sourceNode).keySet()) {
					String log = "Query node (" + sourceNode.toString() + "," + destNode.toString() + "), ";
					List<Integer> contentList = queryGraph.get(i).get(sourceNode).get(destNode);
					int frequecy = contentList.get(2).intValue();
					log = log + "Freq:" + frequecy +", Label: "+contentList.get(0)+"--"+contentList.get(1)+".";
					//System.out.println(log);
					//Key
					logger.info(log);
				}
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

	
	
	/**
	 * @param args
	 */
	public static void main(String args[]) {

		//2014-11-11 Gai Lei: added LabeledGraph class and implemented several functions;
		//					  Tested with instance graph and query.
		//		  			  Example from SPath paper.
		//Record Start time of each query.

		long startTime = System.currentTimeMillis();

		loadDataGraph();
		
		loadQueryGraph();
		
		long endTime = System.currentTimeMillis();
		long queryTime = endTime - startTime;

		String s = "Total index running time:"+queryTime;
		//logger.info(s);
		logger.info(s);

		OrderedBacktrackVerFour ug = new OrderedBacktrackVerFour();
		
		logger.info("Total querys: "+queryGraph.size());
		for(int i=0;i<queryGraph.size();i++){
/*			Map<Integer, Map<Integer, List<Integer>>> skeletonQueryGraph =  ug.getSkeletonQueryGraph(queryGraph.get(i));
			
			Map<Integer,List<Integer>> skeletonQueryLabelIndex = new HashMap<Integer,List<Integer>>();
			for(Integer v:queryGraph.get(i).keySet()){
				if(skeletonQueryGraph.containsKey(v)){
					skeletonQueryLabelIndex.put(v,queryLabelIndex.get(i).get(v));
					//logger.info("Construct queryLabel, add vertex u"+v.toString()+",size "+queryGraph.get(i).size()+","+skeletonQueryGraph.get(i).size());
				}
			}
			
			List<List<Integer>> parallelEdges = ug.getParallelEdges(queryGraph.get(i), skeletonQueryGraph);
			
			logger.info("Query "+i+" Skeleton Graph size "+skeletonQueryGraph.size());
			ug.GenericQueryProc(adjIndex,dataLabelIndex,skeletonQueryGraph,skeletonQueryLabelIndex);
*/			
			ug.GenericQueryProc(adjIndex,dataLabelIndex,queryGraph.get(i),queryLabelIndex.get(i));
			logger.info("Go to Query "+(i+1));
			
		}

		//End time
		long endTime2 = System.currentTimeMillis();
		queryTime = endTime2 - endTime;

		s = "Total query running time:"+queryTime;
		logger.info(s);
		//logger.info(s);
	
	
	
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
