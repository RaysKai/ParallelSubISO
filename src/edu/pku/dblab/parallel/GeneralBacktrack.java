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
//import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.Collections;
import java.util.Comparator;


//import org.jgrapht.alg.DijkstraShortestPath;
//import org.jgrapht.graph.DefaultWeightedEdge;
//import org.jgrapht.graph.SimpleDirectedWeightedGraph;
import org.jgrapht.alg.*;
import org.jgrapht.*;
import org.jgrapht.graph.*;
import org.apache.log4j.Logger;
//import org.jgrapht.graph.DefaultEdge;

public class GeneralBacktrack {

	//private static final String IGRAPH_FILE = "D:\\data\\example-full.igraph";
	//private static final String QUERY_FILE = "D:\\data\\example-query.igraph";
	private static final String IGRAPH_FILE = "D:\\data\\human.igraph";
	private static final String QUERY_FILE = "D:\\data\\query-human-q10.igraph";
	//private static final String QUERY_FILE = "F:\\iGraph\\iGraph20\\querysets\\human\\gs\\human_q10.igraph";
	//private static final String QUERY_FILE = "F:\\iGraph\\iGraph20\\querysets\\human\\cliques\\human_clique7.igraph";
	private static final Logger logger = Logger.getLogger(GeneralBacktrack.class);

	private static Map<Integer,Map<Integer,Set<int[]>>> inmemIndex = new HashMap<Integer,Map<Integer,Set<int[]>>>();
	private static Map<Integer,Map<Integer,Integer>> freqPattern = new HashMap<Integer,Map<Integer,Integer>>();
	private static Map<Integer,List<Integer>> adjIndex = new HashMap<Integer,List<Integer>>();
	private static Map<Integer,Set<Integer>> adjLabelIndex = new HashMap<Integer,Set<Integer>>();
	private static Map<Integer,Set<Integer>> dataLabelIndex = new HashMap<Integer, Set<Integer>>();
	private static Map<Integer,Integer> LabelFrequency = new HashMap<Integer, Integer>();
	
	//private static Map<Integer,Map<Integer,List<Set<Integer>>>> queryGraph = new HashMap<Integer,Map<Integer,List<Set<Integer>>>>();
	//private static Map<Integer,Map<Integer,List<Integer>>> queryGraph = new HashMap<Integer,Map<Integer,List<Integer>>>();
	//private static Map<Integer,Set<Integer>> queryLabelIndex = new HashMap<Integer,Set<Integer>>();
	
	//in Key: nodeID. Content in ArrayList: (0)degree, (1)label (3)frequency.
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
	   // @SuppressWarnings("unchecked")
		
	    //Get the size of query graph's vertex set.
	    //Set<Integer> V_q = q.vertexSet();
	    
	    
		//Set<Object> V_g = g.vertexSet();
		//Vector F denote which vertices of g have been used at an intermediate state of the match computation.
		//The data structure of F can be optimized to only use one bit for an ordered list of vertices. 
		Map<Object, Set<Object>> F = new HashMap<Object,Set<Object>>();
//		Map<Object,Boolean> F = new HashMap<Object,Boolean>();
//		for(Object e:V_g){
//			F.put(e,Boolean.valueOf(false));
//		}
		
/*		int minsize = 0;
		Object first_vertex = null;
		//for(Integer u:V_q){
		for(Integer u:q.keySet()){
			C_u = (ArrayList<Object>)FilterCandidate(L_g,L_q,u);
			//No match for one query node.
			if(C_u.size()==0){
				return;
			}else{
				if((C_u.size() < minsize)|| (minsize == 0 ) ){
					first_vertex = u;
					minsize=C_u.size();
				}
			}
		}//End of query node matching. (For)
		
		C_u = (ArrayList<Object>) FilterCandidate(L_g,L_q,first_vertex);
*/	
		
		//Comment all below after implementation of getSearchOrder()
		int min = Integer.MAX_VALUE;
		Integer s = null;
		Integer d = null;
		for(Integer source: q.keySet() ){
			for(Integer destination: q.get(source).keySet()){
				Integer freq=q.get(source).get(destination).get(2);
				if(freq.intValue() < min){
						min = freq.intValue();
						s = source;
						d = destination;
				}
			}
		}
		
		Integer startVertex = null;
		if(q.get(s).size() > q.get(d).size()){
			startVertex = s;
		}else{
			startVertex = d;
		}
		
		C_u = (ArrayList<Object>) FilterCandidate(L_g,L_q,startVertex);
		Set<Object> visited = new HashSet<Object>();
		
		//2014-12-14 get search order.
		//List<Integer[]> order = getSearchOrder(g, L_g, q,L_q, 3 );
		
		//logger.info("First query node: u"+order.get(0)[0].toString());

		C_u = (ArrayList<Object>) FilterCandidate(L_g,L_q,startVertex);
		
		for(Object c:C_u){
			System.out.print("|"+c.toString());
		}
		//Set<Object> visited = new HashSet<Object>();

		
		for(int i=0;i<C_u.size();i++){
			//System.out.println("First Query Vertex: "+C_u.get(i));
			M = new ArrayList<Object[]>();
			//F = new HashSet<Object>();
			
			//get search order do the job.
			//Object[] first_matched_pair={startVertex,C_u.get(i)};
			Object[] first_matched_pair={startVertex,C_u.get(i)};
			
			M.add(first_matched_pair);
			visited.add(C_u.get(i));
			if(i==0){
				//F.put(startVertex, visited);
				F.put(startVertex, visited);
			}
			//F.add(C_u.get(i));
			SubgraphSearch(g,L_g,q,L_q,M,F);
			//F.put(C_u.get(i),Boolean.valueOf(true));
//			for(Object e:V_g){
//				F.put(e,Boolean.valueOf(false));
//			}
		}
		
		//Print testing result.
		//logger.info("#################################################################");
		int total_matched = 0 ;
		for(String result:R){
			logger.info(result);
			total_matched ++;
		}
		logger.info("Total matched subgraphs::"+total_matched);

		logger.info("Total recursive counts::"+count);
		//logger.info("################################################################");
		
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
			Map<Object,Set<Object>> F
			) {
//	public void SubgraphSearch(LabeledGraph g, LabeledGraph q, List<Object[]> M, Set<Object> F) {
	
		//@SuppressWarnings("unchecked")
		//Set<Integer> V_q = q.keySet();
		
		//@TEST: define total recursive call times.		
		Object parentVertex = null;
		count++;
		//if(count>=10000) return null;
		if(R.size()>=1000) return null;
		
		//Have been backtracked to the first node.
		if(M.size()==0) return null;
		
		if(M.size()<q.keySet().size() ){
			//not yet matched query vertex.
			Object[] u = NextQueryVertex(q,L_q,M);
			if(u == null) return null;
			//logger.info("NextQueryVertex():: get vertex q"+u[0].toString()+", prev vertex q"+u[1].toString());
			
			//get refined candidate list C(R) of vertex u. 
			Set<Object> C_R = RefineCandidates(g,L_g,q,L_q,M,u,F);
			//@TEST: print all elements in Candidate set.
//			for(Object e:C_R){
//				System.out.print("C(R)"+e.toString()+" ");
//			}
			//logger.info("SubgraphSearch():: candidate u"+u[0]+" C(R) size: "+C_R.size());
			
			if(C_R.size()==0){
				parentVertex = ClearState(M,F,u);
				return parentVertex;
			}
			
			for(Object v:C_R){
				//check whether vertex v is not yet matched.
				boolean isMatched = false;
/*				for(Object[] pairs:M){
					if(pairs[1].equals(v)){
						isMatched = true;
						break;
					}
				}
*/				
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
					
						//System.out.println("SubgraphSearch(): q"+u+",g"+v);
						
						//UpdateState(M,F,u,v);
				    	//System.out.print("contents in V before UpdateState");
	//			    	for(Object vv:F){
	//			    		System.out.print("::"+vv.toString());
	//			    	}
	//			    	System.out.println(".");
	
	//			    	for(Object[] matched:M){
	//						System.out.println("SubgraphSearch()::Matched pairs: q"+matched[0].toString()+",g"+matched[1].toString());
	//					}
						
						//Key debug output.
/*						String matched = "Matched in M: ";
						for(Object[] pairs:M){
							matched = matched + "u"+pairs[0].toString()+"-v"+pairs[1].toString()+"| ";
						}
						matched = matched + "To be added::u"+u[0].toString()+"-v"+v.toString()+"| [Matched: "+R.size()+ "]";
						logger.info(matched);
*/						
						UpdateState(M,F,u[0],v);
						Object parent = SubgraphSearch(g,L_g,q,L_q,M,F);
						if(parent == null) return null;
						if(parent.equals(u[0])){
							RestoreState(M,F,u,v);
						}else{
							return parent;
						}
	//			    	System.out.print("contents in V after RestoreState");
	//			    	for(Object vv:F){
	//			    		System.out.print("::"+vv.toString());
	//			    	}
	//			    	System.out.println(".");

					}//isJoinable()
				}

			}//looped all candidate in C_R.
			
			parentVertex = ClearState(M,F,u);
			
			
			//System.out.println("TEST:size: M="+M.size()+",V(q)="+V_q.size()+",C(R)="+C_R.size());
		}else{
			//logger.info("M Size::"+M.size());
			
			String result = "Matched Result::";
			for(Object[] pairs:M){
				result = result + "V"+pairs[1].toString()+"(U"+pairs[0].toString()+"),";
			}
			R.add(result);
			//logger.info(result+", count:"+R.size());
			
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
//	public boolean IsLabelSetContained(LabeledGraph g, LabeledGraph q, Object u, Object v) {
		
	    //@SuppressWarnings("unchecked")
		//List<Object> L_v = g.getVertexLabel(v);
	    //@SuppressWarnings("unchecked")
		//List<Object> L_u = q.getVertexLabel(u);
	    Set<Integer> L_v = L_g.get(v);
	    //Set<Integer> L_u = new HashSet<Integer>();
	    
//	    for(int i=1;i<L_q.get(u).size();i++){
//	    		L_u.add(L_q.get(u).get(i));
//	    }
	    
	    Integer L_u = L_q.get(u).get(1);
	    
/* 	    List<Boolean> label_match = new ArrayList<Boolean>();

 	    //L(u) \in L(v)
	    	for(Object l_u:L_u){
	    		for(Object l_v:L_v){
	    			if (l_v.toString().equals(l_u.toString())){
	    				label_match.add(new Boolean(true));
	    				continue;
	    			}
	    		}
	    	}
	    	if(label_match.size()==L_u.size()){
	    		return true;
	    	}else{
	    		return false;
	    	}
*/	
    
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
		Map<Integer,Set<Integer>> L_g, 					//V_g(dataLabelIndex)
		Map<Integer,List<Integer>>  L_q, 					//V_q(queryLabelIndex)
		Object u ){
//		public List<Object> FilterCandidate(LabeledGraph g, LabeledGraph q, Object u) {
		
		List<Object> candidate=new ArrayList<Object>();

		//For backtrack algorithm, filtering is only graph label matching.
	    //@SuppressWarnings("unchecked")
		//Set<Object> V_g = g.vertexSet();
		Set<Integer> V_g = L_g.keySet(); 
	    
	    for(Object v:V_g){
	    	if(IsLabelSetContained(L_g,L_q,u,v)){
	    		candidate.add(v);
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
//	public List<Object> FilterCandidate(LabeledGraph g, LabeledGraph q, Object u,Set<Object> F) {
	//public List<Object> FilterCandidate(LabeledGraph g, LabeledGraph q, Object u,Map<Object,Boolean> F) {
		List<Object> candidate=new ArrayList<Object>();

		//For Ullmann algorithm, filtering is only graph label matching.
	    //@SuppressWarnings("unchecked")
		//Set<Object> V_g = g.vertexSet();
		Set<Integer> V_g = L_g.keySet();
	    
	    for(Object v:V_g){
	    	//System.out.println("FilterCandidate()::vertex:"+v+",F value:"+(F.get(v)).booleanValue());
	    	//if value in F is false(not been matched)
//	    	if(!(F.get(v)).booleanValue()){
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

	public Object[] NextQueryVertex(
		Map<Integer,Map<Integer,List<Integer>>> q, 		//query graph(queryGraph)
		Map<Integer,List<Integer>>  L_q, 				//V_q(queryLabelIndex)
		List<Object[]> M ){
	
		List<Object[]> candidate = new ArrayList<Object[]>();
		Object[] v = null;

		//BFS based next query vertex.
		try{
			if(M.size()==0){
				return null;
			}else{
				for(int i=1;i<M.size()+1;i++){
					Object u = M.get(M.size()-i)[0];
	
					for(Integer nextVertex:q.get(u).keySet()){
						boolean contains = false;
						//nextVertex is already in M.
						for (Object[] pairs : M) {
							if (pairs[0].equals(nextVertex)) {
								contains = true;
								break;
							}
						}
						if(contains==false){
							Object[] next ={nextVertex,u};
							candidate.add(next);
						}
					}
					//all neighbors of current query vertex u have been matched.
					//go to previous node
				}
			}
			
			float minRank = Float.MAX_VALUE;
			for(Object[] pairs:candidate){
				float rank = L_q.get((Integer)pairs[0]).get(2).intValue() / L_q.get((Integer)pairs[0]).get(0).intValue();
				if (rank < minRank){
					minRank = rank;
					v = pairs;
				}
			}
			
		}catch(Exception e){
			return null;
		}
		return v;
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
			Map<Object,Set<Object>> F){

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
			
			
			//check neighbor label containment. whether current vertex contains all neighbors's label set in q
			boolean isNeighborLabelContained = true;
			//get label set of vertex in g.
			Set<Integer> gLabelSet = adjLabelIndex.get(candidate);
			//get all neighbor of query vertex in q.
			for(Integer neighbor:q.get(u[0]).keySet()){
				Integer nLabel = L_q.get(neighbor).get(1);
				if(!gLabelSet.contains(nLabel)){
					isNeighborLabelContained = false;
					break;
				}
			}
			
			if(!isNeighborLabelContained) continue;
			
			Integer preVertex = null;
			for(Object[] pairs: M){
				if(pairs[0].equals(u[1])){
					preVertex = (Integer)pairs[1];
				}
			}
			
			//logger.info("PreVertex of u"+u[0].toString()+" is :: u"+u[1].toString()+",v"+preVertex.toString()+".Candidate:v"+candidate.toString());
			//logger.info("Degree of candidate:"+g_degree+", degree of query:"+q_degree);
			
			//if ((g_degree!=0)&&(q_degree!=0)&&(g_degree >= q_degree)) {
				if(preVertex != null){
					if(g.containsKey(preVertex)){
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
//				RefinedC_u.add(candidate);
			//}
		}
		
		//logger.info("RefineCandidates():: candidate u"+u[0]+" C(R) size: "+RefinedC_u.size());
		
		//String content = "RefineCandidates():: List candidates in C_R: ";
		//for(Object candidate : RefinedC_u){
		//	content = content + "v"+candidate.toString()+"|";
		//}
		//logger.info(content);
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
	
	public void UpdateState(List<Object[]> M,  Map<Object,Set<Object>> F, Object u, Object v ){
		
		Object[] match = {u,v};
		M.add(match);
		if(F.containsKey(u)){
			F.get(u).add(v);
		}else{
			Set<Object> visited = new HashSet<Object>();
			visited.add(v);
			F.put(u, visited);
		}
		
		//logger.info("UpdateState():: u"+match[0].toString()+",v"+match[1].toString());
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
	//public void RestoreState(List<Object[]> M, Map<Object,Boolean> F,Object u,Object v){
		
/*		int index =0;
		for(Object[] matchedPairs:M){
			if(matchedPairs[0].equals(u[0])){
				break;
			}
			index++;
		}

		//delete all elements added after u in M
		for(int i=index;i<M.size();i++){
			M.remove(index);
			//F.remove(v);
		}
*/		
		
/*		Object prevMatchedQueryVertex = M.get(M.size()-1)[0];
		Object parentOfPrevVertex = null;
		//Object currentMatchedQueryVertex = u[0];
		//Object parentOfCurrentVertex = u[1];
		for(Object vertex:F.keySet()){
			if(F.get(vertex).contains(prevMatchedQueryVertex)){
				parentOfPrevVertex = vertex;
				break;
			}
		}
*/		
		//if(M.size()!=0){
			M.remove(M.size()-1);
		//}
		
/*	
		if(F.get(u[0]).size()>0){
			F.get(u[0]).remove(v);
		}else{
			F.remove(u[0]);
		}
*/		
		//if u is starting vertex, mark corresponding v in F as TRUE(visited).
/*		if(index==0){
			//F.put(v, Boolean.valueOf(true));
			//F = new HashSet<Object>();
			F.add(v);
		}
*/		
		//logger.info("RestoreState():: remove u"+u[0].toString());

	}	
	
	public Object ClearState(List<Object[]> M, Map<Object,Set<Object>> F,Object[] u){

		int index =0;
		int size = M.size();
		
		//find parent vertex in stack M.
		for(Object[] matchedPairs:M){
			if(matchedPairs[0].equals(u[1])){
				break;
			}
			index++;
		}
		//logger.info("ClearState():: backtack to index "+index);
		
		//clear all visited entry in F, and remove entry in M.
		for(int i=index+1;i<size;i++){
//			for(Object vertex:F.keySet()){
//				if(vertex.equals(M.get(i)[0])){
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
		//logger.info("ClearState():: remove u"+u[0].toString()+",backtack to u"+u[1].toString());
		return u[1];

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
	
/*	public Map<Integer,Map<Integer,List<Integer>>> getSkeletonQueryGraph(
			Map<Integer,Map<Integer,List<Integer>>> queryG
			){
		
		Map<Integer,Map<Integer,List<Integer>>> SkeletonQueryGraph = new HashMap<Integer,Map<Integer,List<Integer>>>();

		int minFreq = Integer.MAX_VALUE;
		Integer s1 = null;
		Integer s2 = null;
		
		for(Integer source:queryG.keySet()){
			for(Integer dest:queryG.get(source).keySet()){
				//logger.info("Edge u"+source.toString()+"-u"+dest.toString()+",freq "+queryG.get(source).get(dest).get(2).intValue());
				if(queryG.get(source).size()>0 && queryG.get(dest).size()>0) {
					if(queryG.get(source).get(dest).get(2).intValue() < minFreq  ){
						minFreq = queryG.get(source).get(dest).get(2).intValue();
						s1 = source;
						s2 = dest;
					}
				}
			}
		}
		
		//logger.info("First Edge is u"+s1.toString()+"-u"+s2.toString()+",freq "+minFreq);
		
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
		Map<Integer,List<Integer>> entry1 = new HashMap<Integer,List<Integer>>();
		Map<Integer,List<Integer>> entry2 = new HashMap<Integer,List<Integer>>();
		List<Integer> pattern1 = new ArrayList<Integer>();
		List<Integer> pattern2 = new ArrayList<Integer>();
		//logger.info("getSkeletonQueryGraph(): s1: "+s1.toString()+", S2: "+s2.toString());
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
						//logger.info("===Add Entry: u"+m.toString()+"-u"+c.toString()+",freq "+queryG.get(m).get(c).get(2).intValue());
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
			
			Map<Integer, List<Integer>> entry = new HashMap<Integer, List<Integer>>();
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
			//logger.info("Add Entry: u"+newVertex[0].toString()+"->u"+newVertex[1].toString()+",Freq "+newVertex[2].toString());
			
		}
		
//		for(Integer ss:SkeletonQueryGraph.keySet()){
//			for(Integer d:SkeletonQueryGraph.get(ss).keySet()){
//				logger.info("SKeleton Edge u"+ss.toString()+"-u"+d.toString()+", Label "+ SkeletonQueryGraph.get(ss).get(d).get(0).intValue()
//						+ "-" + SkeletonQueryGraph.get(ss).get(d).get(1).intValue() + " ,freq "+SkeletonQueryGraph.get(ss).get(d).get(2).intValue());
//			}
//		}
	
		return SkeletonQueryGraph;
	}*/
	
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
					//logger.info(log);
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

	
/*	public static void loadQueryGraph(){
		
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
					//LabeledVertex v = new LabeledVertex();
					//v.setId(nodeID.intValue());
					if(labelSet != null) {
						//v.setLabels(labelSet);
						//vertexIndex.put(nodeID, labelSet);
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
							Map<Integer, List<Integer>> adjEntry = new HashMap<Integer, List<Integer>>();
							adjEntry.put(sourceNode, contentList);
							queryGraph.put(destNode,adjEntry);
						}
					}else{
						Map<Integer, List<Integer>> adjSourceEntry = new HashMap<Integer, List<Integer>>();
						adjSourceEntry.put(destNode, contentList);
						queryGraph.put(sourceNode,adjSourceEntry);
						
						if(queryGraph.containsKey(destNode)){
							queryGraph.get(destNode).put(sourceNode,contentList);
						}else{
							Map<Integer, List<Integer>> adjDestEntry = new HashMap<Integer, List<Integer>>();
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
					//System.out.println(log);
					//logger.info(log);
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
*/
	
	/**
	 * @param args
	 */
	public static void main(String args[]) {

		//2014-11-11 Gai Lei: added LabeledGraph class and implemented several functions;
		//					  Tested with instance graph and query.
		//		  			  Example from SPath paper.
/*		LabeledGraph<Integer, DefaultEdge> graph = new LabeledGraph<Integer, DefaultEdge>(
				DefaultEdge.class);
		graph.addVertex(Integer.valueOf(1));
		graph.addVertexLabel(Integer.valueOf(1), "A");
		graph.addVertexLabel(Integer.valueOf(1), "B");
		graph.addVertexLabel(Integer.valueOf(1), "C");
		graph.addVertexLabel(Integer.valueOf(1), "D");
		graph.addVertex(Integer.valueOf(2));
		graph.addVertexLabel(Integer.valueOf(2), "A");
		graph.addVertexLabel(Integer.valueOf(2), "B");
		graph.addVertexLabel(Integer.valueOf(2), "C");
		graph.addVertexLabel(Integer.valueOf(2), "D");
		graph.addVertex(Integer.valueOf(3));
		graph.addVertexLabel(Integer.valueOf(3), "A");
		graph.addVertexLabel(Integer.valueOf(3), "B");
		graph.addVertexLabel(Integer.valueOf(3), "C");
		graph.addVertexLabel(Integer.valueOf(3), "D");
		graph.addVertex(Integer.valueOf(4));
		graph.addVertexLabel(Integer.valueOf(4), "A");
		graph.addVertexLabel(Integer.valueOf(4), "B");
		graph.addVertexLabel(Integer.valueOf(4), "C");
		graph.addVertexLabel(Integer.valueOf(4), "D");
		graph.addVertex(Integer.valueOf(5));
		graph.addVertexLabel(Integer.valueOf(5), "A");
		graph.addVertexLabel(Integer.valueOf(5), "B");
		graph.addVertexLabel(Integer.valueOf(5), "C");
		graph.addVertexLabel(Integer.valueOf(5), "D");
		graph.addVertex(Integer.valueOf(6));
		graph.addVertexLabel(Integer.valueOf(6), "A");
		graph.addVertexLabel(Integer.valueOf(6), "B");
		graph.addVertexLabel(Integer.valueOf(6), "C");
		graph.addVertexLabel(Integer.valueOf(6), "D");
		graph.addVertex(Integer.valueOf(7));
		graph.addVertexLabel(Integer.valueOf(7), "A");
		graph.addVertexLabel(Integer.valueOf(7), "B");
		graph.addVertexLabel(Integer.valueOf(7), "C");
		graph.addVertexLabel(Integer.valueOf(7), "D");
		graph.addVertex(Integer.valueOf(8));
		graph.addVertexLabel(Integer.valueOf(8), "A");
		graph.addVertexLabel(Integer.valueOf(8), "B");
		graph.addVertexLabel(Integer.valueOf(8), "C");
		graph.addVertexLabel(Integer.valueOf(8), "D");
		graph.addVertex(Integer.valueOf(9));
		graph.addVertexLabel(Integer.valueOf(9), "A");
		graph.addVertexLabel(Integer.valueOf(9), "B");
		graph.addVertexLabel(Integer.valueOf(9), "C");
		graph.addVertexLabel(Integer.valueOf(9), "D");
		graph.addVertex(Integer.valueOf(10));
		graph.addVertexLabel(Integer.valueOf(10), "A");
		graph.addVertexLabel(Integer.valueOf(10), "B");
		graph.addVertexLabel(Integer.valueOf(10), "C");
		graph.addVertexLabel(Integer.valueOf(10), "D");
		graph.addVertex(Integer.valueOf(11));
		graph.addVertexLabel(Integer.valueOf(11), "A");
		graph.addVertexLabel(Integer.valueOf(11), "B");
		graph.addVertexLabel(Integer.valueOf(11), "C");
		graph.addVertexLabel(Integer.valueOf(11), "D");
		graph.addVertex(Integer.valueOf(12));
		graph.addVertexLabel(Integer.valueOf(12), "A");
		graph.addVertexLabel(Integer.valueOf(12), "B");
		graph.addVertexLabel(Integer.valueOf(12), "C");
		graph.addVertexLabel(Integer.valueOf(12), "D");
		graph.addEdge(Integer.valueOf(1), Integer.valueOf(2));
		graph.addEdge(Integer.valueOf(1), Integer.valueOf(3));
		graph.addEdge(Integer.valueOf(1), Integer.valueOf(4));
		graph.addEdge(Integer.valueOf(1), Integer.valueOf(5));
		graph.addEdge(Integer.valueOf(2), Integer.valueOf(5));
		graph.addEdge(Integer.valueOf(2), Integer.valueOf(6));
		graph.addEdge(Integer.valueOf(2), Integer.valueOf(7));
		graph.addEdge(Integer.valueOf(3), Integer.valueOf(8));
		graph.addEdge(Integer.valueOf(4), Integer.valueOf(8));
		graph.addEdge(Integer.valueOf(4), Integer.valueOf(9));
		graph.addEdge(Integer.valueOf(5), Integer.valueOf(6));
		graph.addEdge(Integer.valueOf(5), Integer.valueOf(9));
		graph.addEdge(Integer.valueOf(5), Integer.valueOf(10));
		graph.addEdge(Integer.valueOf(6), Integer.valueOf(7));
		graph.addEdge(Integer.valueOf(7), Integer.valueOf(10));
		graph.addEdge(Integer.valueOf(7), Integer.valueOf(12));
		graph.addEdge(Integer.valueOf(8), Integer.valueOf(9));
		graph.addEdge(Integer.valueOf(9), Integer.valueOf(10));
		graph.addEdge(Integer.valueOf(10), Integer.valueOf(11));
		graph.addEdge(Integer.valueOf(10), Integer.valueOf(12));
		graph.addEdge(Integer.valueOf(11), Integer.valueOf(12));
		graph.addEdge(Integer.valueOf(11), Integer.valueOf(8));
		graph.addEdge(Integer.valueOf(11), Integer.valueOf(9));

		graph.addEdge(Integer.valueOf(6), Integer.valueOf(10));
		
		//Query graph
		LabeledGraph<Integer, DefaultEdge> query = new LabeledGraph<Integer, DefaultEdge>(
				DefaultEdge.class);
		query.addVertex(Integer.valueOf(1));
		query.addVertexLabel(Integer.valueOf(1), "A");
		query.addVertex(Integer.valueOf(2));
		query.addVertexLabel(Integer.valueOf(2), "B");
		query.addVertex(Integer.valueOf(3));
		query.addVertexLabel(Integer.valueOf(3), "B");
		query.addVertex(Integer.valueOf(4));
		query.addVertexLabel(Integer.valueOf(4), "C");
		query.addVertex(Integer.valueOf(5));
		query.addVertexLabel(Integer.valueOf(5), "D");
		query.addVertex(Integer.valueOf(6));
		query.addVertexLabel(Integer.valueOf(6), "A");
		query.addVertex(Integer.valueOf(7));
		query.addVertexLabel(Integer.valueOf(7), "C");

		query.addEdge(Integer.valueOf(1), Integer.valueOf(2));
		query.addEdge(Integer.valueOf(1), Integer.valueOf(3));
		query.addEdge(Integer.valueOf(1), Integer.valueOf(4));
		query.addEdge(Integer.valueOf(2), Integer.valueOf(4));
		query.addEdge(Integer.valueOf(2), Integer.valueOf(5));
		query.addEdge(Integer.valueOf(4), Integer.valueOf(5));
		query.addEdge(Integer.valueOf(4), Integer.valueOf(6));
		query.addEdge(Integer.valueOf(3), Integer.valueOf(6));
		query.addEdge(Integer.valueOf(4), Integer.valueOf(7));
*/
		//Record Start time of each query.

		long startTime = System.currentTimeMillis();

		loadDataGraph();
		
		loadQueryGraph();
		
		long endTime = System.currentTimeMillis();
		long queryTime = endTime - startTime;

		String s = "Total index running time:"+queryTime;
		//logger.info(s);
		logger.info(s);

		GeneralBacktrack ug = new GeneralBacktrack();
		
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
			logger.info("Go to Query "+i);
			
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
		
		QueryNode(Integer id,Integer Label,Integer freq){
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
