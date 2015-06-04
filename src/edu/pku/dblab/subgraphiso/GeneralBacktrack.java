package edu.pku.dblab.subgraphiso;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.graph.DefaultEdge;

import edu.pku.dblab.graph.LabeledGraph;

public class GeneralBacktrack {

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
	@SuppressWarnings("rawtypes")
	public void GenericQueryProc(LabeledGraph g, LabeledGraph q, int k) {
		
		//M: a partial embedding, contains pairs of a query vertex and a corresponding data vertex.
		//data structure: pairs as array. the first element is query vertex, the second element is corresponding data vertex.
		//if |M|=|v(q)|, the algorithm finds a complete solution.
		List<Object[]> M = new ArrayList<Object[]>();
		
		//C(u): a set of candidate vertices.
		List<Object> C_u = new ArrayList<Object>();
	    @SuppressWarnings("unchecked")
		
	    //Get the size of query graph's vertex set.
	    Set<Integer> V_q = q.vertexSet();
		@SuppressWarnings("unchecked")
		Set<Object> V_g = g.vertexSet();
		//Vector F denote which vertices of g have been used at an intermediate state of the match computation.
		//The data structure of F can be optimized to only use one bit for an ordered list of vertices. 
		Map<Object,Boolean> F = new HashMap<Object,Boolean>();
		for(Object e:V_g){
			F.put(e,Boolean.valueOf(false));
		}
		
		int minsize = 0;
		Object first_vertex = null;
		for(Integer u:V_q){
			C_u = (ArrayList<Object>) FilterCandidate(g,q,u);
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
		
		C_u = (ArrayList<Object>) FilterCandidate(g,q,first_vertex);
		for(int i=0;i<minsize;i++){
			System.out.println("First Query Vertex: "+C_u.get(i));
			M = new ArrayList<Object[]>();
			Object[] first_matched_pair={first_vertex,C_u.get(i)};
			M.add(first_matched_pair);
			SubgraphSearch(g,q,M,F,1);
			//F.put(C_u.get(i),Boolean.valueOf(true));
			for(Object e:V_g){
				F.put(e,Boolean.valueOf(false));
			}
		}
		
		//Print testing result.
		System.out.println("#################################################################");
		int total_matched = 0 ;
		for(String result:R){
			System.out.println(result);
			total_matched ++;
		}
		System.out.println("Total matched subgraphs::"+total_matched);
		System.out.println("Total recursive counts::"+count);
		System.out.println("################################################################");
		
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
	@SuppressWarnings("rawtypes")
	public void SubgraphSearch(LabeledGraph g, LabeledGraph q, List<Object[]> M, Map<Object,Boolean> F, int d) {
	
		@SuppressWarnings("unchecked")
		Set<Object> V_q = q.vertexSet();
		
		//@TEST: define total recursive call times.		
		count++;
		if(count==10000) return;
		
		if(M.size()<V_q.size() ){
			//not yet matched query vertex.
			Object u = NextQueryVertex(q,M);
			System.out.println("NextQueryVertex():: get vertex q"+u.toString());
			//get refined candidate list C(R) of vertex u. 
			List<Object> C_R = RefineCandidates(g,q,u,F);
			//@TEST: print all elements in Candidate set.
			for(Object e:C_R){
				System.out.print("C(R)"+e.toString()+" ");
			}
			System.out.println("==Graph C(R) size: "+C_R.size());
			
			for(Object v:C_R){
				//check whether vertex v is not yet matched.
				boolean isMatched = false;
				for(Object[] pairs:M){
					if(pairs[1].equals(v)){
						isMatched = true;
						break;
					}
				}
				System.out.println("SubgraphSearch()::Graph Vertex g"+v.toString()+" isMatched in M: "+isMatched);
				//vertex v \in C(R) and is not yet matched.
				if((!isMatched) && IsJoinable(g,q,M,F,u,v)){
					
					System.out.println("SubgraphSearch()::TODO: q"+u+",g"+v);
					
					UpdateState(M,u,v);
					for(Object[] matched:M){
						System.out.println("SubgraphSearch()::Matched pairs: q"+matched[0].toString()+",g"+matched[1].toString());
					}
					SubgraphSearch(g,q,M,F,d+1);
					RestoreState(M,F,u,v);
				}
			}
			
			System.out.println("TEST:size: M="+M.size()+",V(q)="+V_q.size()+",C(R)="+C_R.size());
		}else{
			String result = "Matched Result::";
			//System.out.print("Matched Result::");
			for(Object[] pairs:M){
				result = result + "V"+pairs[1].toString()+"(U"+pairs[0].toString()+"),";
			}
			System.out.println(result);
			R.add(result);
		}
		
		return;
		
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
	@SuppressWarnings("rawtypes")
	public boolean IsLabelSetContained(LabeledGraph g, LabeledGraph q, Object u, Object v) {
		
	    @SuppressWarnings("unchecked")
		List<Object> L_v = g.getVertexLabel(v);
	    @SuppressWarnings("unchecked")
		List<Object> L_u = q.getVertexLabel(u);
 	    List<Boolean> label_match = new ArrayList<Boolean>();

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
	@SuppressWarnings("rawtypes")
	public List<Object> FilterCandidate(LabeledGraph g, LabeledGraph q, Object u) {
		List<Object> candidate=new ArrayList<Object>();

		//For backtrack algorithm, filtering is only graph label matching.
	    @SuppressWarnings("unchecked")
		Set<Object> V_g = g.vertexSet();
	    
	    for(Object v:V_g){
	    	if(IsLabelSetContained(g,q,u,v)){
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

	@SuppressWarnings("rawtypes")
	public List<Object> FilterCandidate(LabeledGraph g, LabeledGraph q, Object u,Map<Object,Boolean> F) {
		List<Object> candidate=new ArrayList<Object>();

		//For Ullmann algorithm, filtering is only graph label matching.
	    @SuppressWarnings("unchecked")
		Set<Object> V_g = g.vertexSet();
	    
	    for(Object v:V_g){
	    	//System.out.println("FilterCandidate()::vertex:"+v+",F value:"+(F.get(v)).booleanValue());
	    	//if value in F is false(not been matched)
	    	if(!(F.get(v)).booleanValue()){
		    	if(IsLabelSetContained(g,q,u,v)){
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
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Object NextQueryVertex(LabeledGraph q, List<Object[]> M){
		
		Object selectedVertex =null;
				
		Set<Object> V_q = q.vertexSet();
//		for(Object[] pairs: M){
//			System.out.print("u:"+pairs[0]+"-v:"+pairs[1]+",");
//		}
//		System.out.println("..");
//		for(Object u : V_q) {
//			System.out.print("u:"+u.toString()+",");
//		}
//		System.out.println("..");
//		
		for (Object u : V_q) {
			boolean flag = false;
			for (Object[] pairs : M) {
				if (pairs[0].equals(u)) {
					flag = true;
					break;
				}
			}
			// get the unmatched query vertex.
			if (flag == false) {
				if (M.size() == 0) {
					System.out.println("NextQueryVertex()::M init:u"+u.toString());
					return u;
				} else {
					for(Object[] current:M){
					if (q.containsEdge(current[0], u)) {
							System.out.println("NextQueryVertex()::M current:u"+current[0]+", neighbour:u"+u.toString());
							return u;
						}
					}
				}

			}
		}
		
		return selectedVertex;

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
	
	@SuppressWarnings("rawtypes")
	public List<Object> RefineCandidates(LabeledGraph g, LabeledGraph q, Object u,Map<Object,Boolean> F){

		List<Object> RefinedC_u = new ArrayList<Object>();
		List<Object> C_u = FilterCandidate(g, q, u, F);

		for (Object candidate : C_u) {
			// A Matrix of degree for g should be precomputed, instead of using
			// degreeOf function.
			@SuppressWarnings("unchecked")
			int g_degree = g.degreeOf(candidate);
			@SuppressWarnings("unchecked")
			int q_degree = q.degreeOf(u);
			System.out.println("RefineCandidates()::g" + candidate.toString());
			if (g_degree >= q_degree) {
				RefinedC_u.add(candidate);
			}
		}
		
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
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public boolean IsJoinable(LabeledGraph g, LabeledGraph q, List<Object[]> M,  Map<Object,Boolean> F, Object u, Object v ){
		
		boolean isJoinable = false;
		
		if(M.size()==0){
			return true;
		}else{
			boolean allMatched = true;
			for(Object[] matched:M){
				if(q.containsEdge(u,matched[0])){
					allMatched = allMatched && g.containsEdge(v, matched[1]);
					System.out.println("IsJoinable():: Matched:g"+matched[1].toString()+",q"+matched[0].toString()
							+" Candidate:g"+v.toString()+",q"+u.toString()+" "+allMatched);
				}
				if(!allMatched){
					break;
				}
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
	
	public void UpdateState(List<Object[]> M,  Object u, Object v ){
		
		Object[] match = {u,v};
		M.add(match);
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
	
	public void RestoreState(List<Object[]> M, Map<Object,Boolean> F,Object u,Object v){
		
		int index =0;
		for(Object[] matchedPairs:M){
			if(matchedPairs[0].equals(u)){
				break;
			}
			index++;
		}

		//delete all elements added after u in M
		for(int i=index;i<M.size();i++){
			M.remove(index);
		}
		//if u is starting vertex, mark corresponding v in F as TRUE(visited).
		if(index==0){
			F.put(v, Boolean.valueOf(true));
		}

	}	
	
	/**
	 * @param args
	 */
	public static void main(String args[]) {

		//2014-11-11 Gai Lei: added LabeledGraph class and implemented several functions;
		//					  Tested with instance graph and query.
		//		  			  Example from SPath paper.
		LabeledGraph<Integer, DefaultEdge> graph = new LabeledGraph<Integer, DefaultEdge>(
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

		GeneralBacktrack ug = new GeneralBacktrack();
		ug.GenericQueryProc(graph,query,100);
	
	
	}

}
