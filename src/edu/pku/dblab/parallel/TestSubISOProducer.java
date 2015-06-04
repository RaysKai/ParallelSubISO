package edu.pku.dblab.parallel;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Set;
//import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.ConcurrentHashMap;

public class TestSubISOProducer  implements Runnable  {

	private Map<Integer, List<Integer>> g;
	private Map<Integer, Set<Integer>> L_g;
	private Map<Integer, Map<Integer, List<Integer>>> q;
	private Map<Integer, Set<Integer>> L_q;
//	private List<Object[]> M; 
//	private Map<Object, Set<Object>> F;
	private Object u;
	private Object v;
	private BlockingQueue<Object> buffer;
	
	public TestSubISOProducer(Map<Integer, List<Integer>> g, // data graph(adjIndex)
			Map<Integer, Set<Integer>> L_g, // V_g(dataLabelIndex)
			Map<Integer, Map<Integer, List<Integer>>> q, // query graph(queryGraph)
			Map<Integer, Set<Integer>> L_q, // V_q(queryLabelIndex)
			Object u, Object v,
			BlockingQueue<Object> buffer ) {
        this.g = g;
        this.L_g = L_g ;
        this.q = q;
        this.L_q = L_q;
 //       this.M = M;
 //       this.F = F;
        this.u = u;
        this.v =v; 
        this.buffer = buffer;
    }

    @Override
    public void run() {
    	
		long startTime = System.currentTimeMillis();
		
		List<Object[]> M = new ArrayList<Object[]>(); 
    	Map<Object, Set<Object>> F = new TreeMap<Object, Set<Object>>();
    	
		Set<Object> visited = new HashSet<Object>();

		Object[] first_matched_pair = { u ,v };
		M.add(first_matched_pair);
		visited.add(v);
		F.put(u, visited);

    	SubgraphSearch(g,L_g,q,L_q,M,F);

        //End time
 		long endTime = System.currentTimeMillis();
 		long queryTime = endTime - startTime;

 		System.out.println("Thread cost: "+queryTime);
    }   

    /**
	 * Main recursive function for subgraph isomorphism search.
	 * 
	 * @param g
	 *            : LabeledGraph, for data graph to be queried.
	 * @param q
	 *            : LabeledGraph, for query graph.
	 * @param M
	 *            : List, a partial embedding, contains pairs of a query vertex
	 *            and a corresponding data vertex.
	 * @param F
	 *            : Map, a vector for indication of matched vertex in g. entry
	 *            is vertex, value is boolean object.
	 * @param d
	 *            : int, top-k matched.
	 * 
	 * @return void.
	 * 
	 */
	// @SuppressWarnings("rawtypes")
	// public void SubgraphSearch(LabeledGraph g, LabeledGraph q, List<Object[]>
	// M, Map<Object,Boolean> F, int d) {
	public Object SubgraphSearch(Map<Integer, List<Integer>> g, // data graph(adjIndex)
			Map<Integer, Set<Integer>> L_g, // V_g(dataLabelIndex)
			Map<Integer, Map<Integer, List<Integer>>> q, // query graph(queryGraph)
			Map<Integer, Set<Integer>> L_q, // V_q(queryLabelIndex)
			List<Object[]> M, Map<Object, Set<Object>> F) {

		Object parentVertex = null;

		// if(count==10000) return null;
		if (buffer.size() == 1000)
			return null;

		// Have been backtracked to the first node.
		if (M.size() == 0)
			return null;

		if (M.size() < q.keySet().size()) {
			// not yet matched query vertex.
			Object[] u = NextQueryVertex(q, L_q, M);
			if (u == null)
				return null;
			// logger.info("NextQueryVertex():: get vertex q"+u[0].toString()+", prev vertex q"+u[1].toString());

			// get refined candidate list C(R) of vertex u.
			List<Object> C_R = RefineCandidates(g, L_g, q, L_q, M, u, F);
			// @TEST: print all elements in Candidate set.
			// for(Object e:C_R){
			// System.out.print("C(R)"+e.toString()+" ");
			// }
			// logger.info("SubgraphSearch():: candidate u"+u[0]+" C(R) size: "+C_R.size());

			if (C_R.size() == 0) {
				parentVertex = ClearState(M, F, u);
				return parentVertex;
			}

			for (Object v : C_R) {
				// check whether vertex v is not yet matched.
				boolean isMatched = false;
				/*
				 * for(Object[] pairs:M){ if(pairs[1].equals(v)){ isMatched =
				 * true; break; } }
				 */
				for (Object vertex : F.get(u[0])) {
					if (vertex.equals(v)) {
						isMatched = true;
						break;
					}
				}

				// logger.info("SubgraphSearch()::u"+u[0].toString()+"-v"+v.toString()+" isMatched in M: "+isMatched+". Parent u"+u[1].toString());
				// vertex v \in C(R) and is not yet matched.
				if (!isMatched) {
					F.get(u[0]).add(v);
					if (IsJoinable(g, q, M, u[0], v)) {

						// System.out.println("SubgraphSearch(): q"+u+",g"+v);

						// UpdateState(M,F,u,v);
						// System.out.print("contents in V before UpdateState");
						// for(Object vv:F){
						// System.out.print("::"+vv.toString());
						// }
						// System.out.println(".");

						// for(Object[] matched:M){
						// System.out.println("SubgraphSearch()::Matched pairs: q"+matched[0].toString()+",g"+matched[1].toString());
						// }
						UpdateState(M, F, u[0], v);
						Object parent = SubgraphSearch(g, L_g, q, L_q, M, F);
						if (parent == null)
							return null;
						if (parent.equals(u[0])) {
							RestoreState(M, F, u, v);
						} else {
							return parent;
						}
						// System.out.print("contents in V after RestoreState");
						// for(Object vv:F){
						// System.out.print("::"+vv.toString());
						// }
						// System.out.println(".");

					}// isJoinable()
				}

			}// looped all candidate in C_R.

			parentVertex = ClearState(M, F, u);

			// System.out.println("TEST:size: M="+M.size()+",V(q)="+V_q.size()+",C(R)="+C_R.size());
		} else {
			// logger.info("M Size::"+M.size());
			String result = "Matched Result::";
			// System.out.print("Matched Result::");
			for (Object[] pairs : M) {
				result = result + "V" + pairs[1].toString() + "(U"
						+ pairs[0].toString() + "),";
			}
			buffer.add(result);
			System.out.println(result+", count:"+ buffer.size());

			return M.get(M.size() - 1)[0];
		}

		return parentVertex;

		// return candidate;
	}

	/**
	 * Check whether the label set of vertex u is contained in the label set of
	 * vertex v.
	 * 
	 * @param g
	 *            : LabeledGraph, for data graph to be queried.
	 * @param q
	 *            : LabeledGraph, for query graph.
	 * @param u
	 *            : Object, vertex in query to be matched.
	 * @param v
	 *            : Object, vertex in data graph.
	 * 
	 * @return boolean: true is L(u) \in L(v).
	 * 
	 */
	// @SuppressWarnings("rawtypes")
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

	/**
	 * Filtering candidates, based on g and q.
	 * 
	 * @param g
	 *            : LabeledGraph, for data graph to be queried.
	 * @param q
	 *            : LabeledGraph, for query graph.
	 * @param u
	 *            : Object, vertex in query to be matched.
	 * 
	 * @return List<Object>: all matched vertex in graph for a given query
	 *         vertex.
	 * 
	 */
	// @SuppressWarnings("rawtypes")
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

	/**
	 * Filtering candidates, based on g and q, considering matched vector F.
	 * 
	 * @param g
	 *            : LabeledGraph, for data graph to be queried.
	 * @param q
	 *            : LabeledGraph, for query graph.
	 * @param F
	 *            : match indication vector.
	 * @param u
	 *            : Object, vertex in query to be matched.
	 * 
	 * @return List<Object>: all matched vertex in graph for a given query
	 *         vertex.
	 * 
	 */

	// @SuppressWarnings("rawtypes")
	public List<Object> FilterCandidate(Map<Integer, Set<Integer>> L_g, // V_g(dataLabelIndex)
			Map<Integer, Set<Integer>> L_q, // V_q(queryLabelIndex)
			Object u, Map<Object, Set<Object>> F, List<Object[]> M) {
		// public List<Object> FilterCandidate(LabeledGraph g, LabeledGraph q,
		// Object u,Set<Object> F) {
		// public List<Object> FilterCandidate(LabeledGraph g, LabeledGraph q,
		// Object u,Map<Object,Boolean> F) {
		List<Object> candidate = new ArrayList<Object>();

		// For Ullmann algorithm, filtering is only graph label matching.
		// @SuppressWarnings("unchecked")
		// Set<Object> V_g = g.vertexSet();
		Set<Integer> V_g = L_g.keySet();

		for (Object v : V_g) {
			// System.out.println("FilterCandidate()::vertex:"+v+",F value:"+(F.get(v)).booleanValue());
			// if value in F is false(not been matched)
			// if(!(F.get(v)).booleanValue()){
			if (!(F.get(u).contains(v))) {
				boolean containedinM = false;
				for (Object[] pairs : M) {
					if (pairs[1].equals(v)) {
						containedinM = true;
						break;
					}
				}
				if ((!containedinM) && IsLabelSetContained(L_g, L_q, u, v)) {
					candidate.add(v);
				}
			}
		}
		return candidate;
	}

	/**
	 * Select a query vertex u \in V(q) which is not yet matched.
	 * 
	 * @param q
	 *            : LabeledGraph, for query graph.
	 * @param M
	 *            : List, a partial embedding, contains pairs of a query vertex
	 *            and a corresponding data vertex.
	 * 
	 * @return a vertex u \in V(q) which is not yet matched..
	 * 
	 */
	// @SuppressWarnings({ "rawtypes", "unchecked" })
	public Object[] NextQueryVertex(
			Map<Integer, Map<Integer, List<Integer>>> q, // query
															// graph(queryGraph)
			Map<Integer, Set<Integer>> L_q, // V_q(queryLabelIndex)
			List<Object[]> M) {
		// public Object NextQueryVertex(LabeledGraph q, List<Object[]> M){

		// Object selectedVertex =null;

		// Set<Object> V_q = q.vertexSet();
		// Set<Integer> V_q = L_q.keySet();

		// for(Object[] pairs: M){
		// System.out.print("u:"+pairs[0]+"-v:"+pairs[1]+",");
		// }
		// System.out.println("..");
		// for(Object u : V_q) {
		// System.out.print("u:"+u.toString()+",");
		// }
		// System.out.println("..");
		//
		/*
		 * for (Object u : V_q) { boolean flag = false; for (Object[] pairs : M)
		 * { if (pairs[0].equals(u)) { flag = true; break; } } // get the
		 * unmatched query vertex. if (flag == false) { if (M.size() == 0) {
		 * //System.out.println("NextQueryVertex()::M init:u"+u.toString());
		 * return u; } else { for(Object[] current:M){ // if
		 * (q.containsEdge(current[0], u)) { try{ if
		 * (q.get(current[0]).containsKey(u)) {
		 * logger.info("NextQueryVertex()::M current:u"
		 * +current[0]+", neighbour:u"+u.toString()); return u; }//if
		 * }catch(Exception e){ return null; } } }
		 * 
		 * } }
		 * 
		 * return null;
		 */
		// BFS based next query vertex.
		try {
			if (M.size() == 0) {
				return null;
			} else {
				for (int i = 1; i < M.size() + 1; i++) {
					Object u = M.get(M.size() - i)[0];

					for (Integer nextVertex : q.get(u).keySet()) {
						boolean contains = false;
						// nextVertex is already in M.
						for (Object[] pairs : M) {
							if (pairs[0].equals(nextVertex)) {
								contains = true;
								break;
							}
						}
						if (contains == false) {
							Object[] next = { nextVertex, u };
							return next;
						}
					}
					// all neighbors of current query vertex u have been
					// matched.
					// go to previous node
				}
			}
		} catch (Exception e) {
			return null;
		}
		return null;
	}

	/**
	 * Obtain a refined candidate certex set C_R from C_u by using
	 * algorithm-specific pruning rules. For Ullmann algorithm, it only uses the
	 * degree of data graph vertex.
	 * 
	 * @param g
	 *            : LabeledGraph, for data graph to be queried.
	 * @param q
	 *            : LabeledGraph, for query graph.
	 * @param M
	 *            : List, a partial embedding, contains pairs of a query vertex
	 *            and a corresponding data vertex.
	 * @param F
	 *            : Map, a vector for indication of matched vertex in g. entry
	 *            is vertex, value is boolean object.
	 * 
	 * @return a vertex u \in V(q) which is not yet matched..
	 * 
	 */

	// @SuppressWarnings({ "rawtypes", "unchecked" })
	public List<Object> RefineCandidates(// LabeledGraph g, LabeledGraph q,
			Map<Integer, List<Integer>> g, // data graph(adjIndex)
			Map<Integer, Set<Integer>> L_g, // V_g(dataLabelIndex)
			Map<Integer, Map<Integer, List<Integer>>> q, // query
															// graph(queryGraph)
			Map<Integer, Set<Integer>> L_q, // V_q(queryLabelIndex)
			List<Object[]> M, Object[] u, Map<Object, Set<Object>> F) {
		// public List<Object> RefineCandidates(LabeledGraph g, LabeledGraph q,
		// Object u, Set F){
		// public List<Object> RefineCandidates(LabeledGraph g, LabeledGraph q,
		// Object u,Map<Object,Boolean> F){

		List<Object> RefinedC_u = new ArrayList<Object>();
		if (!F.containsKey(u[0])) {
			Set<Object> visited = new HashSet<Object>();
			F.put(u[0], visited);
		}
		List<Object> C_u = FilterCandidate(L_g, L_q, u[0], F, M);
		// logger.info("RefineCandidates():: u"+u[0].toString()+",parent u"+u[1].toString()+". Latest matched in M, u"+M.get(M.size()-1)[0]+"-v"+M.get(M.size()-1)[1]+". Candidate Size: "+C_u.size());

		/*
		 * for(Integer source:g.keySet()){ for(Integer dest:g.get(source)){
		 * logger
		 * .info("adjecent list of v"+source.toString()+" is v"+dest.toString
		 * ()); } }
		 */
		for (Object candidate : C_u) {
			// A Matrix of degree for g should be precomputed, instead of using
			// degreeOf function.
			// int g_degree = g.degreeOf(candidate);
			// int q_degree = q.degreeOf(u);
			int g_degree = 0;
			int q_degree = 0;
			try {
				g_degree = g.get(candidate).size();
			} catch (Exception e) {
				g_degree = 0;
			}
			try {
				q_degree = q.get(u[0]).size();
			} catch (Exception e) {
				q_degree = 0;
			}
			// System.out.println("RefineCandidates()::g" +
			// candidate.toString());

			/*
			 * Integer preVertex = null; for(Object[] pairs: M){
			 * if(pairs[0].equals(u[1])){ preVertex = (Integer)pairs[1]; } }
			 */
			// logger.info("PreVertex of u"+u[0].toString()+" is :: u"+u[1].toString()+",v"+preVertex.toString()+".Candidate:v"+candidate.toString());
			// logger.info("Degree of candidate:"+g_degree+", degree of query:"+q_degree);

			if ((g_degree != 0) && (q_degree != 0) && (g_degree >= q_degree)) {
				/*
				 * if(preVertex != null){
				 * if(g.get(preVertex).contains(candidate)){
				 * RefinedC_u.add(candidate);
				 * //logger.info("add candidate: "+candidate.toString()); }
				 * }else{ RefinedC_u.add(candidate); }
				 */RefinedC_u.add(candidate);
			}
		}

		return RefinedC_u;

	}

	/**
	 * Check whether the edges between u and already matched query vertices of q
	 * have corresponding edges between v and already matched query vertices of
	 * g.
	 * 
	 * @param g
	 *            : LabeledGraph, for data graph to be queried.
	 * @param q
	 *            : LabeledGraph, for query graph.
	 * @param M
	 *            : List, a partial embedding, contains pairs of a query vertex
	 *            and a corresponding data vertex.
	 * @param F
	 *            : match indication vector.
	 * @param u
	 *            : Object, query vertex.
	 * @param v
	 *            : Object, data graph vertex.
	 * 
	 * @return a vertex u \in V(q) which is not yet matched..
	 * 
	 */

	// @SuppressWarnings({ "rawtypes", "unchecked" })
	public boolean IsJoinable( // LabeledGraph g, LabeledGraph q,
			Map<Integer, List<Integer>> g, // data graph(adjIndex)
			Map<Integer, Map<Integer, List<Integer>>> q, // query
															// graph(queryGraph)
			List<Object[]> M, Object u, Object v) {
		// public boolean IsJoinable(LabeledGraph g, LabeledGraph q,
		// List<Object[]> M, Object u, Object v ){

		boolean isJoinable = false;

		if (M.size() == 0) {
			return true;
		} else {
			boolean allMatched = true;
			try {
				for (Object[] matched : M) {
					// if(q.containsEdge(u,matched[0])){
					if (q.get(u).containsKey(matched[0])) {
						// allMatched = allMatched && g.containsEdge(v,
						// matched[1]);
						allMatched = allMatched
								&& g.get(v).contains(matched[1]);
						// System.out.println("IsJoinable():: Matched:g"+matched[1].toString()+",q"+matched[0].toString()
						// +" Candidate:g"+v.toString()+",q"+u.toString()+" "+allMatched);
					}
					if (!allMatched) {
						break;
					}
				}
			} catch (Exception e) {
				return false;
			}

			isJoinable = allMatched;
		}
		// System.out.println("IsJoinable()::"+isJoinable);
		return isJoinable;

	}

	/**
	 * update Matched status information.
	 * 
	 * @param M
	 *            : List, a partial embedding, contains pairs of a query vertex
	 *            and a corresponding data vertex.
	 * @param u
	 *            : Object, query vertex.
	 * @param v
	 *            : Object, data graph vertex.
	 * 
	 * @return void.
	 * 
	 */

	public void UpdateState(List<Object[]> M, Map<Object, Set<Object>> F,
			Object u, Object v) {

		Object[] match = { u, v };
		M.add(match);
		if (F.containsKey(u)) {
			F.get(u).add(v);
		} else {
			Set<Object> visited = new HashSet<Object>();
			visited.add(v);
			F.put(u, visited);
		}

		// logger.info("UpdateState():: u"+match[0].toString()+",v"+match[1].toString());
	}

	/**
	 * restore Matched status information.
	 * 
	 * @param M
	 *            : List, a partial embedding, contains pairs of a query vertex
	 *            and a corresponding data vertex.
	 * @param F
	 *            : match indication vector.
	 * @param u
	 *            : Object, query vertex.
	 * @param v
	 *            : Object, data graph vertex.
	 * 
	 * @return void.
	 * 
	 */

	public void RestoreState(List<Object[]> M, Map<Object, Set<Object>> F,
			Object[] u, Object v) {
		// public void RestoreState(List<Object[]> M, Map<Object,Boolean>
		// F,Object u,Object v){

		/*
		 * int index =0; for(Object[] matchedPairs:M){
		 * if(matchedPairs[0].equals(u[0])){ break; } index++; }
		 * 
		 * //delete all elements added after u in M for(int
		 * i=index;i<M.size();i++){ M.remove(index); //F.remove(v); }
		 */

		/*
		 * Object prevMatchedQueryVertex = M.get(M.size()-1)[0]; Object
		 * parentOfPrevVertex = null; //Object currentMatchedQueryVertex = u[0];
		 * //Object parentOfCurrentVertex = u[1]; for(Object vertex:F.keySet()){
		 * if(F.get(vertex).contains(prevMatchedQueryVertex)){
		 * parentOfPrevVertex = vertex; break; } }
		 */
		// if(M.size()!=0){
		M.remove(M.size() - 1);
		// }

		/*
		 * if(F.get(u[0]).size()>0){ F.get(u[0]).remove(v); }else{
		 * F.remove(u[0]); }
		 */
		// if u is starting vertex, mark corresponding v in F as TRUE(visited).
		/*
		 * if(index==0){ //F.put(v, Boolean.valueOf(true)); //F = new
		 * HashSet<Object>(); F.add(v); }
		 */
		// logger.info("RestoreState():: remove u"+u[0].toString());

	}

	public Object ClearState(List<Object[]> M, Map<Object, Set<Object>> F,
			Object[] u) {

		int index = 0;
		int size = M.size();

		// find parent vertex in stack M.
		for (Object[] matchedPairs : M) {
			if (matchedPairs[0].equals(u[1])) {
				break;
			}
			index++;
		}
		// logger.info("ClearState():: backtack to index "+index);

		// clear all visited entry in F, and remove entry in M.
		for (int i = index + 1; i < size; i++) {
			// for(Object vertex:F.keySet()){
			// if(vertex.equals(M.get(i)[0])){
			F.remove(M.get(i)[0]);
			// logger.info("ClearState()::remove u"+M.get(i)[0].toString()+" in F");
			// }
			// }
		}
		// if(index ==0){
		F.remove(u[0]);
		// logger.info("ClearState()::remove u"+u[0].toString()+" in F");
		// }

		for (int i = index + 1; i < size; i++) {
			// logger.info("ClearState()::remove u"+M.get(M.size()-1)[0].toString()+" in M");
			M.remove(M.size() - 1);
		}

		// logger.info("ClearState()::size of F "+F.size()+",size of M"+M.size());
		// for(Object vertex:F.keySet()){
		// logger.info("ClearState():: content in F-u"+vertex.toString());
		// }
		// logger.info("ClearState():: remove u"+u[0].toString()+",backtack to u"+u[1].toString());
		return u[1];

	}


	
	
}
