package edu.pku.dblab.graph;

import java.util.*;
//import java.util.Map.Entry;

import org.jgrapht.graph.*;
import org.jgrapht.*;
//import org.jgrapht.util.*;

public class LabeledGraph<V, E>
extends AbstractBaseGraph<V, E>
implements UndirectedGraph<V, E>{
	
    private static final long serialVersionUID = 3904960843590599219L;
    
//    private List<Object> vertexLabelSet = new ArrayList<Object>();
//    private Specifics specifics;
    private Map<V, List<Object>> vertexLabelMap = new HashMap<V, List<Object>>();
  
    /**
     * Creates a new directed multigraph.
     *
     * @param edgeClass class on which to base factory for edges
     */
    public LabeledGraph(Class<? extends E> edgeClass)
    {
        this(new ClassBasedEdgeFactory<V, E>(edgeClass));
    }

    /**
     * Creates a new directed multigraph with the specified edge factory.
     *
     * @param ef the edge factory of the new graph.
     */
    public LabeledGraph(EdgeFactory<V, E> ef)
    {
        super(ef, false, false);
    }
    
    /**
     * Adds the specified vertex to this graph if not already present. More
     * formally, adds the specified vertex, <code>v</code>, to this graph if
     * this graph contains no vertex <code>u</code> such that <code>
     * u.equals(v)</code>. If this graph already contains such vertex, the call
     * leaves this graph unchanged and returns <tt>false</tt>. In combination
     * with the restriction on constructors, this ensures that graphs never
     * contain duplicate vertices.
     *
     * @param id vertex to be added to this graph.
     *
     * @return <tt>true</tt> if this graph did not already contain the specified
     * vertex.
     *
     * @throws NullPointerException if the specified vertex is <code>
     * null</code>.
     */
    public boolean addLabeledVertex(V id, List<Object> labels){

    	List<Object> vertexLabels = new ArrayList<Object>();
    	if (id == null) {
			throw new NullPointerException();
		} else if (containsVertex(id)) {
			return false;
		} else {
			addVertex(id);
			for (Object label : labels) {
				vertexLabels.add(label);
			}
			vertexLabelMap.put(id, vertexLabels);
			return true;
		}
   }
    
    /**
     * Adds the specified vertex to this graph if not already present. More
     * formally, adds the specified vertex, <code>v</code>, to this graph if
     * this graph contains no vertex <code>u</code> such that <code>
     * u.equals(v)</code>. If this graph already contains such vertex, the call
     * leaves this graph unchanged and returns <tt>false</tt>. In combination
     * with the restriction on constructors, this ensures that graphs never
     * contain duplicate vertices.
     *
     * @param id vertex to be added to this graph.
     *
     * @return <tt>true</tt> if this graph did not already contain the specified
     * vertex.
     *
     * @throws NullPointerException if the specified vertex is <code>
     * null</code>.
     */
    public boolean addVertexLabel(V id, Object label){

		if (id == null) {
			throw new NullPointerException();
		} else if (containsVertex(id)) {
			if(vertexLabelMap.get(id) == null){
				List<Object> vertexLabels = new ArrayList<Object>();
				vertexLabels.add(label);
				vertexLabelMap.put(id,vertexLabels);
				//System.out.println("adding new list");
			}else{
				//System.out.println("adding existing list");
				vertexLabelMap.get(id).add(label);
			}
			return true;
		} else {
			return false;
		}
   }
    
    /**
     * Adds the specified vertex to this graph if not already present. More
     * formally, adds the specified vertex, <code>v</code>, to this graph if
     * this graph contains no vertex <code>u</code> such that <code>
     * u.equals(v)</code>. If this graph already contains such vertex, the call
     * leaves this graph unchanged and returns <tt>false</tt>. In combination
     * with the restriction on constructors, this ensures that graphs never
     * contain duplicate vertices.
     *
     * @param id vertex to be added to this graph.
     *
     * @return <tt>true</tt> if this graph did not already contain the specified
     * vertex.
     *
     * @throws NullPointerException if the specified vertex is <code>
     * null</code>.
     */
    public List<Object> getVertexLabel(V id){

//    	List<Object> vertexLabels = new ArrayList<Object>();
    	if (id == null) {
			throw new NullPointerException();
		} else if (containsVertex(id)) {
/*			for (Entry<V, Object> entry : vertexLabelMap.entrySet()) {
			    if(entry.getKey().equals(id)){
				    vertexLabels.add(entry.getValue());
			    }
			}*/
			
			return vertexLabelMap.get(id);
		} else {
			System.out.println("null");
			return null;
		}
   }
    
    /**
     * Adds the specified vertex to this graph if not already present. More
     * formally, adds the specified vertex, <code>v</code>, to this graph if
     * this graph contains no vertex <code>u</code> such that <code>
     * u.equals(v)</code>. If this graph already contains such vertex, the call
     * leaves this graph unchanged and returns <tt>false</tt>. In combination
     * with the restriction on constructors, this ensures that graphs never
     * contain duplicate vertices.
     *
     * @param id vertex to be added to this graph.
     *
     * @return <tt>true</tt> if this graph did not already contain the specified
     * vertex.
     *
     * @throws NullPointerException if the specified vertex is <code>
     * null</code>.
     */
    public Object getVertexFirstLabel(V id){

    	if (id == null) {
			throw new NullPointerException();
		} else if (containsVertex(id)) {
			return vertexLabelMap.get(id).get(0);
		} else {
			//System.out.println("null");
			return null;
		}
   }
    
    
}

