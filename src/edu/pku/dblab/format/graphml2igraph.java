package edu.pku.dblab.format;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.OutputStream;
//import java.util.Iterator;
import java.util.Random;
 
public class graphml2igraph {
 
  private static final String XML_FILE = "F:\\yeast.igraph";
  private static final String IGRAPH_FILE = "F:\\output.igraph";
	
  public static int getBiasedRandom2(double bias, int min, int max) {
	    double rndBiased;
	    double variance = (max-min)*0.3;
	    Random random = new Random();

	    do {
	         rndBiased = bias + (random.nextGaussian() * variance);
	    } while(rndBiased < min && rndBiased <= max);

	    return (int)Math.floor(rndBiased);
	}
  
  public static int getBiasedRandom(double bias, double bias_depth, int min, int max) {
	    double bias_depth_perc = 0.1;
	    double bias_depth_abs = (max - min)*bias_depth_perc;
	    double min_bias = bias - bias_depth;
	    double max_bias = bias + bias_depth;
	    Random tRandom = new Random();

	    if (max_bias > max) max_bias = max;
	    if (min_bias < min) min_bias = min;

	    double variance = (max_bias - min_bias)/2;


	    double rndBiased = bias + tRandom .nextGaussian() * variance;

	    if (rndBiased > max)
	       rndBiased = max - (rndBiased - max);

	    if (rndBiased < min)
	       rndBiased = min + (min - rndBiased);

	    return (int)Math.floor(rndBiased);
	}

	public static void main(String[] args) throws Exception {
/*    Graph graph = new TinkerGraph();
    GraphMLReader reader = new GraphMLReader(graph);
 
    InputStream is = new BufferedInputStream(new FileInputStream(XML_FILE));
    reader.inputGraph(is);
    
    for (Vertex vertex : graph.getVertices()) {
    	 System.out.println(vertex);
    	 }
    	 System.out.println("Edges of " + graph);
    	 for (Edge edge : graph.getEdges()) {
    	 System.out.println(edge);
    	 }
    	 
*/			FileReader fr = new FileReader(XML_FILE);
			BufferedReader br = new BufferedReader(fr);

			String crlf = System.getProperty("line.separator");
			OutputStream os = new FileOutputStream(new File(IGRAPH_FILE));
//			os.write("t # 0".getBytes());
//			os.write(crlf.getBytes());

			String str = br.readLine();
			while (str != null) {
				String[] words = str.split(" ");
				String row = "";
					for(String s:words){
						row = row+","+s;
				}
				os.write(row.getBytes());
				os.write(crlf.getBytes());
				str = br.readLine();

			}
			
			os.flush();
			os.close();
			br.close();
			fr.close();

			/*    for(int i=0;i<100;i++){
    	System.out.println("Random: "+getBiasedRandom2(0.3,1,90));
    }
*/ 
    }
}