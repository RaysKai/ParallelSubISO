package edu.pku.dblab.format;

import java.io.*;
import java.util.*;

public class edgelist2igraph {

	private static final String EDGELIST_FILE = "D:\\data\\scalability\\powerlaw1m.edgelist";
	private static final String IGRAPH_FILE = "D:\\data\\scalability\\powerlaw1m.igraph";
	private static final int NUM_NODES = 1000000;
	private static final int MAX_LABELS = 25;

	  public static int getBiasedRandom(double bias, int min, int max) {
		    double rndBiased;
		    double variance = (max-min)*0.3;
		    Random random = new Random();

		    do {
		         rndBiased = bias + (random.nextGaussian() * variance);
		    } while(rndBiased < min && rndBiased <= max);

		    return (int)Math.floor(rndBiased);
		}
	
	public static void main(String[] args) {
		
		//synthetic labelset.
		int[] labelset = new int[200];
		for(int i=0;i<200;i++) labelset[i]=i;
		int edge_counter = 0;

		try {
			// Open the generated iGraph file.
			String crlf = System.getProperty("line.separator");
			OutputStream os = new FileOutputStream(new File(IGRAPH_FILE));
			os.write("t # 0".getBytes());
			os.write(crlf.getBytes());

			//add the file header and vertex list
			for (int m = 0; m < NUM_NODES; m++) {

				int num_labels = getBiasedRandom(0,1,MAX_LABELS);
				List<Integer> chosen_labelset = new ArrayList<Integer>();
				String vertex = "v "+m;

				for(int i=0;i<num_labels;i++){
					int choosed_label = getBiasedRandom(0,1,labelset.length+1);
					boolean isChosen = false;
					for(Integer label:chosen_labelset){
						if(label.intValue() == choosed_label){
							isChosen = true;
							break;
						}
					}
					if(!isChosen){
						chosen_labelset.add(Integer.valueOf(choosed_label));
					}
				}
				
				for(Integer label:chosen_labelset){
					vertex = vertex +" "+label.toString();
				}
				System.out.println("Vertex is: ["+vertex+"]");

				os.write(vertex.getBytes());
				os.write(crlf.getBytes());
			}
			FileReader fr = new FileReader(EDGELIST_FILE);
			BufferedReader br = new BufferedReader(fr);

			String str = br.readLine();

			while (str != null) {
				String[] words = str.split(" ");
				String edge = "e "+words[0]+ " "+ words[1]+ " 0";
				System.out.println("Edge is: ["+edge+"]");
				os.write(edge.getBytes());
				os.write(crlf.getBytes());
				str = br.readLine();
				edge_counter ++;
			}
			System.out.println("Total Edges: "+edge_counter);
			
			os.flush();
			os.close();

			br.close();
			fr.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
