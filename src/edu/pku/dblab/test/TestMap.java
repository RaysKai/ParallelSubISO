package edu.pku.dblab.test;

import java.util.*;

public class TestMap {
	
	public static void main(String[] args){
		
		Map<Integer,Map<Integer,Set<int[]>>> test = new TreeMap<Integer,Map<Integer,Set<int[]>>>();
		
		Map<Integer,Set<int[]>> outerEntry1 = new TreeMap<Integer,Set<int[]>>();
		Map<Integer,Set<int[]>> outerEntry2 = new TreeMap<Integer,Set<int[]>>();
		
		Set<int[]> innerEntry1 = new HashSet<int[]>();
		//Set<int[]> innerEntry1 = new HashSet<int[]>();
		
		int[] test1 = {0,1};
		int[] test2 = {1,2};
		int[] test3 = {4,5};
		int[] test4 = {3,2};
		
		innerEntry1.add(test1);
		innerEntry1.add(test2);
		
		outerEntry1.put(Integer.valueOf(1), innerEntry1);
		test.put(Integer.valueOf(2), outerEntry1);
		
		outerEntry2.put(Integer.valueOf(2), innerEntry1);
		test.put(Integer.valueOf(1), outerEntry2);
		
		for(int[] entry:test.get(Integer.valueOf(1)).get(Integer.valueOf(2))){
			System.out.println("Entry in (1,2) is:("+entry[0]+","+entry[1]+").");
		}
		System.out.println("--------------------------------------");
		
		for(int[] entry:test.get(Integer.valueOf(2)).get(Integer.valueOf(1))){
			System.out.println("Entry in (2,1) is:("+entry[0]+","+entry[1]+").");
		}
		
		System.out.println("++++++++++++++++++++++++++++++++++++++");
		
		innerEntry1.add(test3);
		innerEntry1.add(test4);
		
		for(int[] entry:test.get(Integer.valueOf(1)).get(Integer.valueOf(2))){
			System.out.println("Entry in (1,2) is:("+entry[0]+","+entry[1]+").");
		}
		
		System.out.println("--------------------------------------");

		for(int[] entry:test.get(Integer.valueOf(2)).get(Integer.valueOf(1))){
			System.out.println("Entry in (2,1) is:("+entry[0]+","+entry[1]+").");
		}
		
		
		
		
	}

}
