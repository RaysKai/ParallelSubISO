package edu.pku.dblab.parallel;

import java.util.concurrent.ExecutorService;   
import java.util.concurrent.Executors;   

public class CachedThreadPool {   
    public static void main(String[] args) {   
        ExecutorService exec = Executors.newCachedThreadPool();   
		//Record Start time of each query.
		long startTime = System.currentTimeMillis();

       for(int i = 0; i < 3000; i++) {   
           exec.execute(new LiftOff());   
        }   

       //End time
		long endTime = System.currentTimeMillis();
		long queryTime = endTime - startTime;

		exec.shutdown();       
		
		System.out.println("Total expense: "+queryTime);
    }   
}
