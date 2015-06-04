package edu.pku.dblab.parallel;

import java.util.concurrent.ExecutorService;   
import java.util.concurrent.Executors;   
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class TestConcurrentCP {

	//private static int BUFFER_SIZE = 5;

	private static ConcurrentHashMap<Integer,BlockingQueue<Message>> buffer = 
			new ConcurrentHashMap<Integer,BlockingQueue<Message>>();
 
	public static void main(String[] args) {   
        ExecutorService exec = Executors.newCachedThreadPool();   
		//Record Start time of each query.
		long startTime = System.currentTimeMillis();
		
/*		for(int candidate_id = 0 ;candidate_id < BUFFER_SIZE ;candidate_id++){
			BlockingQueue<Message> entry = new ArrayBlockingQueue<Message>(BUFFER_SIZE);
			for(int pattern_id = 0;pattern_id < BUFFER_SIZE; pattern_id++ ){
				Message msg = new Message("Content"+pattern_id);
				entry.add(msg);
			}
			buffer.put(Integer.valueOf(candidate_id),entry);
		}
		
		BlockingQueue<Message> entry = new ArrayBlockingQueue<Message>(1);
		entry.add(new Message("exit"));
		buffer.put(Integer.valueOf(BUFFER_SIZE),entry);
		
*/
        for(int i = 0; i < 100; i++) {   
           exec.execute(new TestConcurrentProducer(i, buffer));
           //exec.execute(new TestConcurrentConsumer(i, buffer));
        }   
	    
        try{
	    	while(buffer.size()==0){
	    	   Thread.sleep(1);
	       }
	    }catch(Exception e) {
            e.printStackTrace();
	    }

        for(int i = 0; i < 100; i++) {   
            //exec.execute(new TestConcurrentProducer(i, buffer));
            exec.execute(new TestConcurrentConsumer(i, buffer));
         }   

       //End time
		long endTime = System.currentTimeMillis();
		long queryTime = endTime - startTime;

		exec.shutdown();       
		
		System.out.println("Total expense: "+queryTime);
    }   


}
