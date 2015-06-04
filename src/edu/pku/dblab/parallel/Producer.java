package edu.pku.dblab.parallel;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class Producer implements Runnable {
 
    //private BlockingQueue<Message> queue;
	private ConcurrentHashMap<Integer,ConcurrentHashMap<Integer,Message>> buffer = 
			new ConcurrentHashMap<Integer,ConcurrentHashMap<Integer,Message>>();
     
    public Producer(ConcurrentHashMap<Integer,ConcurrentHashMap<Integer,Message>> buffer){
        this.buffer=buffer;
    }
    @Override
    public void run() {
        //produce messages
/*        for(int i=0; i<100; i++){
            Message msg = new Message(""+i);
            try {
                Thread.sleep(i);
                queue.put(msg);
                System.out.println("Produced "+msg.getMsg());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
*/      
    	
    	for(int candidate_id = 0;candidate_id < 10;candidate_id++){
    		ConcurrentHashMap<Integer,Message> pattern = new ConcurrentHashMap<Integer,Message>();
    		buffer.put(Integer.valueOf(candidate_id) , pattern);
    		for(int pattern_id = 0;pattern_id < 3;pattern_id++){
    			Message in_msg = new Message("partial");
    	        try {
    	            //Thread.sleep(i);
    	            pattern.put(Integer.valueOf(pattern_id), in_msg);
    	            System.out.println("Produced "+in_msg.getMsg());
    	        } catch (Exception e) {
    	            e.printStackTrace();
    	        }
    		}
    	}
    	
    	
    	//adding exit message
        //Message msg = new Message("exit");
        try {
        	buffer.put(Integer.valueOf(101),null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
 
}