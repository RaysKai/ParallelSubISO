package edu.pku.dblab.parallel;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class Consumer implements Runnable{
 
	private BlockingQueue<Message> queue;
     
	private ConcurrentHashMap<Integer,ConcurrentHashMap<Integer,Message>> buffer = 
			new ConcurrentHashMap<Integer,ConcurrentHashMap<Integer,Message>>();

	public Consumer(ConcurrentHashMap<Integer,ConcurrentHashMap<Integer,Message>> buffer){
	        this.buffer=buffer;
	    }
	 
	    @Override
	    public void run() {
	        try{
	            Message msg;
	            //consuming messages until exit message is received
	            while((msg = queue.take()).getMsg() !="exit"){
	            Thread.sleep(10);
	            System.out.println("Consumed "+msg.getMsg());
	            }
	        }catch(InterruptedException e) {
	            e.printStackTrace();
	        }
	    }
}