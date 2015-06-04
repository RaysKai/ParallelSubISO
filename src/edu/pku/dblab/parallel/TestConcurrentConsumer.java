package edu.pku.dblab.parallel;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;


public class TestConcurrentConsumer  implements Runnable {

	private ConcurrentHashMap<Integer,BlockingQueue<Message>> buffer = 
			new ConcurrentHashMap<Integer,BlockingQueue<Message>>();
	private int i;

	public TestConcurrentConsumer(int i, ConcurrentHashMap<Integer,BlockingQueue<Message>> buffer){
	        this.buffer=buffer;
	        this.i = i;
	    }
	 
	@Override
	public void run() {
	try{
	   while(buffer.get(Integer.valueOf(i)).size()!=0){
	   try{
	       Message msg;
	       //consuming messages until exit message is received
	            //Thread.sleep(10);
	    	   msg = buffer.get(Integer.valueOf(i)).take();
	    	   System.out.println(""+Thread.currentThread().getId()+": Consumed "+msg.getMsg()+" in candidate "+i+". Queue size:"+buffer.get(Integer.valueOf(i)).size());
	    }
	   catch(NullPointerException npe){
	    		Thread.sleep(2);
	    }
	   catch(Exception e) {
	            e.printStackTrace();
	    }
	}
       buffer.remove(Integer.valueOf(i));
       System.out.println(""+Thread.currentThread().getId()+": Entry "+ i +" in buffer is removed. Buffer size: "+buffer.size());
    }
   catch(Exception e) {
            e.printStackTrace();
    }
	}

}
