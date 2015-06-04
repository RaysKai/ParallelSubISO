package edu.pku.dblab.parallel;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class TestConcurrentProducer implements Runnable {

	private ConcurrentHashMap<Integer,BlockingQueue<Message>> buffer = 
			new ConcurrentHashMap<Integer,BlockingQueue<Message>>();
	private int BUFFER_SIZE = 10;
	private int producer;
 
    public TestConcurrentProducer(int producer, ConcurrentHashMap<Integer,BlockingQueue<Message>> buffer){
        this.buffer=buffer;
        this.producer = producer;
    }

    @Override
    public void run() {
		
		//for(int candidate_id = 0 ;candidate_id < BUFFER_SIZE ;candidate_id++){
			BlockingQueue<Message> entry = new ArrayBlockingQueue<Message>(BUFFER_SIZE);
			for(int pattern_id = 0;pattern_id < BUFFER_SIZE; pattern_id++ ){
				Message msg = new Message("Content"+pattern_id);
				entry.add(msg);
			}
			buffer.put(Integer.valueOf(producer),entry);
			
			if( producer+1 == BUFFER_SIZE){
				//BlockingQueue<Message> entry = new ArrayBlockingQueue<Message>(1);
				entry = new ArrayBlockingQueue<Message>(1);
				Message msg = new Message("exit");
				entry.add(msg);
				buffer.put(Integer.valueOf(BUFFER_SIZE),entry);
			}
		//}
		
/*		BlockingQueue<Message> entry = new ArrayBlockingQueue<Message>(1);
		entry.add(new Message("exit"));
		buffer.put(Integer.valueOf(BUFFER_SIZE),entry);
		

        for(int i = 0; i < BUFFER_SIZE; i++) {   
           //exec.execute(new Producer(buffer));
           exec.execute(new TestConcurrentConsumer(i, buffer));
        }   
*/
    }   

}
