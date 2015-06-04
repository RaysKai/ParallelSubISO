package edu.pku.dblab.binary;

import java.io.*;
import java.util.*;
import java.nio.channels.*;
import java.nio.*;
import org.apache.log4j.Logger;

public class CheckBinaryPatternIndex {
	
	private static final String EPI_FILE = "D:\\data\\graphdb.epi";
//	private static final String ELI_FILE = "D:\\graphdb.eli";
//	private static final String IGRAPH_FILE = "D:\\yeast.igraph";
//	private static final String OUTPUT_FILE = "D:\\output.txt";
	
//	private static final int MAX_VERTICES = 30000;
	private static final int PATH_IN_BLOCK = 255;
	private static final int BLOCK_SIZE = (PATH_IN_BLOCK + 1)*8;
	private static final int MARGIN = 10*20;
	private static final Logger logger = Logger.getLogger(CheckBinaryPatternIndex.class);

	public static void main(String[] args){
		
		try {
			RandomAccessFile edgePatternIndex = new RandomAccessFile(new File(EPI_FILE), "r");
			FileChannel  inChannel = edgePatternIndex.getChannel();
			
/*			ByteBuffer bb = ByteBuffer.allocate(4);  
			inChannel.read(bb);
			int num_patterns = bb.getInt();
			
			bb = ByteBuffer.allocate(8);  
			inChannel.read(bb);
			long edgelist_offset = bb.getLong();
*/			
			int num_patterns = edgePatternIndex.readInt();
			long edgelist_offset = edgePatternIndex.readLong();
			String s = "Number of Patterns:"+num_patterns+". Current file pointer:"+edgePatternIndex.getFilePointer();
			logger.info(s);
			
			s = "Start offset of edge list:" + edgelist_offset +". Current file pointer:"+edgePatternIndex.getFilePointer();
			logger.info(s);
			
			int counter = 1;
			//long pos = edgePatternIndex.getFilePointer();
			long pos = inChannel.position();
			while(num_patterns>0){
				edgePatternIndex.seek(pos);
/*				inChannel.position(pos);
				
				bb = ByteBuffer.allocate(4);  
				inChannel.read(bb);
				int source = bb.getInt();
				
				bb = ByteBuffer.allocate(4);  
				inChannel.read(bb);
				int dest = bb.getInt();
				
				bb = ByteBuffer.allocate(4);  
				inChannel.read(bb);
				int num = bb.getInt();
				
				bb = ByteBuffer.allocate(8);  
				inChannel.read(ByteBuffer.allocate(8));
				long offset = bb.getLong();
*/
				int source = edgePatternIndex.readInt();
				int dest = edgePatternIndex.readInt();
				int num = edgePatternIndex.readInt();
				long offset = edgePatternIndex.readLong();
				pos= edgePatternIndex.getFilePointer();
				//pos = inChannel.position();
				
				s = "Pattern "+counter+":"+source+"-"+dest+", frequency: "+num+", offset:"+offset+". Current file pointer:"+edgePatternIndex.getFilePointer()+". Next pos:"+pos;
				logger.info(s);
				
				int block = 1;
				
				edgePatternIndex.seek(offset);
				//inChannel.position(offset);
				
				for(int i=1;i<num+1;i++){

					if((i!=1)&&( i%(PATH_IN_BLOCK) == 1)){
						block ++;
						offset = edgePatternIndex.readLong();
//						bb = ByteBuffer.allocate(8);  
//						inChannel.read(ByteBuffer.allocate(8));
//						offset = bb.getLong();
						
						s = "Jump to offset:"+offset+".Current:"+edgePatternIndex.getFilePointer();
						edgePatternIndex.seek(offset);
						//inChannel.position(offset);
						logger.info(s);
					}

					int nodea = edgePatternIndex.readInt();
//					bb = ByteBuffer.allocate(4);  
//					inChannel.read(bb);
//					int nodea = bb.getInt();
					int nodeb = edgePatternIndex.readInt();
//					bb = ByteBuffer.allocate(4);  
//					inChannel.read(bb);
//					int nodeb = bb.getInt();
					
					s = "//block:"+block+", node:"+nodea+"-"+nodeb+", Current file pointer:"+edgePatternIndex.getFilePointer();
					logger.info(s);
					
				}
				
				num_patterns--;
				
			}
				
			//finalize
			//br.close();
			//igraphFile.close();
			inChannel.close();
			edgePatternIndex.close();
			
			//edgeListIndex.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

/*	public static int byte2Int(byte[] bytes) throws IOException{
	    int s = 0;
	    int s0 = bytes[0] & 0xff;// ×îµÍÎ»
	    int s1 = bytes[1] & 0xff;
	    int s2 = bytes[2] & 0xff;
	    int s3 = bytes[3] & 0xff;
	      
	    s1 <<= 8;
	    s2 <<= 16;
	    s3 <<= 24;
	      
	    s = s0 | s1 | s2 | s3; 
	    return s;
	}

	public static long byte2Long(byte[] byteArray)throws IOException{ 
        
            long longArray=(((long)byteArray[0]&0xff)<<56) 
                        |(((long)byteArray[1]&0xff)<<48) 
                        |(((long)byteArray[2]&0xff)<<40) 
                        |(((long)byteArray[3]&0xff)<<32) 
                        |(((long)byteArray[4]&0xff)<<24) 
                        |(((long)byteArray[5]&0xff)<<16) 
                        |(((long)byteArray[6]&0xff)<<8) 
                        |(((long)byteArray[7]&0xff)<<0); 
             
 
        return longArray; 
    } 
*/
}
