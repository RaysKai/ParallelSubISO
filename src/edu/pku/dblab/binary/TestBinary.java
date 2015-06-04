package edu.pku.dblab.binary;

import java.io.*;
import java.util.*;


public class TestBinary {

	private static final String EPI_FILE = "D:\\test.binary";

	public static void main(String[] args){
		
		try {
			
			File outputFile = new File(EPI_FILE);
			if (outputFile.exists()){
				outputFile.delete();
	          }  

			RandomAccessFile edgePatternIndex = new RandomAccessFile(outputFile, "rw");
			
			System.out.println("Current File Position: "+edgePatternIndex.getFilePointer());
			edgePatternIndex.write(new byte[32*8]);
			System.out.println("Current File Position: "+edgePatternIndex.getFilePointer());
			long end_of_file = edgePatternIndex.length();
			System.out.println("End of file:"+end_of_file);
			edgePatternIndex.skipBytes( (int)edgePatternIndex.length() );
			System.out.println("End of File Position: "+edgePatternIndex.getFilePointer());
			edgePatternIndex.writeInt(20);
			System.out.println("Current File Position: "+edgePatternIndex.getFilePointer());
			edgePatternIndex.write(new byte[32*8]);
			System.out.println("Current File Position: "+edgePatternIndex.getFilePointer());
			edgePatternIndex.seek(end_of_file);
			System.out.println("Current File Position: "+edgePatternIndex.getFilePointer());
			int content = edgePatternIndex.readInt();
			System.out.println("Current content: "+content+", Current file pointer:"+edgePatternIndex.getFilePointer());
			edgePatternIndex.seek(edgePatternIndex.length());
			edgePatternIndex.writeInt(20);
			System.out.println("Current File Position: "+edgePatternIndex.getFilePointer());
			//content = edgePatternIndex.readInt();
			//System.out.println("Current content: "+content+", Current file pointer:"+edgePatternIndex.getFilePointer());
			edgePatternIndex.seek(edgePatternIndex.length());
			System.out.println("Current File Position: "+edgePatternIndex.getFilePointer());
			
			 edgePatternIndex.close();
			

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
