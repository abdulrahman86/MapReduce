package test.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;

import util.KVPairFileIterator;

public class TestKVPairFileIterator {

	public static void main(String[] args) throws IOException, InterruptedException
	{
//		KVPairFileIterator itr = 
//				new KVPairFileIterator("/Users/abdullahsattar/eclipse/workspace/MapReduce/src/test/util/file1");
//		
//		long previous = -1;
//		boolean bSorted = true;
//		
//		long lBytesRead = 0;
//		
//		while (itr.hasNext() && bSorted)
//		{
//			long next = new Long(itr.next().toString()).longValue();
//			
//			bSorted = previous < next;
//			
//			previous = next;
//			
//			lBytesRead += (previous+"").getBytes().length;
//			
//			System.out.println(lBytesRead);
//		}
//		for (int i = 0; i < 26; i++)
//		{
//			boolean bSorted = checkSortOrder(new File("/Users/abdullahsattar/eclipse/workspace/MapReduce/src/test/util/"+i));
//			
//			System.out.println(i + " sort:"+bSorted);
//			
//		}
		
		System.out.println(checkSortOrder());
		//generateLargeFile();
	}
	
	public static boolean checkSortOrder() throws IOException
	{
		return checkSortOrder(new File("/Users/abdullahsattar/eclipse/workspace/MapReduce/src/test/util/MERGE_1241"));
	}
	
	public static boolean checkSortOrder(File file) throws IOException
	{
		BufferedReader reader = new BufferedReader(new FileReader(file));
		
		long previous = -1;
		boolean bSorted = true;
		
		long lBytesRead = 0;
		long linesRead = 0;
		
		while (bSorted)
		{
			String line = reader.readLine();
			
			if (line == null) break;
			
			long next = new Long(line).longValue();
			
			bSorted = previous <= next;
						
			previous = next;
			
			lBytesRead += (previous+"").getBytes().length;
			
			//System.out.println(linesRead++);
		}
		
		return bSorted;
	}
	
	public static void generateLargeFile () throws IOException
	{
				FileOutputStream out = new FileOutputStream("/Users/abdullahsattar/eclipse/workspace/MapReduce/src/test/util/file2");
				
				long lBytesWritten = 0;
				long i = 1388889;
				
				while (lBytesWritten < 10000000 )
				{
					String line = i + "\n";
					
					lBytesWritten += line.getBytes().length;
					
					i++;
					
					out.write(line.getBytes());
					
					System.out.println(lBytesWritten+":"+i);
				}
				
				out.close();
	}
}
