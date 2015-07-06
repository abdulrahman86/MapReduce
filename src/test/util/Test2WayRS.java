package test.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.Random;

import util.Util;

public class Test2WayRS {

	public static void main(String[] args) throws Exception 
	{
//		generateFile();
		long time = System.currentTimeMillis();
		test2WayRS();
		
		System.out.println("Time: " + (System.currentTimeMillis() - time));
	}
	
	public static void test2WayRS() throws Exception
	{
		Util.twoWayReplacementSelectionSort(new File("/Users/abdullahsattar/eclipse/workspace/MapReduce/src/test/util/file4"), 
				new Comparator<String> (){

					public int compare(String o1, String o2) {
						return Integer.valueOf(o1).compareTo(Integer.valueOf(o2));
					}
			
		}, 
		"/Users/abdullahsattar/eclipse/workspace/MapReduce/src/test/util/");
	}
	
	public static void generateFile () throws IOException
	{
		Random random = new Random();
		FileOutputStream fOutStream = 
				new FileOutputStream(new File("/Users/abdullahsattar/eclipse/workspace/MapReduce/src/test/util/file4"));
		
		for (int i = 0; i < 10000000; i++)
		{
			int next = random.nextInt(10000);
			System.out.println(i);
			fOutStream.write(new String(""+ next).getBytes());
			fOutStream.write('\n');
		}
		
		fOutStream.close();
	}

}
