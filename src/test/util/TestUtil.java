package test.util;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

import util.KVPairFileIterator;
import util.PeekableIterator;
import util.Util;

public class TestUtil {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		
		PeekableIterator arrItr1 = 
				new KVPairFileIterator("/Users/abdullahsattar/eclipse/workspace/MapReduce/src/test/util/file1"); 
		
		PeekableIterator arrItr2 = 
				new KVPairFileIterator("/Users/abdullahsattar/eclipse/workspace/MapReduce/src/test/util/file2"); 
		
		
		ArrayList<PeekableIterator> arrList = new ArrayList<PeekableIterator>();
		
		arrList.add(arrItr1);
		arrList.add(arrItr2);
		
		PipedInputStream iStream = new PipedInputStream();
		
		Util.kWayMerge(arrList, 
				new DefaultComparator() 
				{
					public int compare(Object o1, Object o2)
					{
						return (new Long(o1.toString())).compareTo(new Long(o2.toString()));
					}
			
				}, 
				"/Users/abdullahsattar/eclipse/workspace/MapReduce/src/test/util/", "sort", 1000000);
	}

	public static abstract class DefaultComparator implements Comparator
	{

		
	}
}
