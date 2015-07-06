package util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Random;


public class Util {

	public static long
		kWayMerge (ArrayList<PeekableIterator> itrArray, Comparator comparator, 
				String sFileDir, String sBaseFileName, long lSizeInBytesPerFile) throws IOException
	{
		//while there is an iterator that has next value, keep going.
		
		long lNumFileCreated = 0;
		long lBytesWrittenSoFar = 0;
		FileOutputStream fOutStream = null;
		
		while (hasNext(itrArray))
		{
			
			//get objects from all the iterators. remove empty iterators from the iterator array.
			ArrayList<Object> objectArr = new ArrayList(itrArray.size());
			ArrayList<PeekableIterator> newItrArray = new ArrayList();
			
			int shift = 0;
			int nItrArrayLength = itrArray.size();
			
			for (int i = 0; i < nItrArrayLength; i++)
			{
				
				if (itrArray.get(i).peek() != null)
				{
					objectArr.add(i-shift, itrArray.get(i).peek());
					newItrArray.add(i-shift, itrArray.get(i));
				}
				else
				{
					shift++;
				}
			}
			
			itrArray = newItrArray;
			
			{
				/*find minimum object in the object array and insert it into the buffer. 
				advance the iterator related to the min object.*/
				
				int nMinIndex = 0;
				Object minObject = objectArr.get(0);
				
				for (int i = 1; i < objectArr.size(); i++)
				{
					Object currObject = objectArr.get(i);
					
					if (comparator.compare(minObject, currObject) > 0)
					{
						minObject = currObject;
						nMinIndex = i;
					}
				}

				itrArray.get(nMinIndex).next();
				
				if (fOutStream == null)
				{
					fOutStream = createFileOutputStream(sFileDir, sBaseFileName, ++lNumFileCreated);
				}
				else
				{
					if (lBytesWrittenSoFar + minObject.toString().getBytes().length > lSizeInBytesPerFile)
					{
						fOutStream.flush();
						fOutStream.close();
						
						lBytesWrittenSoFar = 0;
						
						fOutStream = createFileOutputStream(sFileDir, sBaseFileName, ++lNumFileCreated);
					}
				}
				
				lBytesWrittenSoFar += minObject.toString().getBytes().length;
				
				fOutStream.write(minObject.toString().getBytes());
				fOutStream.write('\n');
			}
		}
		
		return lNumFileCreated;
	}

	public static void twoWayReplacementSelectionSort (File inFile, final Comparator<String> comparator, String baseDir) throws Exception
	{
		BufferedReader iStream = null;
		try
		{
			iStream = new BufferedReader(new FileReader(inFile));
			Random randomNumGen = new Random();
			
			final int CONVERGENT_ARRAY_THRESHOLD_SIZE = 2000;
			final int VICTIM_ARRAY_THRESHOLD_SIZE = 20000;
			final int HEAP_SIZE = 20000;
			final char ROUND_NUM_DELIM = ':';
			final int CHUNK_SIZE = 100000000;
			
			ArrayList<String> filesToDeleteArray = new ArrayList<String>();
			
			
			Comparator<String> topHeapComparator = new Comparator<String>(){

				public int compare(String e1, String e2) 
				{
					if (e1 == null)
					{
						return +1;
					}
					else if (e2 == null)
					{
						return -1;
					}
					
					String sE1RunNum = extractRoundNum(e1, ROUND_NUM_DELIM)[0];
					String sE2RunNum = extractRoundNum(e2, ROUND_NUM_DELIM)[0];
					
					if (Long.valueOf(sE1RunNum).longValue() < Long.valueOf(sE2RunNum))
					{
						return -1;
					}
					else if (Long.valueOf(sE1RunNum).longValue() > Long.valueOf(sE2RunNum))
					{
						return +1;
					}
					else
					{
						return -1 * comparator.compare(extractRoundNum(e1, ROUND_NUM_DELIM)[1], 
								extractRoundNum(e2, ROUND_NUM_DELIM)[1]);
					}
				}
				
			};
			
			Comparator<String> bottomHeapComparator = new Comparator<String>(){

				public int compare(String e1, String e2) 
				{
					if (e1 == null)
					{
						return +1;
					}
					else if (e2 == null)
					{
						return -1;
					}
					
					String sE1RunNum = extractRoundNum(e1, ROUND_NUM_DELIM)[0];
					String sE2RunNum = extractRoundNum(e2, ROUND_NUM_DELIM)[0];
					
					if (Long.valueOf(sE1RunNum).longValue() < Long.valueOf(sE2RunNum))
					{
						return -1;
					}
					else if (Long.valueOf(sE1RunNum).longValue() > Long.valueOf(sE2RunNum))
					{
						return +1;
					}
					else
					{
						return comparator.compare(extractRoundNum(e1, ROUND_NUM_DELIM)[1], 
								extractRoundNum(e2, ROUND_NUM_DELIM)[1]);
					}
				}
				
			};
			
			PriorityQueue<String> topHeap = new PriorityQueue<String>(10, topHeapComparator);
			PriorityQueue<String> bottomHeap = new PriorityQueue<String>(10, bottomHeapComparator);
			
			LinkedList<String> convergentListTopHalfOutStream = new LinkedList<String>();
			LinkedList<String> convergentListBottomHalfOutStream = new LinkedList<String>();
			LinkedList<String> topHeapOutStream = new LinkedList<String>();
			LinkedList<String> bottomHeapOutStream = new LinkedList<String>();
			LinkedList<String> runList = new LinkedList<String>();
			
			
			ArrayList<String> convergentArray = new ArrayList<String>(CONVERGENT_ARRAY_THRESHOLD_SIZE);
			ArrayList<String> victimArray = new ArrayList<String>(VICTIM_ARRAY_THRESHOLD_SIZE);
					
			String sTopHeapLValue = null;
			String sBottomHeapHValue = null;
			String sConvergentRangeLValue = null;
			String sConvergentRangeHValue = null;
			
			String sInputLine = null;
			
			long lRoundNum = 0;
			
			for (int i = 0; i < VICTIM_ARRAY_THRESHOLD_SIZE; i++)
			{
				String sLine = iStream.readLine();
				
				if (sLine == null)
				{
					break;
				}
				
				victimArray.add(sLine);
			}
			
			//initialization phase
			while (victimArray.size() > 0)
			{
				//initialization phase. Fill the top and bottom heaps
				{
					Collections.sort(victimArray, comparator);
					
					//fill the top heap
					for (int i = 0; i <= victimArray.size() / 2; i++)
					{
						topHeap.add(lRoundNum + ":" + victimArray.get(i));
					}
					
					for (int i = (victimArray.size() / 2) + 1; i <= victimArray.size() - 1; i ++)
					{
						bottomHeap.add(lRoundNum + ":" + victimArray.get(i));
					}
					
					victimArray.clear();
					
					//remove one element from top or bottom heap and add it to the convergent array.
					
					sTopHeapLValue = extractRoundNum(topHeap.remove(), ROUND_NUM_DELIM)[1];
					convergentArray.add(sTopHeapLValue);

					sBottomHeapHValue = extractRoundNum(bottomHeap.remove(), ROUND_NUM_DELIM)[1]; 
					convergentArray.add(sBottomHeapHValue);
				}
				
				//loop phase
				{
					boolean bHeapFull = false;
					
					long lRunSize = 0;
					
					while (lRunSize <= 1000000 && ! bHeapFull && (sInputLine = iStream.readLine()) != null)
					{
						lRunSize += sInputLine.getBytes().length;
						
						//insert values in one of the convergent array, top heap or bottom heap
						boolean bIsContainedInConvergentRange = sConvergentRangeLValue != null && sConvergentRangeHValue != null &&
								comparator.compare(sInputLine, sConvergentRangeLValue) >= 0 &&  
								comparator.compare(sInputLine, sConvergentRangeHValue) <= 0;
						
						// if value part of convergent array add the value to top heap
						if (bIsContainedInConvergentRange)
						{
							convergentArray.add(sInputLine);
						}
						else
						{
							boolean bIsContainedInTopHeap = comparator.compare(sInputLine, sTopHeapLValue) <= 0;
							boolean bIsContainedInBottomHeap = comparator.compare(sInputLine, sBottomHeapHValue) >= 0;
							
							// enter the value in the right heap
							if (bIsContainedInTopHeap)
							{
								topHeap.add(lRoundNum + ":" + sInputLine);
							}
							else if (bIsContainedInBottomHeap)
							{
								bottomHeap.add(lRoundNum + ":" + sInputLine);
							}
							else
							{
								//randomly insert into a heap which is not full
								
								int heapNum = randomNumGen.nextInt(2);
								
								if (heapNum == 0)
								{
									topHeap.add(lRoundNum +1 + ":" + sInputLine);
								}
								else
								{
									bottomHeap.add(lRoundNum +1 + ":" + sInputLine);
								}
							}
						}
						
						//if convergent array does not have a valid range remove value from the heap and insert it into convergent array.
						
						if (sConvergentRangeHValue == null && sConvergentRangeLValue == null)
						{
							int nHeapNum = randomNumGen.nextInt(2);
							
							if (nHeapNum == 0)
							{
								String sVal = extractRoundNum(topHeap.remove(), ROUND_NUM_DELIM)[1]; 
								sTopHeapLValue = sVal;
								convergentArray.add(sVal);
							}
							else
							{
								String val = extractRoundNum(bottomHeap.remove(), ROUND_NUM_DELIM)[1]; 
								sBottomHeapHValue = val;
								convergentArray.add(val);
							}
						}
						
						else
						{
							//randomly flush one of the two heaps.
							
							int nHeapNum = randomNumGen.nextInt(2);
							String sVal = (nHeapNum == 0) ? topHeap.peek() : bottomHeap.peek();
							
							long lValRunNum = (sVal != null) ? Long.valueOf(extractRoundNum(sVal, ROUND_NUM_DELIM)[0]).longValue() : -1;
							sVal = (sVal != null) ? extractRoundNum(sVal, ROUND_NUM_DELIM)[1] : null;
							
							bIsContainedInConvergentRange = (sVal != null) ? sConvergentRangeLValue != null && sConvergentRangeHValue != null &&
									comparator.compare(sVal, sConvergentRangeLValue) >= 0 &&  
									comparator.compare(sVal, sConvergentRangeHValue) <= 0 : false;
							
							if (lValRunNum != lRoundNum && ! bIsContainedInConvergentRange)
							{
								nHeapNum = 1 - nHeapNum;
								sVal = (nHeapNum == 0) ? topHeap.peek() : bottomHeap.peek();
								lValRunNum = Long.valueOf(extractRoundNum(sVal, ROUND_NUM_DELIM)[0]).longValue();
								sVal = extractRoundNum(sVal, ROUND_NUM_DELIM)[1];
								
								bIsContainedInConvergentRange = sConvergentRangeLValue != null && sConvergentRangeHValue != null &&
										comparator.compare(sVal, sConvergentRangeLValue) >= 0 &&  
										comparator.compare(sVal, sConvergentRangeHValue) <= 0;
							}
							
							if (lValRunNum == lRoundNum || bIsContainedInConvergentRange)
							{
								sVal = (nHeapNum == 0) ? topHeap.remove() : bottomHeap.remove();
								sVal = extractRoundNum(sVal, ROUND_NUM_DELIM)[1];
								
								if (bIsContainedInConvergentRange)
								{
									convergentArray.add(sVal);
								}
								else if (nHeapNum == 0)
								{
									topHeapOutStream.add(0,sVal);
									sTopHeapLValue = sVal;
								}
								else
								{
									bottomHeapOutStream.add(sVal);
									sBottomHeapHValue = sVal;
								}
							}
						}
						
						bHeapFull = (topHeap.size() + bottomHeap.size()) == HEAP_SIZE;
						
						//flush bottom heap if it is full
						
						if (convergentArray.size() == CONVERGENT_ARRAY_THRESHOLD_SIZE)
						{
							flushConvergentArray(randomNumGen, CONVERGENT_ARRAY_THRESHOLD_SIZE, convergentArray, 
									convergentListTopHalfOutStream, convergentListBottomHalfOutStream, comparator);
							convergentArray.clear();
							sConvergentRangeLValue  = convergentListTopHalfOutStream.get(convergentListTopHalfOutStream.size() - 1);
							sConvergentRangeHValue  = convergentListTopHalfOutStream.get(0);
						}
					}
				
				}
				
				//aggregate phase
				{
					flushConvergentArray(randomNumGen, CONVERGENT_ARRAY_THRESHOLD_SIZE, convergentArray, 
							convergentListTopHalfOutStream, convergentListBottomHalfOutStream, comparator);
					
					convergentArray.clear();
					
					//aggregate heap and convergent output streams
					if (topHeapOutStream != null)
					{
						isSorted(topHeapOutStream, comparator);
						runList.addAll(topHeapOutStream);
					}
					
					if (convergentListTopHalfOutStream != null)
					{
						isSorted(convergentListTopHalfOutStream, comparator);
						runList.addAll(convergentListTopHalfOutStream);
					}
					
					if (convergentListBottomHalfOutStream != null)
					{
						isSorted(convergentListBottomHalfOutStream, comparator);
						runList.addAll(convergentListBottomHalfOutStream);
					}
					
					if (bottomHeapOutStream != null)
					{
						isSorted(bottomHeapOutStream, comparator);
						runList.addAll(bottomHeapOutStream);
					}
					
					//add the run to the file
					
					
					File file = new File(baseDir + lRoundNum);
					
					filesToDeleteArray.add(baseDir + lRoundNum);
					
					FileOutputStream runOutputStream = new FileOutputStream(file);
					
					for(String sElement: runList)
					{
						runOutputStream.write(sElement.getBytes());
						runOutputStream.write('\n');
					}
					
					runOutputStream.close();
					
					
					//empty the heaps into the victim array
					for (String sElement : topHeap)
					{
						victimArray.add(extractRoundNum(sElement, ':')[1]);
					}
					
					for (String sElement : bottomHeap)
					{
						victimArray.add(extractRoundNum(sElement, ':')[1]);
					}
					
					//cleanup 
					topHeap.clear();
					bottomHeap.clear();
					topHeapOutStream.clear();
					bottomHeapOutStream.clear();
					convergentArray.clear();
					convergentListTopHalfOutStream.clear();
					convergentListBottomHalfOutStream.clear();
					runList.clear();
					sConvergentRangeHValue = null;
					sConvergentRangeLValue = null;
				}
				
				lRoundNum++;
				
				//if end of file reached empty victim array into a file and break.
				if (sInputLine == null)
				{
					File file = new File(baseDir+ lRoundNum);
					
					filesToDeleteArray.add(baseDir + lRoundNum);
					
					FileOutputStream runOutputStream = new FileOutputStream(file);
					
					Collections.sort(victimArray, comparator);
					
					for(String sElement: victimArray)
					{
						runOutputStream.write(sElement.getBytes());
						runOutputStream.write('\n');
					}
					
					runOutputStream.close();
					
					break;
				}
			}
			
			//merge phase
			long lTotalFileNum = lRoundNum + 1; 
			long maxBatchSize = 3;
			final long MERGE_CHUNK_SIZE = 100000000;
			long lMergeRoundNum = 0;
			
			String sBaseFileName = "MERGE_";
			ArrayList<String> fileNameArray = new ArrayList<String>();
			
			for (int i = 0; i < lTotalFileNum; i++)
			{
				fileNameArray.add(baseDir + i);
			}
			
			while (fileNameArray.size() > 1)
			{
				// remove batch size of files from fileNameArray
				ArrayList<String> fileNamesForMergeArr = new ArrayList<String>();
				
				long lOffSet = 0;
				
				while (fileNameArray.size() > 0 && lOffSet < maxBatchSize)
				{
					fileNamesForMergeArr.add(fileNameArray.remove(0));
					lOffSet++;
				}
				
				ArrayList<PeekableIterator> fileIterArr = new ArrayList<PeekableIterator>();
				
				for (int i = 0; i < fileNamesForMergeArr.size(); i++)
				{
					fileIterArr.add(new KVPairFileIterator(fileNamesForMergeArr.get(i)));
				}
				
				kWayMerge(fileIterArr, comparator, baseDir, sBaseFileName + lMergeRoundNum, MERGE_CHUNK_SIZE);
				
				String sMergedResultFileName = baseDir + sBaseFileName + lMergeRoundNum + "1";
				File file = new File (sMergedResultFileName);
				
				if (! file.exists())
				{
					throw new FileNotFoundException("");
				}
				
				filesToDeleteArray.add(sMergedResultFileName);
				
				fileNameArray.add(sMergedResultFileName);
				
				for (PeekableIterator itr : fileIterArr)
				{
					((KVPairFileIterator) itr).close();
				}
								
				lMergeRoundNum++;
				
			}
			
			//cleanup phase. Delete all the files created by the sort procedure.
			filesToDeleteArray.remove(filesToDeleteArray.size() - 1);
			
			while (filesToDeleteArray.size() > 0)
			{
				(new File(filesToDeleteArray.remove(0))).delete();
			}
		}
		finally
		{
			if (iStream != null)
			{
				iStream.close();
			}
		}
	}
	
	protected static void isSorted(Iterable<String> iterable, Comparator<String> comparator)
	{
		Iterator<String> itr = iterable.iterator();
		
		String s1 = (itr.hasNext()) ? itr.next() : null;
		String s2 = null;
		
		while (itr.hasNext())
		{
			s2 = itr.next();
			
			int result = comparator.compare(s1, s2);
			
			if (result > 0)
			{
				System.out.println("Not sorted");
			}
			
			s1 = s2;
		}
	}
	
	protected static void flushConvergentArray(Random randomNumGen, 
			final int CONVERGENT_ARRAY_THRESHOLD_SIZE, ArrayList<String> convergentArray, 
			LinkedList<String> convergentListTopHalfOutStream, LinkedList<String> convergentListBottomHalfOutStream, Comparator<String> comparator)
	{
		int convergentArraySplitRangeLowerIndex = (convergentArray.size() >= CONVERGENT_ARRAY_THRESHOLD_SIZE) ? randomNumGen.nextInt(convergentArray.size() - 2) : 0;
		
		Collections.sort(convergentArray, comparator);
		
		for (int i = 0; i <= convergentArraySplitRangeLowerIndex && i < convergentArray.size(); i++)
		{
			if (convergentArray.get(i) != null)
			{
				convergentListTopHalfOutStream.add(convergentArray.get(i));
			}
		}
		
		for (int i = convergentArraySplitRangeLowerIndex+1, j = 0; i < convergentArray.size(); i++, j++)
		{
			if (convergentArray.get(i) != null)
			{
				convergentListBottomHalfOutStream.add(j, convergentArray.get(i));
			}
		}
	}
	
	protected static void flushHeap (PriorityQueue<String> heap, LinkedList<String> heapOutList, 
			long lCurrRoundNum, char roundNumDelim, boolean bPrepend)
	{
		String sNextElement = null;
		
		while ((sNextElement = heap.poll() ) != null)
		{
			String[] sElemInfo = extractRoundNum(sNextElement, roundNumDelim);
			
			if (lCurrRoundNum == Long.valueOf(sElemInfo[0]).longValue())
			{
				
				if (bPrepend)
				{
					heapOutList.add(0,sElemInfo[1]);
				}
				else
				{
					heapOutList.add(sElemInfo[1]);
				}
			}
			else
			{
				break;
			}
		}
		
		if (sNextElement != null)
		{
			heap.add(sNextElement);
		}
	}
	
	protected static String[] extractRoundNum (String sElement, char roundNumDelim)
	{
		int nIndex = sElement.indexOf(roundNumDelim, 0);
		return new String[]{sElement.substring(0, nIndex), sElement.substring(nIndex + 1)};
		
	}
	
	
	protected static boolean hasNext(ArrayList<PeekableIterator> itrArray) 
	{
		for (PeekableIterator itr: itrArray)
		{
			if (itr.hasNext()) return true;
		}
		
		return false;
	}
	
	protected static FileOutputStream createFileOutputStream(String sFileDir, String sBaseFileName, long lFileNum) throws FileNotFoundException
	{
		String sFileName = sFileDir + sBaseFileName + lFileNum;
		
		File outFile = new File(sFileName);
		
		return new FileOutputStream(outFile);
	}
	
}
