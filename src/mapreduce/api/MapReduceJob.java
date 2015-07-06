package mapreduce.api;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.PriorityQueue;

public class MapReduceJob 
{
	protected enum State
	{
		MAP_PHASE, REDUCE_PHASE, DONE
	};
	
	protected State m_jobState;
	protected PriorityQueue<MapReduceTaskWrapper> m_taskNodeCountPriorityQueue;
	protected LinkedList<MapReduceTaskWrapper> m_tasksToBeScheduledList;
	protected HashMap<String, LinkedList<MapReduceTaskWrapper>> m_taskListByNodeNameMap;
	protected boolean m_bMapTaskCreationInProgress;
	protected long m_lTaskUID = 0 ;
	
	protected final int MAP_TASK_CHUNK_SIZE_BYTES = 1 * 1000 * 1000;
	
	public MapReduceJob(final MapReduceRequest mapReduceRequest) throws IOException
	{
		//set job state
		m_jobState = State.MAP_PHASE;
		
		//initialize data structures.
		m_tasksToBeScheduledList = new LinkedList<MapReduceTaskWrapper>();
		m_taskListByNodeNameMap = new HashMap<String, LinkedList<MapReduceTaskWrapper>>();
		m_taskNodeCountPriorityQueue = new PriorityQueue<MapReduceTaskWrapper>(10, new Comparator<MapReduceTaskWrapper>()
		{

			public int compare(MapReduceTaskWrapper o1, MapReduceTaskWrapper o2) 
			{
				if (o1.getNodeCount() != o2.getNodeCount())
				{
					return o1.getNodeCount().compareTo(o2.getNodeCount());
				}
				else
				{
					return o1.getTaskId().compareTo(o2.getTaskId());
				}
			}
			
		});
		
		m_bMapTaskCreationInProgress = true;
		
		//initialize map tasks in a background thread
		
		new Thread()
		{
			
			public void run()
			{
				File file = new File(mapReduceRequest.getFileName());
				RandomAccessFile randFile = null;
				try 
				{
					long lFileSize = file.length();
					
					randFile = new RandomAccessFile(file, "r");
					while (randFile.getFilePointer()  <= lFileSize - 1)
					{
						if (randFile.getFilePointer() + 2 * MAP_TASK_CHUNK_SIZE_BYTES >= lFileSize)
						{
							MapReduceTaskWrapper mapReduceTask = new MapReduceTaskWrapper();
							mapReduceTask.setFileLoc(mapReduceRequest.getFileName());
							mapReduceTask.setStartByteOffSet(randFile.getFilePointer());
							mapReduceTask.setEndByteOffSet(lFileSize - 1);
							mapReduceTask.setTaskId(m_lTaskUID++);
							mapReduceTask.setNodeCount(0);
							
							synchronized (m_tasksToBeScheduledList)
							{
								m_tasksToBeScheduledList.add(mapReduceTask);
								notify();
							}
						}
						
						else
						{
							//adjust offset to next whitespace character
							long lStartOffSet = randFile.getFilePointer();
							
							long lCurrPointer = 0;
							
							randFile.seek(randFile.getFilePointer() + MAP_TASK_CHUNK_SIZE_BYTES + 1);
							
							//read until white space. 
							while (randFile.getFilePointer() <= lFileSize - 1)
							{
								lCurrPointer = randFile.getFilePointer();
								
								char c = randFile.readChar();
								
								if (Character.isWhitespace(c))
								{
									break;
								}
							}
							
							MapReduceTaskWrapper mapReduceTask = new MapReduceTaskWrapper();
							mapReduceTask.setFileLoc(mapReduceRequest.getFileName());
							mapReduceTask.setStartByteOffSet(lStartOffSet);
							mapReduceTask.setEndByteOffSet(lCurrPointer);
							mapReduceTask.setTaskId(m_lTaskUID++);
							mapReduceTask.setNodeCount(0);
							
							synchronized (m_tasksToBeScheduledList)
							{
								m_tasksToBeScheduledList.add(mapReduceTask);
								notify();
							}
						}
					}
				} 
				catch (FileNotFoundException e) 
				{
					//TODO: record in a log file that the map reduce task was not completed successfully and cleanup
				} 
				catch (IOException e) 
				{
					//TODO: record in a log file that the map reduce task was not completed sucessfully and cleanup
				}
				finally
				{
					try 
					{
						if (randFile != null)
						{
							randFile.close();
						}
						
						synchronized(m_tasksToBeScheduledList)
						{
							m_bMapTaskCreationInProgress = false;
							m_tasksToBeScheduledList.notify();
						}
					} 
					catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}.start();
		
		
	}
	
	public MapReduceTask allocateTaskToNode(String sNodeName) throws InterruptedException
	{
		MapReduceTaskWrapper task = null;
		
				
		while (task == null)
		{
			synchronized(m_tasksToBeScheduledList)
			{
				if (m_bMapTaskCreationInProgress == true)
				{
					task = m_tasksToBeScheduledList.getFirst();
					
					if (task == null)
					{
						m_tasksToBeScheduledList.wait();
						continue;
					}
					
					task = m_tasksToBeScheduledList.removeFirst();
				}
				else
				{
					break;
				}
			}
		}
		
		if (task == null)
		{
			task = m_taskNodeCountPriorityQueue.remove();
		}
		
		if (task != null)
		{
			LinkedList<MapReduceTaskWrapper> nodeTasksList = m_taskListByNodeNameMap.get(sNodeName);
			
			if (nodeTasksList == null)
			{
				nodeTasksList = new LinkedList<MapReduceTaskWrapper>();
			}
			
			nodeTasksList.add(task);
			m_taskListByNodeNameMap.put(sNodeName, nodeTasksList);
			
			task.incrementNodeCount();
			
			m_taskNodeCountPriorityQueue.add(task);
		}
		
		return task;
	}
	
	public void deallocateNode(String sNodeName)
	{
		LinkedList<MapReduceTaskWrapper> nodeTaskList = m_taskListByNodeNameMap.get(sNodeName);
		
		HashSet<MapReduceTaskWrapper> nodeTaskSet = new HashSet<MapReduceTaskWrapper>();
		
		if (nodeTaskList != null)
		{
			for (MapReduceTaskWrapper task: nodeTaskList)
			{
				if(! nodeTaskSet.contains(task))
				{
					m_taskNodeCountPriorityQueue.remove(task);
					task.decrementNodeCount();
					nodeTaskSet.add(task);
				}
				
				task.decrementNodeCount();
			}
		}
		
		nodeTaskList.clear();
		m_taskListByNodeNameMap.remove(sNodeName);
		
		//adjust the count for the tasks and readd them to the priority queue.
		for (MapReduceTaskWrapper task: nodeTaskSet)
		{
			m_taskNodeCountPriorityQueue.add(task);
		}
	}
	
	protected class MapReduceTaskWrapper extends MapReduceTask
	{
		protected Integer m_nNodeCount;
				
		public boolean equals(Object obj) 
		{
	        if (obj != null)
	        {
	        	return getTaskId() == ((MapReduceTaskWrapper) obj).getTaskId();
	        }
	        
	        return false;
	    }
				
		
		public Integer getNodeCount()
		{
			return m_nNodeCount;
		}
		
		public void setNodeCount(int nNodeCount)
		{
			m_nNodeCount = nNodeCount;
		}
		
		public void incrementNodeCount()
		{
			m_nNodeCount++;
		}
		
		public void decrementNodeCount()
		{
			m_nNodeCount--;
		}
	}
}
