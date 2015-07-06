package util;

import java.util.ArrayList;


public class CircularBuffer<K> {

	protected ArrayList<K> m_queue;
	protected int m_nHeadPos;
	protected int  m_nTailPos;
	protected int m_nCapacity;
	protected int m_nFillCount;
	
	public CircularBuffer (int nCapacity, K[] initValArr) throws InterruptedException
	{
		m_queue = new ArrayList<K>(nCapacity);
		m_nCapacity = nCapacity;
		m_nFillCount = 0;
		m_nHeadPos = 0;
		m_nTailPos = 0;
		
		if (initValArr != null)
		{
			for (int i = 0; i < initValArr.length; i++)
			{
				if (initValArr[i] != null)
				{
					add(initValArr[i]);
				}
			}
		}
	}
	
	public synchronized int getFreeSlotsCount()
	{
		return m_nCapacity - m_nFillCount;
	}
	
	public synchronized int getFillCount()
	{
		return m_nFillCount;
	}
	
	public int getSlotsCount()
	{
		return m_nCapacity;
	}
	

	public synchronized void add(K element) throws InterruptedException
	{
		lock:
			for (;;)
			{
				if (m_nCapacity - m_nFillCount > 0)
				{
					m_queue.add(m_nTailPos, element);
					m_nTailPos = (m_nTailPos+1) % m_nCapacity;
					m_nFillCount++;
					
					break lock;
				}
				else
				{
					wait(5);
					
					continue lock;
				}
				
			}
	}
	
	public synchronized K getNext()
	{
		K nextElement = null;
		
		if (m_nFillCount > 0)
		{
			nextElement = m_queue.get(m_nHeadPos);
			m_nHeadPos = (m_nHeadPos+1) % m_nCapacity;
			m_nFillCount--;
		}
		
		notifyAll();
		
		return nextElement;
	}
	
	public synchronized K peek()
	{
		K nextElement = null;
		
		if (m_nFillCount > 0)
		{
			nextElement = m_queue.get(m_nHeadPos);
		}

		return nextElement;
	}
}
