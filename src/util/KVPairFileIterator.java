package util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class KVPairFileIterator implements PeekableIterator
{
	protected BufferedReader m_fileIStream;
	protected String m_sNextPair;
	
	public KVPairFileIterator (String sFileName) throws IOException, InterruptedException
	{
		File file = new File(sFileName);
		m_fileIStream = new BufferedReader(new FileReader(file));
		m_sNextPair = m_fileIStream.readLine();
	}

	public boolean hasNext() 
	{
		return m_sNextPair != null;
	}

	public Object next() 
	{		
		String sPrevPair = m_sNextPair;
		
		try 
		{
			m_sNextPair = m_fileIStream.readLine();
		} 
		catch (IOException e) {
			m_sNextPair = null;
		}
		
		return sPrevPair;
	}

	public void remove() 
	{
		throw new UnsupportedOperationException();
	}

	public Object peek() 
	{
		return m_sNextPair;
	}
	
	public void close() throws IOException
	{
		if (m_fileIStream != null)
		{
			m_fileIStream.close();
		}
	}
}

