package mapreduce.api;

import java.io.Serializable;

public class MapReduceTask implements Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = -6271397966526689734L;
	
	private String m_sFileLoc;
	private Long m_lTaskId;
	private long m_lStartByteOffSet;
	private long m_lEndByteOffSet;
	
	
	public Long getTaskId() 
	{
		return m_lTaskId;
	}

	public void setTaskId(Long lTaskId) 
	{
		m_lTaskId = lTaskId;
	}

	public String getFileLoc() 
	{
		return m_sFileLoc;
	}
	
	public void setFileLoc(String sFileLoc) 
	{
		m_sFileLoc = sFileLoc;
	}

	public long getStartByteOffSet() 
	{
		return m_lStartByteOffSet;
	}

	public void setStartByteOffSet(long lStartByteOffSet) 
	{
		m_lStartByteOffSet = lStartByteOffSet;
	}

	public long getEndOByteffSet() 
	{
		return m_lEndByteOffSet;
	}

	public void setEndByteOffSet(long lEndOByteffSet) 
	{
		m_lEndByteOffSet = lEndOByteffSet;
	}
	
	
	
}
