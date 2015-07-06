package mapreduce.api;

import java.io.Serializable;
import java.util.Vector;

public class MapReduceNodeTaskRequest extends NodePacketBase
{
	private static final long serialVersionUID = 8630421024964149098L;
	protected String m_sNodeName;
	protected String m_sSID;
	protected Vector<String> m_sPreviousJobIdsList;
	
	public String getSID() 
	{
		return m_sSID;
	}

	public void setSID(String sSID) 
	{
		m_sSID = sSID;
	}

	public String getNodeName() 
	{
		return m_sNodeName;
	}

	public void setM_sNodeName(String sNodeName) 
	{
		this.m_sNodeName = sNodeName;
	}
	
	public void addPreviousJob(String sJobId)
	{
		if (m_sPreviousJobIdsList == null)
		{
			m_sPreviousJobIdsList = new Vector<String> ();
		}
		
		m_sPreviousJobIdsList.add(sJobId);
	}
	
	
}
