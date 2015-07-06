package mapreduce.api;

import java.io.Serializable;


public class NodePacketBase implements Serializable

{
	private static final long serialVersionUID = 4399194399345309657L;
	protected boolean m_bFree;
	protected long m_lHeartBeatTimeInMillis;
	protected String m_sNodeName;
	protected long m_lHeartBeatSID;
	protected int m_nNodeHeartBeatPort;
	protected int m_nMapReduceTaskPort;
	protected boolean m_bRejoin;
	protected long m_lWorkerNodeNonce;
	
	
	public NodePacketBase()
	{
		
	}
	
	public long getWorkerNodeNonce()
	{
		return m_lWorkerNodeNonce;
	}
	
	public void setWorkerNodeNonce(long lWorkerNodeNonce)
	{
		m_lWorkerNodeNonce= lWorkerNodeNonce;
	}
	
	public void setMapReduceTaskPort(int port)
	{
		m_nMapReduceTaskPort = port;
	}
	
	public int getMapReduceTaskPort(int port)
	{
		return m_nMapReduceTaskPort;
	}
	
	public boolean isRejoin() 
	{
		return m_bRejoin;
	}


	public void setRejoin(boolean bRejoin) 
	{
		m_bRejoin = bRejoin;
	}


	public boolean isFree() 
	{
		return m_bFree;
	}

	public void setFree(boolean bFree) 
	{
		m_bFree = bFree;
	}

	public long getHeartBeatTimeInMillis() 
	{
		return m_lHeartBeatTimeInMillis;
	}

	public void setTime(long lAckNumber) 
	{
		m_lHeartBeatTimeInMillis = lAckNumber;
	}
	
	public void setNodeName(String sNodeName)
	{
		m_sNodeName = sNodeName; 
	}
	
	public String getNodeName()
	{
		return m_sNodeName;
	}
	
	public long getHeartBeatSID()
	{
		return m_lHeartBeatSID;
	}
	
	public void setHeartBeatSID(long lHeartBeatSID)
	{
		m_lHeartBeatSID = lHeartBeatSID;
	}
	
	public void setNodeHeartBeatPort(int nHeartBeatPort)
	{
		m_nNodeHeartBeatPort = nHeartBeatPort;
	}
	
	public int getNodeHeartBeatport()
	{
		return m_nNodeHeartBeatPort;
	}	
}
