package mapreduce.master;

import java.util.HashSet;
import java.util.LinkedList;

import mapreduce.api.MapReduceJob;
import mapreduce.api.MapReduceNodeTaskRequest;
import mapreduce.api.NodeMessageHandler;
import mapreduce.api.NodePacketBase;


public class MapReduceRequestHandler extends Thread implements NodeMessageHandler
{
	protected LinkedList<NodePacketBase> m_nodePacketsList;
	protected boolean m_bShutdown;
	protected HashSet<String> m_activeNodeNameSet;
	protected MapReduceJob m_mapReduceJob;
	
	protected static MapReduceRequestHandler m_instance;
	
	protected MapReduceRequestHandler()
	{
		m_nodePacketsList = new LinkedList<NodePacketBase>();
		m_activeNodeNameSet = new HashSet<String>();
		m_bShutdown = false;
		m_mapReduceJob = null;
	}
	
	protected synchronized MapReduceRequestHandler getInstance()
	{
		if (m_instance == null)
		{
			m_instance = new MapReduceRequestHandler();
		}
		
		return m_instance;
	}
	
	public void run()
	{		
		//TODO: add as a listener to listen for node add and remove events
		
		while (! m_bShutdown)
		{
			
			NodePacketBase nodePacket= null;
			
			//wait for packet from the node
			synchronized (m_nodePacketsList)
			{
				nodePacket = m_nodePacketsList.getFirst();
				
				if (nodePacket == null)
				{
					try 
					{
						m_nodePacketsList.wait();
					} 
					catch (InterruptedException e) 
					{
						continue;
					}
				}
				
				nodePacket = m_nodePacketsList.removeFirst();
			}
			
			//delegate task to the right node packet handler
			if (nodePacket instanceof MapReduceNodeTaskRequest)
			{
				handleMapReduceRequest(nodePacket);
			}
			
			//send task to the node and mark the task being delegated to the node.
		}
	}
	
	public void shutdown() throws InterruptedException
	{
		synchronized (m_instance)
		{
			if (! m_bShutdown)
			{
				m_bShutdown = true;
				wait();
			}
		}
		
		System.out.println("MapReduceRequestHandler shutting down");
	}
	
	public class MapReduceJobHanlder extends Thread
	{
		
		
		
	}

	public void handleMapReduceRequest(NodePacketBase nodePacket) 
	{
		synchronized (m_activeNodeNameSet)
		{
			if (m_activeNodeNameSet.contains(nodePacket.getNodeName()))
			{
				if (m_mapReduceJob != null)
				{
					//get task from map reduce job and send it as a datagram packet
				}
				else
				{
					//initialize map reduce job if there is a request in the queue.
				}
			}
		}
	}

	public void handle(NodePacketBase nodePacket) 
	{
		synchronized (m_nodePacketsList)
		{
			m_nodePacketsList.add(nodePacket);
			m_nodePacketsList.notify();
		}
	}
	
	
}
