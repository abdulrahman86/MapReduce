package mapreduce.master;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;

import mapreduce.api.NodePacketBase;

public class ClusterManager extends Thread
{
	protected static ClusterManager m_instance;
	protected boolean m_bShutdown;
	protected HashMap<String, NodePacketBase> m_nodeInfoByNodeNameMap;
	protected HashMap<String,Long> m_nodeSIDByNodeNameMap;
	protected Vector<NodeStatusListener> m_nodeStatusListenersList;
	//sorted array wrt time of all the node infos/
	protected ArrayList<NodePacketBase> m_nodeInfoArray;
	protected final Object m_NodeInfoUpdateLock = new Object();
	//timer interval for monitoring heartbeat is 20 seconds
	protected static final long TIMER_INTERVAL_MILLISECONDS = 20 * 1000;
	
	protected ClusterManager()
	{
		m_bShutdown = false;
		m_nodeInfoByNodeNameMap = new HashMap<String, NodePacketBase>();
		m_nodeSIDByNodeNameMap = new HashMap<String, Long>();
		m_nodeInfoArray = new ArrayList<NodePacketBase>();
		m_nodeStatusListenersList = new Vector<NodeStatusListener>();
	}
	
	public void addNodeStatusListener(NodeStatusListener nodeStatusListener)
	{
		synchronized (m_nodeStatusListenersList)
		{
			if (! m_nodeStatusListenersList.contains(nodeStatusListener))
			{
				m_nodeStatusListenersList.addElement(nodeStatusListener);
			}
		}
	}
	
	public void removeNodeStatusListener(NodeStatusListener nodeStatusListener)
	{
		synchronized (m_nodeStatusListenersList)
		{
			
			m_nodeStatusListenersList.removeElement(nodeStatusListener);
		}
	}
	
	
	public void run()
	{
		System.out.println("WorkerHealthMonitor starting up");
		
		boolean bException = false;
		DatagramSocket serverSocket = null;
		DatagramSocket workerNodeSocket = null;
		byte[] receiveData = null;
		DatagramPacket receivePacket = null;
		Timer heartBeatTimer = new Timer("HEART_BEAT_TIMER");
		
		try 
		{
			serverSocket = new DatagramSocket(9812);
			
			workerNodeSocket = new DatagramSocket();
			
			heartBeatTimer.schedule(new HeartBeatTimerTask(), 
					2 * TIMER_INTERVAL_MILLISECONDS, TIMER_INTERVAL_MILLISECONDS);
			
			receiveData = new byte[1024];
			receivePacket = new DatagramPacket(receiveData, receiveData.length);
			
			
		} 
		catch (SocketException e) 
		{
			bException = true;
		}
		
		//streams for receiving incoming node info packet
		ByteArrayInputStream byteIStream = null;
		ObjectInputStream objectIStream = null;
		
		//streams for sending out outgoing node info packet
		ByteArrayOutputStream byteOutStream = null;
		ObjectOutputStream objOutStream = null;
		
		//loop forever until shutdown or exception
		while (! m_bShutdown && ! bException)
		{
			
			try 
			{
				serverSocket.receive(receivePacket);
				byteIStream = new ByteArrayInputStream(receivePacket.getData());
				objectIStream = new ObjectInputStream(byteIStream);
					
				try 
				{
					NodePacketBase nodeInfo = (NodePacketBase) objectIStream.readObject();
					
					//update node info
					synchronized (m_NodeInfoUpdateLock)
					{
						NodePacketBase nodeInfoFromMap = m_nodeInfoByNodeNameMap.get(nodeInfo.getNodeName());
						
						//check if the packet is not an old packet
						if (nodeInfoFromMap != null && 
								m_nodeSIDByNodeNameMap.get(nodeInfo.getNodeName()).longValue() == nodeInfo.getHeartBeatSID())
						{
							System.out.println("Node " + nodeInfo.getNodeName() + " heartbeat.");
							//remove node info from array and add it to the end of the array as the most recent node info received
							{
								int nodeInfoArrayIndex = 0;
								for (NodePacketBase nodeInfoFromArray : m_nodeInfoArray)
								{
									
									if (nodeInfoFromArray.getNodeName().equals(nodeInfo.getNodeName()))
									{
										break;
									}
									
									nodeInfoArrayIndex++;
								}
								
								m_nodeInfoArray.remove(nodeInfoArrayIndex);
							}
							
							//add node info to the map
							nodeInfo.setTime(System.currentTimeMillis());
							m_nodeInfoByNodeNameMap.put(nodeInfoFromMap.getNodeName(), nodeInfo);
							m_nodeInfoArray.add(nodeInfo);
							
							//handle specific node info packets
						}
						//remove node and ask node to rejoin
						else
						{
							if (nodeInfoFromMap != null)
							{
								removeNode(nodeInfoFromMap);
							}
							//resue the output streams if already initialized
							if (byteOutStream == null)
							{
								byteOutStream = new ByteArrayOutputStream();
							}
							else
							{
								byteOutStream.reset();
							}
							
							objOutStream = new ObjectOutputStream(byteOutStream);


							System.out.println("Node:" + nodeInfo.getNodeName() + " joined.");
							
							if (nodeInfo.getHeartBeatSID() > 0 )
							{
								nodeInfo.setRejoin(true);
							}
							
							long lNodeHeartBeatSID = System.nanoTime();
							
							nodeInfo.setHeartBeatSID(lNodeHeartBeatSID);
							m_nodeInfoByNodeNameMap.put(nodeInfo.getNodeName(), nodeInfo);
							m_nodeSIDByNodeNameMap.put(nodeInfo.getNodeName(), lNodeHeartBeatSID);
							nodeInfo.setTime(System.currentTimeMillis());
							
								
							m_nodeInfoArray.add(nodeInfo);
														
							objOutStream.writeObject(nodeInfo);
							
							objOutStream.flush();
							
							byte[] nodeInfoObjByteArr = byteOutStream.toByteArray();
							//send node heartbeat session id
							workerNodeSocket.send(new DatagramPacket(nodeInfoObjByteArr, 
									nodeInfoObjByteArr.length, InetAddress.getByName("localhost"), nodeInfo.getNodeHeartBeatport()));
							
						}
					}
				} 
				catch (ClassNotFoundException e) 
				{
					//dont do anything.
				}
				finally
				{
					for (int i = 0; i < receiveData.length; i++)
					{
						receiveData[i] = 0;
					}
					
					if (objectIStream != null)
					{
						objectIStream.close();
					}
					
					if (byteIStream != null)
					{
						byteIStream.close();
					}
					if (objOutStream != null)
					{
						objOutStream.close();
					}
				}
			} 
			catch (IOException e) 
			{
				bException = true;
				
				break;
			}
			finally
			{
				if (objectIStream != null)
				{
					
					try 
					{
						objectIStream.close();
					} 
					catch (IOException e) {
						//don't do anything
					}
				}
				
				if (byteIStream != null)
				{
					try 
					{
						byteIStream.close();
					} 
					catch (IOException e) 
					{
						//don't do anything
					}
				}
				
				if (objOutStream != null)
				{
					try 
					{
						objOutStream.close();
					} 
					catch (IOException e) 
					{
						//don't do anything
					}
				}
				
				if (byteOutStream != null)
				{
					try 
					{
						byteOutStream.close();
					} 
					catch (IOException e) 
					{
						//don't do anything
					}
				}
			}
		}
	
		if (bException)
		{
			//TODO: register an exception
		}
		
		heartBeatTimer.cancel();
		
		synchronized (m_instance)
		{
			m_bShutdown = true;
			notify();
		}
		
		System.out.println("HeartBeat monitor shutting down");
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
		
		System.out.println("HeartBeat monitor shut down");
	}
	
	
	public static synchronized ClusterManager getWorkerHealthMonitorInstance()
	{
		if (m_instance == null)
		{
			m_instance = new ClusterManager();
		}
		
		return m_instance;
	}
	
	
	protected class HeartBeatTimerTask extends TimerTask
	{

		@Override
		public void run() 
		{
			
			synchronized (m_NodeInfoUpdateLock)
			{
				System.out.println("HeartBeatTimer starting...");
				if (m_nodeInfoArray.size() > 0)
				{
					System.out.println("Node check in progress");
					NodePacketBase nodeInfo = m_nodeInfoArray.get(0);
					
					//heart beat has not been received from node for more than timer interval
					if (nodeInfo.getHeartBeatTimeInMillis() + TIMER_INTERVAL_MILLISECONDS < System.currentTimeMillis())
					{
						removeNode(nodeInfo);
					}
				}
			}
			
		}
	
	}
	
	protected void removeNode(NodePacketBase nodeInfo)
	{
		m_nodeInfoArray.remove(0);
		m_nodeInfoByNodeNameMap.remove(nodeInfo.getNodeName());
		m_nodeSIDByNodeNameMap.remove(nodeInfo.getNodeName());
		
		synchronized (m_nodeStatusListenersList)
		{
			for (NodeStatusListener nodeStatusListener: m_nodeStatusListenersList)
			{
				nodeStatusListener.listen(NodeStatusListener.NodeState.LEFT, nodeInfo.getNodeName(), nodeInfo.getHeartBeatSID());
			}
		}
		
		
		System.out.println("Node " + nodeInfo.getNodeName() + " left.");
	}
	
	public static interface NodeStatusListener
	{
		public enum NodeState
		{
			JOINED, LEFT;
		}
		public void listen(NodeState enumNodeState, String sNodeName, long lSID);
	}
}
