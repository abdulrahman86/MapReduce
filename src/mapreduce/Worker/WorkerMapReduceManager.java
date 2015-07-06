package mapreduce.Worker;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.Timer;
import java.util.TimerTask;

import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream;

import mapreduce.api.NodePacketBase;

public class WorkerMapReduceManager extends Thread
{

	protected String m_sNodeName;
	protected int m_nPort;
	protected int m_nMasterPort;
	protected long m_lSID;
	protected DatagramSocket m_serverSocket;
	protected static WorkerMapReduceManager m_instance;
	protected final int HEART_BEAT_TIMER_INTERVAL_MILLISECONDS = 3 * 1000;
	
	protected WorkerMapReduceManager()
	{
		m_sNodeName = System.getProperty("worker.nodeName");
		m_nPort = Integer.getInteger("worker.port");
		m_nMasterPort = Integer.getInteger("master.port");
	}
	
	public synchronized static WorkerMapReduceManager getInstance()
	{
		if (m_instance == null)
		{
			m_instance = new WorkerMapReduceManager();
		}
		
		return m_instance;
	}
	
	public void run()
	{
		Timer heartBeatTimer = new Timer("HEART_BEAT_TIMER");
		
		ByteArrayInputStream byteIStream = null;
		ObjectInputStream objectIStream = null;
		byte[] incomingDataByteArr = new byte[1024];
		
		while (m_serverSocket == null)
		{
			try 
			{
				m_serverSocket = new DatagramSocket(m_nPort);
			} 
			catch (SocketException e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		boolean bRejoin = true;
		
		while(1 == 1)
		{			
			HeartBeatTimerTask heartBeatTimerTask = new HeartBeatTimerTask();
			//join cluster
			if (bRejoin)
			{
				m_lSID = joinCluster();				
				//start timer to send periodic heartbeats to master
				heartBeatTimer.schedule(heartBeatTimerTask, 
						0, HEART_BEAT_TIMER_INTERVAL_MILLISECONDS);
				bRejoin = false;
			}
			
			//start map reduce worker thread in the background
					
			//if rejoin packet received stop map reduce worker thread and timer, rejoin and restart map reduce worker thread and timer.	
			try 
			{
				DatagramPacket incomingPacket = new DatagramPacket(incomingDataByteArr, incomingDataByteArr.length);
				m_serverSocket.receive(incomingPacket);		
				
				byteIStream = new ByteArrayInputStream(incomingDataByteArr);
				objectIStream = new ObjectInputStream(byteIStream);
				
				NodePacketBase serverResponse = (NodePacketBase) objectIStream.readObject();
				
				if (serverResponse.isRejoin())
				{
					bRejoin = true;
					heartBeatTimer.cancel();
					heartBeatTimerTask.cancel();

				}
				else
				{
					//handle server response
					
				}
			} 
			catch (IOException e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			catch (ClassNotFoundException e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			finally
			{
				if (objectIStream != null)
				{
					try 
					{
						objectIStream.close();
					} 
					catch (IOException e) 
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				if (incomingDataByteArr != null)
				{
					for (int i = 0; i < incomingDataByteArr.length; i++)
					{
						incomingDataByteArr[i] = 0;
					}
				}
			}
		}
	}
	
	protected class WorkerMapReduceReqRespHandler extends Thread
	{
		public void run()
		{
			
		}
	}
	
	protected long joinCluster()
	{
		boolean bJoinSuccessful = false;
		ByteOutputStream byteOutStream = null;
		ObjectOutputStream objOutStream = null;
		byte [] incomingDataByteArr = null;
		
		ByteArrayInputStream byteIStream = null;
		ObjectInputStream objectIStream = null;
		
		long lSID = -1;
		
		
		while (! bJoinSuccessful)
		{
			try
			{
				NodePacketBase joinPacket = new NodePacketBase();
				long lNONCE = System.nanoTime();
								
				joinPacket.setNodeHeartBeatPort(m_nPort);
				joinPacket.setWorkerNodeNonce(lNONCE);
				joinPacket.setNodeName(m_sNodeName);
				joinPacket.setHeartBeatSID(-1);
				
				
				
				
				if (incomingDataByteArr == null)
				{
					incomingDataByteArr = new byte[1024];
				}
				
				if (byteOutStream == null)
				{
					byteOutStream = new ByteOutputStream();
				}
				
				
				objOutStream = new ObjectOutputStream(byteOutStream);
				
				objOutStream.writeObject(joinPacket);
				
				byte[] outDataByteArr = byteOutStream.getBytes();
				
				m_serverSocket.send(new DatagramPacket(outDataByteArr, 
						outDataByteArr.length, InetAddress.getByName("localhost"), m_nMasterPort));
				
				DatagramPacket incomingPacket = new DatagramPacket(incomingDataByteArr, incomingDataByteArr.length);
				//wait for one minute
				m_serverSocket.setSoTimeout(60 * 1000);
				m_serverSocket.receive(incomingPacket);
				
				if (byteIStream == null)
				{
					byteIStream = new ByteArrayInputStream(incomingPacket.getData());
				}

				objectIStream = new ObjectInputStream(byteIStream);
				
				NodePacketBase serverResponsePacket = (NodePacketBase) objectIStream.readObject();
				
				if (serverResponsePacket.getWorkerNodeNonce() == lNONCE && ! serverResponsePacket.isRejoin())
				{
					bJoinSuccessful = true;
					
					lSID = serverResponsePacket.getHeartBeatSID();
				}
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
			finally
			{
				//clean up 
				if (byteOutStream != null)
				{
					if (bJoinSuccessful)
					{
						byteOutStream.close();
						byteOutStream = null;
					}
					else
					{
						byteOutStream.reset();
					}
				}
				
				if (objOutStream != null)
				{
					try 
					{
						objOutStream.close();
						objOutStream = null;
					} 
					catch (IOException e) 
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				if (incomingDataByteArr != null)
				{
					
					for (int i = 0; i < incomingDataByteArr.length - 1; i++)
					{
						incomingDataByteArr[i] = 0;
					}
					
					if (bJoinSuccessful)
					{
						incomingDataByteArr = null;
					}
				}
				
				if (byteIStream != null)
				{
					if (bJoinSuccessful)
					{
						try 
						{
							byteIStream.close();
						} 
						catch (IOException e) 
						{
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						byteIStream = null;
					}
					else
					{
						byteIStream.reset();
					}
				}
				
				if (objectIStream != null)
				{
					try 
					{
						objectIStream.close();
						
						objectIStream = null;
					} 
					catch (IOException e) 
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					objectIStream = null;
				}
			}
		}
		
		try 
		{
			//reset timeout
			m_serverSocket.setSoTimeout(0);
		} 
		catch (SocketException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return lSID;
	}
	
	protected class HeartBeatTimerTask extends TimerTask
	{

		protected boolean m_bCancel = false;
		
		public boolean cancel()
		{
			synchronized(this)
			{
				m_bCancel = true;
			}
			
			return super.cancel();
		}
		
		@Override
		public void run() 
		{
			ByteOutputStream byteOutStream = null;
			ObjectOutputStream objOutStream = null;
			DatagramSocket heartBeatTimerSocket = null;
			
			try 
			{
				System.out.println("WorkerNode sending heartbeat...");
				heartBeatTimerSocket = new DatagramSocket(9900);
				
				byteOutStream = new ByteOutputStream();
				objOutStream = new ObjectOutputStream(byteOutStream);
				
				NodePacketBase heartBeatPacket = new NodePacketBase();
				heartBeatPacket.setNodeName(m_sNodeName);
				heartBeatPacket.setHeartBeatSID(m_lSID);
				heartBeatPacket.setNodeHeartBeatPort(m_nPort);
				
				objOutStream.writeObject(heartBeatPacket);
				objOutStream.flush();
				
				
				byte[] heartBeatPacketByteArr = byteOutStream.getBytes();
				
				synchronized(this)
				{
					if (! m_bCancel)
					{
						heartBeatTimerSocket.send(new DatagramPacket(heartBeatPacketByteArr, heartBeatPacketByteArr.length, 
								InetAddress.getByName("localhost"), m_nMasterPort));
					}
				}
				
			} 
			catch (SocketException e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			catch (IOException e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			finally
			{
				if (heartBeatTimerSocket != null)
				{
					heartBeatTimerSocket.close();
				}
				
				if (objOutStream != null)
				{
					try 
					{
						objOutStream.close();
					} 
					catch (IOException e) 
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}		
		}
			
	}
}
