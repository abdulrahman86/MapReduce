package test.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

import mapreduce.api.NodePacketBase;

public class TestHealthMonitor {

	protected static String NODE_NAME = "Node2";
	
	public static void main(String[] args) throws IOException, ClassNotFoundException
	{
		System.setProperty("os.name", "OS X");
		
		long lHeartBeatSID = connectNode();
		
		sendNodeInfoToServer(lHeartBeatSID);
		
		
	}
	
	public static void sendNodeInfoToServer (long lSID) throws IOException
	{
		NodePacketBase nodeInfo = new NodePacketBase();
		nodeInfo.setNodeName(NODE_NAME);
		nodeInfo.setHeartBeatSID(lSID);
		
		DatagramSocket socket = new DatagramSocket();
		
		ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();
		ObjectOutputStream objOutStream = new ObjectOutputStream(byteOutStream);
		
		objOutStream.writeObject(nodeInfo);
		
		objOutStream.flush();
		
		
		byte[] nodeInfoByteArray = byteOutStream.toByteArray();
		
		socket.send(new DatagramPacket(nodeInfoByteArray, nodeInfoByteArray.length, InetAddress.getByName("localhost"), 9812));
		
	}

	public static long connectNode() throws IOException, ClassNotFoundException
	{
		DatagramSocket socket = null;
		DatagramSocket inSocket = null;
		
		try
		{
			NodePacketBase nodeInfo = new NodePacketBase();
			nodeInfo.setNodeName(NODE_NAME);
			nodeInfo.setNodeHeartBeatPort(9901);
			
			socket = new DatagramSocket();
			
			ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();
			ObjectOutputStream objOutStream = new ObjectOutputStream(byteOutStream);
			
			objOutStream.writeObject(nodeInfo);
			
			objOutStream.flush();
			
			
			byte[] nodeInfoByteArray = byteOutStream.toByteArray();
			
			socket.send(new DatagramPacket(nodeInfoByteArray, nodeInfoByteArray.length, InetAddress.getByName("localhost"), 9812));
			
			inSocket = new DatagramSocket(9901);
			
			DatagramPacket inPacket = new DatagramPacket(new byte[1024], 1024);
			
			inSocket.receive(inPacket);
			
			ObjectInputStream objectIStream = new ObjectInputStream(new ByteArrayInputStream(inPacket.getData()));
			
			nodeInfo = (NodePacketBase) objectIStream.readObject();
			
			return nodeInfo.getHeartBeatSID();
		}
		finally
		{
			if (socket != null)
			{
				socket.close();
			}
			
			if (inSocket != null)
			{
				inSocket.close();
			}
			
		}
	}
}

