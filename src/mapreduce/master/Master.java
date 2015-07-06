package mapreduce.master;

import java.io.FileInputStream;
import java.util.Properties;

public class Master extends Thread{
	

	public static void main(String[] args) throws Exception 
	{
		
		Properties prop = new Properties();
		
		if (args.length != 1)
		{
			throw new Exception("The only argument should be the location of properties file");
		}
		
		FileInputStream iStream = null;
				
		try
		{
			iStream = new FileInputStream(args[0]);
			prop.load(iStream);
			
			System.setProperties(prop);
			
		}
		finally
		{
			if (iStream != null)
			{
				iStream.close();
			}
		}
		
		
		//start loop to register and health monitor workers
		Master thread= new Master();
		thread.start();
		
	}

	public void run()
	{
		//register shutdown hook for master
		Runtime.getRuntime().addShutdownHook( new Thread()
		{
			public void run ()
			{
					try 
					{
						ClusterManager.getWorkerHealthMonitorInstance().shutdown();
					} 
					catch (InterruptedException e) 
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			}
		});
			
		//start cluster manager to manage nodes in the cluster
		ClusterManager.getWorkerHealthMonitorInstance().start();
		
		//start mapreduce task handler to start handling tasks
		
		
		
	}
}
