/**
 * 
 */
package com.Nutch.Crawl.Canal;

//import java.io.FileOutputStream;
//import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author surendra
 *
 */
public class CanalCNTSERIND {

	/**
	 * 
	 */
	public CanalCNTSERIND() {
		// TODO Auto-generated constructor stub
	}
	
	HTable ht=null;
	Scan sc=null;
	ResultScanner resc;
	String rownames=null,family=null,qualifier=null,content=null,splitter=null,value=null;
	//static FileOutputStream fos =null;
	//static PrintStream ps=null;
	

	
	
	public void ContRowsSerIND()
	{
		try
		{
			
			//fos = new FileOutputStream(FileStore.fileTvshow,true);
			//ps = new PrintStream(fos);
			 // System.setOut(ps);
			
			
			Configuration config=HBaseConfiguration.create();
			ht=new HTable(config,"canal_webpage");
			sc=new Scan();
			resc=ht.getScanner(sc);
			for(Result res = resc.next(); (res != null); res=resc.next())
			{
				for(KeyValue kv:res.list())
				{
					
					rownames=Bytes.toString(kv.getRow());
					family=Bytes.toString(kv.getFamily());
					qualifier=Bytes.toString(kv.getQualifier());
					
					
					if(family.equals("il"))
					{
						if(qualifier.equals("http://www.canal10.com.ni/"))
						{
							
							
							
							/////////////////  TVSHows////////////////////
							
						
							if(rownames.contains("/programa/"))
							{
								//ContTVShowSerNON(rownames);
								
								
								System.out.println(rownames);
								//ctvs.ContTVShowSer(rownames);
								//ctvs.ContTVShowSerRM(rownames);
								
							}
							
							}
					}
				}
			}
			
			
			
			
		}
		
		
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				
				ht.close();
				resc.close();
				//ps.flush();
				//ps.close();
				
				//fos.close();
				
				
			}
			
			catch(Exception e)
			{
				e.getMessage();
			}
		}
		
		
		
	}
	
}
