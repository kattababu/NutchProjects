/**
 * 
 */
package com.Nutch.Crawl.Canal;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import us.codecraft.xsoup.Xsoup;

/**
 * @author surendra
 *
 */
public class CanalCNTEpsiode{

	/**
	 * 
	 */
	public CanalCNTEpsiode() {
		// TODO Auto-generated constructor stub
	}
	
	
	
	HTable ht=null;
	Scan sc=null;
	ResultScanner resc;
	String rownames=null,family=null,qualifier=null,content=null,splitter=null;
	static FileOutputStream fos =null;
	static PrintStream ps=null;
	
	//CanalRMPeriod crmp=new CanalRMPeriod();
	
	CanalEpsiode ce=new CanalEpsiode();
	CanalRMEpsiode crme=new CanalRMEpsiode();
	
	
	
	
	
	 
	// 
	
	public void ContRowsProgEps()
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
							//System.out.println(" The RowsName are:"+rownames);
							
							
							/////////////////  TVSHows////////////////////
						
						
							
							if(rownames.contains("/periodisticos")||rownames.contains("/variedades"))
							{
								//System.out.println(rownames);
								
								//crmp.ImageUrlsPeriod(rownames);
								
							new CanalCNTEpsiode().ContINTRowsEpsiode(rownames);
								
								
								
								
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
				ps.close();
				
				fos.close();
				
				
			}
			
			catch(Exception e)
			{
				e.getMessage();
			}
		}
		
		
		
	}
	
	/////////////////////////////////////////////////Program CNT ROWs////////////////
	
	
	
public void ContINTRowsEpsiode(String name)
{
	
	
	try
	{
		
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
				
				if(rownames.equals(name))
				{
				
					content=Bytes.toString(kv.getValue());
										
					//System.out.println(content);
					 Document document = Jsoup.parse(content);
					 
											 
					List<String> list =Xsoup.compile("//div[@class='lista']/a/@href").evaluate(document).list();
					for(String href:list)
					{
						
						
						if(href!=null)
						{
							 
							
							//mt.QualifierMatch(href);//------------------->
							//cmr.QualifierMatch(href);
							
							QualifierMatchEpsiode(href);
						//System.out.println(href);
						
						
													//
						}
						
						
					}
					
					//////////////////
					
					
					
					
					
					
					
					
					
					
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
			
		}
		
		catch(Exception e)
		{
			e.getMessage();
		}
	}
	
	
	
}


///////////////////////////////////////


public void QualifierMatchEpsiode(String name)
{
	try
	{
		
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
				
				
				if(family.equals("ol"))
				{
				
				if(qualifier.equals(name))
				{
					
					Spliturl(name);
					//System.out.println(qualifier);
					
					
					
					
					
	//////////////////second family  EPISODE////////////////////
					
					if(rownames.contains(splitter) && !rownames.endsWith(splitter) && !rownames.endsWith("/historico"))
					{
						
					
						//CanalTVDataPer(rownames);
						//System.out.println(rownames);
						ce.CanalEPSDataPer(rownames);
						ce.CanalEPSImagePER(rownames);
						
						
													
					}
					
						
					
					
						 
						
							
							
				}
					
						
					}
				
				
				
				
				if(family.equals("il"))
				{
				
					if(qualifier.equals(name))
					{
						
						Spliturl(name);
					
				if(rownames.contains(splitter)&& rownames.endsWith("/historico"))
						{
					
				
					//CanalTVDataPer(rownames);
					//System.out.println(rownames);
					//ce.CanalEPSDataPer(rownames);
					
					crme.ImageUrlsEpsiode(rownames);
					
					
												
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
			
		}
		
		catch(Exception e)
		{
			e.getMessage();
		}
	}
	
	
	
}

public void Spliturl(String name)
{
	String[] split=name.split("\\/");
	splitter=split[split.length - 1];
	//System.out.println(splitter);
}


}
