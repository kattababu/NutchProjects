/**
 * 
 */
package com.Nutch.Crawl.Canal;


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
public class CanalCNT {

	/**
	 * 
	 */
	public CanalCNT() {
		// TODO Auto-generated constructor stub
	}
	
	
	
	HTable ht=null;
	Scan sc=null;
	ResultScanner resc;
	String rownames=null,family=null,qualifier=null,content=null;
	
	static String title=null;
	
	public void ContRows()
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
					
					
					if(family.equals("il"))
					{
						if(qualifier.equals("http://www.canal10.com.ni/"))
						{
							//System.out.println(" The RowsName are:"+rownames);
							
							/// For Movies Details........///
							
							if(rownames.contains("/peliculas"))
							{
								//System.out.println(rownames);
								ContINTRows(rownames);
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
	
	
	//////////////////////////////////  For Movies List Meta Data.....///////////////
	
	
	public void ContINTRows(String name)
	{
		//int i=1;
		CanalMT mt=new CanalMT();
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
						 
						 /*
						 List<String> list2 =Xsoup.compile("//div[@class='lista']/a/div[@class='titulo']/text()").evaluate(document).list();
							for(String title1:list2)
							{
								if(title1!=null)
								{
									title=title1;
									
									
								//System.out.println(title);
								//mt.QualifierMatch(href);
								//
								}
								
							}
							*/

						 
						 
						 
						List<String> list =Xsoup.compile("//div[@class='lista']/a/@href").evaluate(document).list();
						//List<String> list2 =Xsoup.compile("//div[@class='lista']/a/div[@class='titulo']/text()").evaluate(document).list();
						
						//String a=list.iterator().;
						//System.out.println(a);
						
						//Xsoup.select(element, list)
						for(String href:list)
						{
							
							
							if(href!=null)
							{
								 
								
								mt.QualifierMatch(href);
							//System.out.println(href);
							
							
														//
							}
							
							
						}
						/*
						List<String> list2 =Xsoup.compile("//div[@class='lista']/a/div[@class='titulo']/text()").evaluate(document).list();
						for(String title1:list2)
						{
							if(title1!=null)
							{
								title=title1;
							//System.out.println(title);
							//mt.QualifierMatch(href);
							//
							}
							
						}
						*/
					
						
						
						
						
						
						
						
						//System.out.println(document);
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
	
	

	
	
	

}
