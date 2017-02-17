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
import org.apache.tika.language.LanguageIdentifier;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import us.codecraft.xsoup.Xsoup;

/**
 * @author surendra
 *
 */
public class CanalCNTSer {

	/**
	 * 
	 */
	public CanalCNTSer() {
		// TODO Auto-generated constructor stub
	}
	
	HTable ht=null;
	Scan sc=null;
	ResultScanner resc;
	String rownames=null,family=null,qualifier=null,content=null,splitter=null;
	static FileOutputStream fos =null;
	static PrintStream ps=null;
	
	CanalRMPeriod crmp=new CanalRMPeriod();
	
	
	CanalTVShow ctvs=new CanalTVShow();
	
	
	 
	// 
	
	public void ContRowsSer()
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
							
						
							if(rownames.contains("/series"))
							{
								//System.out.println(rownames);
								
								
								ctvs.ContTVShowSer(rownames);
								ctvs.ContTVShowSerRM(rownames);
								
							}
							
							
							if(rownames.contains("/periodisticos"))
							{
								//System.out.println(rownames);
								
								crmp.ImageUrlsPeriod(rownames);
								
								ContINTRowsPer(rownames);
								
								
								
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
				ps.flush();
				ps.close();
				
				fos.close();
				
				
			}
			
			catch(Exception e)
			{
				e.getMessage();
			}
		}
		
		
		
	}
	
	
	/////////////////////////////// PERIODICS/////////////////
	
	
	public void ContINTRowsPer(String name)
	{
		
		
		//CanalTVShow ctvs=new CanalTVShow();
		
		//CanalMovRich cmr=new CanalMovRich();
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
								
								QualifierMatchPer(href);
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
	
	
	public void QualifierMatchPer(String name)
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
						
						if(rownames.contains(splitter) && rownames.endsWith(splitter))
						{
							
						
							CanalTVDataPer(rownames);
							
							//content=Bytes.toString(kv.getValue());
							//System.out.println(rownames);
							//CanalMovieCNT(rownames);
							//String sk=splitter;
							//System.out.println("SK VAlue:"+sk);
							//System.out.println("Title:"+CanalCNT.title);
							
							
						
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
	
	
	////////////////////////////////
	
	
	
///////////////////////////////////////////////////////////
	
	
	
	
	public void CanalTVDataPer(String name)
	{
		
		//CanalCNT cnt=new CanalCNT();
		
		try
		{
			
			fos = new FileOutputStream(FileStore.fileTvshow,true);
			ps = new PrintStream(fos);
			   System.setOut(ps);
			
			
			
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
						
							if(family.equals("f") && qualifier.equals("cnt"))
							{
								content=Bytes.toString(kv.getValue());
								Document document = Jsoup.parse(content);
								////////// SK Value//////////////
								
								String url=Xsoup.compile("//meta[@property='og:url']/@content").evaluate(document).get();
								Spliturl(url);
								System.out.print(splitter.trim()+"#<>#");
								
								
								////////// Title//////////////
								String title=Xsoup.compile("//meta[@property='og:title']/@content").evaluate(document).get();
								Splittitle(title);
								System.out.print(splitter.trim()+"#<>#");
								
								////////// Original Title/////////
								System.out.print("#<>#");
								

								////////// Other Titles/////////
								System.out.print("#<>#");
						
								/////// Description///////////////
								
								String descript=Xsoup.compile("//*[@id='slideshow']/div[2]/div/div/text()").evaluate(document).get();
													
								System.out.print(descript.trim()+"#<>#");
								
						////////// Genres/////////
								System.out.print("#<>#");
								
						//////////Sub Genres/////////
								System.out.print("#<>#");
								
								
						//////////Category/////////
								System.out.print("#<>#");
								
								
						////////// Duration/////////
								System.out.print("#<>#");
								
								
								
						//////////Languages/////////
								System.out.print("#<>#");
								
						//////////Original Languages/////////
								System.out.print("#<>#");
								
								
						//////////Metadata_language/////////
								
								LanguageIdentifier identifier = new LanguageIdentifier(title);
								String lang=identifier.getLanguage();
								
								
								
								System.out.print(lang.trim()+"#<>#");
								
								
								
						//////////Aka/////////
								System.out.print("#<>#");
								
								
						//////////Production _Country/////////
								System.out.print("#<>#");
						
								
								
						///////////Aux_Info////////
								System.out.print("#<>#");
								
								
								
						//////////Reference URL/////////
								System.out.print(url.trim()+"#<>#");
								
								
						//////////Created_At/////////
								System.out.print("#<>#");
								
						//////////Modified_At/////////
								System.out.print("#<>#");
								
						//////////Last _Seen/////////
								System.out.print("#<>#");
								System.out.print("\n");
								
								
								
								
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
				//System.out.print("\n");
				
				ht.close();
				resc.close();
				fos.close();
				ps.close();
				
			}
			
			catch(Exception e)
			{
				e.getMessage();
			}
		}
		
		
		
	}
	public void Splittitle(String name)
	{
		String[] split=name.split("\\-");
		splitter=split[split.length - 2];
		//System.out.println(splitter);
	}

	
	
	

}
