/**
 * 
 */
package com.Nutch.Crawl.Canal;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Locale;

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
public class CanalEpsiode {

	/**
	 * 
	 */
	public CanalEpsiode() {
		// TODO Auto-generated constructor stub
	}
	HTable ht=null;
	Scan sc=null;
	ResultScanner resc;
	String rownames=null,family=null,qualifier=null,content=null,splitter=null,splitterEps=null,splitterEno=null,splitterShow=null;
	static FileOutputStream fos =null;
	static PrintStream ps=null;
	static 	{
		
		FileStore.EpsiodeTable("episode");
 }

	
	
	
	public void CanalEPSDataPer(String name)
	{
		
		//CanalCNT cnt=new CanalCNT();
		
		try
		{
			
			fos = new FileOutputStream(FileStore.fileTvshowEps,true);
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
									
								//System.out.println(rownames);
								
								

								
								content=Bytes.toString(kv.getValue());
								Document document = Jsoup.parse(content);
								////////// SK Value//////////////
								
								String url=Xsoup.compile("//meta[@property='og:url']/@content").evaluate(document).get();
								//System.out.println(url);
								
								Spliturl(url);
								System.out.print(splitter.trim()+"#<>#");
								
								//////////////////////  Season_Sk//////////////
								
								System.out.print("#<>#");
								
								//////////////////////  TvShow_Sk//////////////
							SpliturlEps(url);
								
								System.out.print(splitterEps.trim()+"#<>#");
								
								
								
								
								
								////////// Title//////////////
								/*
								String title=Xsoup.compile("//meta[@property='og:title']/@content").evaluate(document).get();
								Splittitle(title);
								System.out.print(splitter.trim()+"#<>#");
								*/
								String title=Xsoup.compile("//div[@class='texto']//text()").evaluate(document).get();
								
								System.out.print(title.trim()+"#<>#");
								
								
								
								////////// Show Title/////////
								SplitShowtitle(title);
								System.out.print(splitterShow+"#<>#");
								
								
								////////// Original Title/////////
								System.out.print("#<>#");
								

								////////// Other Titles/////////
								System.out.print("#<>#");
						
								/////// Description///////////////
								/*
								
								String descript=Xsoup.compile("//div[@class='texto']//text()").evaluate(document).get();
													
								System.out.print(descript.trim()+"#<>#");
								*/
								System.out.print("#<>#");
								
						////////// Episode Number/////////
								
								System.out.print("#<>#");
								
						////////// Season_Number/////////
								System.out.print("#<>#");
						
						
								
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
								Locale loc =new Locale(lang);
								String namevalue=loc.getDisplayLanguage(loc);
								System.out.print(namevalue.toLowerCase().trim()+"#<>#");
								
								
								
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
								

								//INLINKSPer(rownames);
								
								
								

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
	
	public void Spliturl(String name)
	{
		String[] split=name.split("\\/");
		splitter=split[split.length - 1];
		//System.out.println(splitter);
	}
	
	
	public void SpliturlEps(String name)
	{
		String[] split=name.split("\\/");
		splitterEps=split[split.length - 2];
		//System.out.println(splitter);
	}
	
	public void SplitShowtitle(String name)
	{
		String[] split=name.split("\\:");
		splitterShow=split[split.length - 2];
		//System.out.println(splitter);
	}
	

	


}
