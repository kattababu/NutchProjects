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
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import us.codecraft.xsoup.Xsoup;

/**
 * @author surendra
 *
 */
public class CanalTVShow {

	/**
	 * 
	 */
	public CanalTVShow() {
		// TODO Auto-generated constructor stub
	}
	
	
	HTable ht=null;
	Scan sc=null;
	ResultScanner resc;
	String rownames=null,family=null,qualifier=null,content=null,splitter=null;
	
	String imag=null;
	String uname=null;
	
	static FileOutputStream fos =null;
	static PrintStream ps=null;

	
	
	static 	{
			//	Date date = new Date() ;
			//	   SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd'T'HHmmssSSSSSS") ;
				 //long time = System.currentTimeMillis();
					
			//		file= new File("/katta/ActionHBO/ACTHBOMV_"+dateFormat.format(date)+".txt"); //Your file
					
					FileStore.TVShowTable("Tvshow");
			 }

	
	


	
	public void ContTVShow(String name)
	{
		try
		{
			fos = new FileOutputStream(FileStore.fileTvshow,true);
			ps = new PrintStream(fos);
			   System.setOut(ps);
			
			
			MSDigest msd=new MSDigest();
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
						 
						 String url=Xsoup.compile("//meta[@property='og:url']/@content").evaluate(document).get();
						 
												 
						List<String> list =Xsoup.compile("//div[@class='titulo']/text()").evaluate(document).list();
						for(String Data:list)
						{
							
							
							if(Data!=null)
							{
								 
								//System.out.println(Data);
								
								////////// SK Value//////////////
								msd.MD5(Data);
								System.out.print(msd.md5s+"#<>#");
								
								
								
								////////// Title//////////////
								String title=Data;
								System.out.print(title);
								
								////////// Original Title/////////
								System.out.print("#<>#");
								

								////////// Other Titles/////////
								System.out.print("#<>#");
						
								/////// Description///////////////
								
													
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
								LanguageIdentifier identifier = new LanguageIdentifier(title);
								String lang=identifier.getLanguage();
								
								
								
								System.out.print(lang.trim()+"#<>#");
								
								
						//////////Original Languages/////////
								System.out.print("#<>#");
								
								
						//////////Metadata_language/////////
								System.out.print("#<>#");
								
								
								
						//////////Aka/////////
								System.out.print("#<>#");
								
								
						//////////Production _Country/////////
								System.out.print("#<>#");
						
								
								
						///////////Aux_Info////////
								System.out.print("#<>#");
								
								
								
						//////////Reference URL/////////
								System.out.print(url+"#<>#");
								
								
						//////////Created_At/////////
								System.out.print("#<>#");
								
						//////////Modified_At/////////
								System.out.print("#<>#");
								
						//////////Last _Seen/////////
								System.out.print("#<>#");
								
								
								
								System.out.println("\n\n");
								

							
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
				fos.close();
				ps.close();
				
			}
			
			catch(Exception e)
			{
				e.getMessage();
			}
		}
		
		
		
	}
	
	
	////////////////////////////// Rich Media Table Data for TVShows/////////////////////
	
	
	
	public void ContTVShowRM(String name)
	{
		try
		{
			
			fos = new FileOutputStream(FileStore.fileRM,true);
			ps = new PrintStream(fos);
			   System.setOut(ps);
			
			
			MSDigest msd=new MSDigest();
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
						
						
						
						Document document = Jsoup.parse(content);
						 
						 
						 Element el=document.body();
						 
						 
						 String refurl=Xsoup.compile("//meta[@property='og:url']/@content").evaluate(document).get();
							
						  Elements els=el.select("div.lista > a");
						  for(Element el1:els)
						  {
							  String imgs=null;
							  String Data=null;
							  
							  
							  
							  Elements els2=el1.getElementsByTag("img");
							  
							  for(Element el2:els2)
							  {
								  imgs=el2.attr("src");
								  
								  //System.out.println(imgs);
							  }
							  
							  Elements els3=el1.select("div.titulo");
							  for(Element el3:els3)
							  {
								  Data=el3.ownText();
								 // System.out.println(Data);
							  }
							  
							  
							  /////////////////////////////////////
							  
							  
							  
					///////////// Image_SK////////////
							  
							  msd.MD5(imgs);
								
								System.out.print(msd.md5s+"#<>#");
								
								
								/// Program _SK//////////// Value
								msd.MD5(Data);
								System.out.print(msd.md5s+"#<>#");
								
								
								/////////////Program_Type///////////
								System.out.print("TVShow"+"#<>#");
								
								
						/////////////Media_Type///////////
								System.out.print("Image"+"#<>#");
								
						/////////////Image_Type///////////
								System.out.print("Medium"+"#<>#");
								
						/////////////Size///////////
								System.out.print("#<>#");
						/////////////Dimensions///////////
								System.out.print("#<>#");
								
						/////////////Description///////////
								System.out.print("#<>#");
								
								
						/////////////Image_URL///////////
								System.out.print(imgs+"#<>#");
								
								
						/////////////Reference_url///////////
								System.out.print(refurl+"#<>#");
								
								
						///////////Aux_Info////////
								System.out.print("#<>#");
								
						//////////Created_At/////////
								System.out.print("#<>#");
								
						//////////Modified_At/////////
								System.out.print("#<>#");
								
						//////////Last _Seen/////////
								System.out.print("#<>#");
								
								
								
								System.out.println("\n\n");


								
								
								

							  
							  
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
				fos.close();
				ps.close();
				
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
	
	//////////////////////////////  RemainingTvShow Files///////////////////
	
	public void QualifierMatchTv(String name)
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
						
						if(rownames.contains(splitter))
						{
							
						
							
							//content=Bytes.toString(kv.getValue());
							System.out.println(rownames);
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
	
	

}
