/**
 * 
 */
package com.Nutch.Crawl.Canal;




import java.io.FileOutputStream;

import java.io.PrintStream;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
//import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.tika.language.LanguageIdentifier;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.lingway.ld.LangDetector;

//import org.apache.tika.language.LanguageIdentifier;



import us.codecraft.xsoup.Xsoup;

/**
 * @author surendra
 *
 */
public class CanalMT {

	/**
	 * 
	 */
	public CanalMT() {
		// TODO Auto-generated constructor stub
	}
	
	HTable ht=null;
	Scan sc=null;
	ResultScanner resc;
	String rownames=null,family=null,qualifier=null,content=null,splitter=null,ImgDimes=null,splitterIMD=null;
	static int count1 = 0;  
	
	static FileOutputStream fos =null;
	static PrintStream ps=null;
	MSDigest msd=new MSDigest();

	
	static 
	 	{
				
				FileStore.MovieTable("movie");
		 }

	
	public void QualifierMatch(String name)
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
							
							new CanalMT().CanalMovieData(rownames);
							new CanalMT().CanalMovieLargeImage(rownames);
							
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
	
	
	
///////////////////////////////////////////////////////////
	
	
	
	
	public void CanalMovieData(String name)
	{
		
		//CanalCNT cnt=new CanalCNT();
		
		try
		{
			
			fos = new FileOutputStream(FileStore.fileM,true);
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
								/*
								
								
								LanguageIdentifier identifier = new LanguageIdentifier(title);
									String lang=identifier.getLanguage();
									System.out.println(lang);
									Locale loc =new Locale(lang);
									String namevalue=loc.getDisplayLanguage(loc);
									System.out.print(namevalue.toLowerCase().trim()+"#<>#");
								
								
								*/
								System.out.print("spanish"+"#<>#");
								
						
						
								
						
								
								//System.out.print(lang.trim()+"#<>#");
								
								
								
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
	public void Splittitle(String name)
	{
		String[] split=name.split("\\-");
		splitter=split[split.length - 2];
		//System.out.println(splitter); 
	}
	
////////////////////////////////////////////////////  For Large Movie Screen Image//////////////////////////////////////
	
	public void CanalMovieLargeImage(String name)
	{
		
		//CanalCNT cnt=new CanalCNT();
		
		try
		{
			
			fos = new FileOutputStream(FileStore.fileRM,true);
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
						
								content=Bytes.toString(kv.getValue());
								Document document = Jsoup.parse(content);
								//System.out.println(name);
								
								///////////// Large Image INTERNAL///////////////////////
								String imgLarge=Xsoup.compile("//div[@id='vodzone']//img/@src").evaluate(document).get();
								if(imgLarge!=null)
								{
								
								//System.out.println(imgLarge);
								msd.MD5(imgLarge);
								System.out.print(msd.md5s+"#<>#");
								
								
								/////////////Program _Sk ///////////////////
								
								String url=Xsoup.compile("//meta[@property='og:url']/@content").evaluate(document).get();
								Spliturl(url);
								System.out.print(splitter.trim()+"#<>#");
								
								/////////////Program_Type///////////
								System.out.print("movie"+"#<>#");
								
								
						/////////////Media_Type///////////
								System.out.print("image"+"#<>#");
								
						/////////////Image_Type///////////
								System.out.print("large"+"#<>#");
								
						/////////////Size///////////
								System.out.print("#<>#");
						/////////////Dimensions///////////
								
								
								if(imgLarge.contains("x"))
								{
									ImageDes(imgLarge);
									System.out.print(ImgDimes+"#<>#");
								}
								else
								{
								System.out.print("#<>#");
								}
								
								System.out.print("#<>#");
								
						/////////////Description///////////
								System.out.print("#<>#");
								
								
						/////////////Image_URL///////////
								System.out.print(imgLarge.trim()+"#<>#");
								
								
						/////////////Reference_url///////////
								System.out.print(url.trim()+"#<>#");
								
								
						///////////Aux_Info////////
								System.out.print("#<>#");
								
						//////////Created_At/////////
								System.out.print("#<>#");
								
						//////////Modified_At/////////
								System.out.print("#<>#");
								
						//////////Last _Seen/////////
								System.out.print("#<>#");
								System.out.print("\n");
								
								
								//System.lineSeparator();
								

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

	public void ImageDes(String name)
	{
		
			String[] split=name.split("\\/");
			splitterIMD=split[split.length - 1];
			//System.out.println("\n");
			
			String pattern="(\\d+)(x)(\\d+)";
			
			Pattern r = Pattern.compile(pattern);

		      // Now create matcher object.
		      Matcher m = r.matcher(splitterIMD);
		      if (m.find( )) {
		    	  ImgDimes=  m.group(0) ;
		           }else {
		         System.out.println("NO MATCH");
		      }
		      /*
			String dsp[]=splitterIMD.split("x");
			String fn=dsp[0];
			System.out.println(num);
			
			
			String nn=dsp[1];
			String lastn=nn.substring(0, num);
			
			ImgDimes=fn+"x"+lastn;
			*/
		
		//System.out.println(dsp);
			
			//System.out.println("\n");
			
			
		}
	
	

	

}
