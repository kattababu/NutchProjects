/**
 * 
 */
package com.Nutch.Crawl.Canal;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * @author surendra
 *
 */
public class CanalRMEpsiode {

	/**
	 * 
	 */
	public CanalRMEpsiode() {
		// TODO Auto-generated constructor stub
	}
	
	
	
	HTable ht=null;
	Scan sc=null;
	ResultScanner resc;
	String rownames=null,family=null,qualifier=null,content=null,splitter=null,splitter1=null,splitterIMD=null,ImgDimes=null;
	
	String imag=null;
	String uname=null;
	
	static FileOutputStream fos =null;
	static PrintStream ps=null;
	MSDigest msd=new MSDigest();
	
static String mainurl="http://www.canal10.com.ni";
	

	
			
	public void QualifierMatchEpsiode(String name,String imgs)
	{
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
					
					
					if(family.equals("ol"))
					{
						
						//System.out.println("External"+rownames);
						
						
						
					if(qualifier.equals(name))
					{
						
						Spliturl(name);
						SplitImg_SK(name);
						
						
						
						
						if(rownames.contains(splitter) && rownames.endsWith(splitter))
						{
							
						
							
							//content=Bytes.toString(kv.getValue());
							//System.out.println(rownames);
							uname=name;
							imag=imgs;
							msd.MD5(imag);
							
							//ImageUrls542(rownames);
							
							
							
							Tabs();
							
							
						
							//CanalMovieCNT(rownames);
							//ImageUrls();
							
							
										
				
				
				
				
							
							
							
							
							
							
							
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
				fos.close();
				ps.close();
				
			}
			
			catch(Exception e)
			{
				e.getMessage();
			}
		}
		
		
		
	}
	
	
	////////////////////
	
	
	
	public void Tabs()
	{
///////////// Image_SK////////////
		//Spliturl1(sk);
		
		System.out.print(msd.md5s+"#<>#");
		//System.out.print(splitter1.substring(0, splitter1.length()-4).trim()+"#<>#");
		
		
		/// Program _SK//////////// Value
		String p_sk=splitter1;
		System.out.print(p_sk.trim()+"#<>#");
		
		
		/////////////Program_Type///////////
		System.out.print("episode"+"#<>#");
		
		
/////////////Media_Type///////////
		System.out.print("image"+"#<>#");
		
/////////////Image_Type///////////
		System.out.print("small"+"#<>#");
		
/////////////Size///////////
		System.out.print("#<>#");
/////////////Dimensions///////////
		
		if(imag.contains("x"))
		{
			ImageDes(imag);
			System.out.print(ImgDimes+"#<>#");
		}
		else
		{
		System.out.print("#<>#");
		}
		//System.out.print("#<>#");
		
/////////////Description///////////
		System.out.print("#<>#");
		
		
/////////////Image_URL///////////
		System.out.print(imag.trim()+"#<>#");
		
		
/////////////Reference_url///////////
		System.out.print(uname.trim()+"#<>#");
		
		
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
	
	
	/////////////////////////////////////
	
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
	

	
	
	///////////////////////////
	
	
	
	
	public void ImageUrlsEpsiode(String name)
	{
		
		
		
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
						 
						 
						 Element el=document.body();
						 
						 
						 
						  Elements els=el.select("div.item > a");
						  for(Element el1:els)
						  {
							  String attrh=el1.attr("href");
							  String totalurl=mainurl+attrh;
							  
							  
							  String imgs=null;
							  
							  //System.out.println(attrh);
							  
							  
							  Elements els2=el1.select("img");
							  
							  for(Element el2:els2)
							  {
								  imgs=el2.attr("src");
								  
								  //System.out.println(imgs);
							  }
							  QualifierMatchEpsiode(totalurl,imgs);
							  
							  
						  }
						  
						  /*
						 
						  XElements elx=Xsoup.compile("//div[@class='lista']/a").evaluate(document);
						  
						  
						  
						  
						 Elements ss= elx.getElements();
						 for(Element el45:ss)
						 {
							 String els34=el45.attr("href");
							 System.out.println(els34);
							 
							 Elements sv=el45.getElementsByTag("img");
							 for(Element e46:sv)
							 {
								 String els46=e46.attr("src");
								 System.out.println(els46);
							 }
								 
						 }
							
							*/
							/*
							List<String> list =Xsoup.compile("//div[@class='lista']/a/@href").evaluate(document).list();
							list2 =Xsoup.compile("//div[@class='lista']//a//img/@src").evaluate(document).list();
							
									
									for(String href:list)
									{
										if(href!=null)
										{
											
											//System.out.println(href);
											//System.out.println(img);
											QualifierMatch(href);	
										}
									}
									
								
							
							*/
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
///////////////////////////////////////////////////////////////////////////
	
	
	
	
	public void Spliturl(String name)
	{
		String[] split=name.split("\\/");
		splitter=split[split.length - 2];
		//System.out.println(splitter);
	}
	
	
	public void SplitImg_SK(String name)
	{
		String[] split=name.split("\\/");
		splitter1=split[split.length - 1];
		//System.out.println(splitter);
	}



}
