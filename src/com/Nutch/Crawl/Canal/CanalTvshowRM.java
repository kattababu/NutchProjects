/**
 * 
 */
package com.Nutch.Crawl.Canal;

import java.io.FileOutputStream;
import java.io.PrintStream;

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
public class CanalTvshowRM {

	/**
	 * 
	 */
	public CanalTvshowRM() {
		// TODO Auto-generated constructor stub
	}

	
	HTable ht=null;
	Scan sc=null;
	ResultScanner resc;
	String rownames=null,family=null,qualifier=null,content=null,splitter=null,splitter1=null;
	
	String imag=null;
	String uname=null;
	
	static FileOutputStream fos =null;
	static PrintStream ps=null;
	MSDigest msd=new MSDigest();
	

	
	
	
	
	
	
	public void QualifierMatchTVRM(String name,String imgs)
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
					
					
					if(family.equals("il"))
					{
						
						//System.out.println("External"+rownames);
						
						
						
					if(qualifier.equals(name))
					{
						
						Spliturl(name);
						
						
						
						
						if(rownames.contains(splitter))
						{
							
						
							
							//content=Bytes.toString(kv.getValue());
							//System.out.println(rownames);
							uname=name;
							imag=imgs;
							msd.MD5(imag);
							
							//ImageUrls542(rownames);
							
							
							
							Tabs();
							
							
							 
							
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
				//System.lineSeparator();
				
				
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
		
		
		System.out.print(msd.md5s+"#<>#");
		//System.out.print(splitter1.substring(0, splitter1.length()-4).trim()+"#<>#");
		
		
		/// Program _SK//////////// Value
		String p_sk=splitter;
		System.out.print(p_sk.trim()+"#<>#");
		
		
		/////////////Program_Type///////////
		System.out.print("tvshow"+"#<>#");
		
		
/////////////Media_Type///////////
		System.out.print("image"+"#<>#");
		
/////////////Image_Type///////////
		System.out.print("small"+"#<>#");
		
/////////////Size///////////
		System.out.print("#<>#");
/////////////Dimensions///////////
		
		
			System.out.print("#<>#");
		
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
	
	
	
	///////////////////////////
	
	
	
	
	public void ImageUrlsTV(String name)
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
						 
						 
						 
						  Elements els=el.select("div.lista > a");
						  for(Element el1:els)
						  {
							  String attrh=el1.attr("href");
							  String imgs=null;
							  
							  //System.out.println(attrh);
							  
							  
							  Elements els2=el1.getElementsByTag("img");
							  
							  for(Element el2:els2)
							  {
								  imgs=el2.attr("src");
								  
								  //System.out.println(imgs);
							  }
							  QualifierMatchTVRM(attrh,imgs);
							  
							  
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
		splitter=split[split.length - 1];
		//System.out.println(splitter);
	}
	
	public void Spliturl1(String name)
	{
		String[] split=name.split("\\_|\\-");
		splitter1=split[split.length - 1];
		//System.out.println(splitter);
	}

	
}
