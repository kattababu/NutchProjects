/**
 * 
 */
package com.Nutch.Crawl.Canal;

import java.io.FileOutputStream;
import java.io.PrintStream;
//import java.util.List;

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

//import com.sun.jersey.server.wadl.generators.resourcedoc.xhtml.Elements;

/**
 * @author surendra
 *
 */
public class CanalMovRich {

	//private static final boolean String = false;



	/**
	 * 
	 */
	public CanalMovRich() {
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

	
	//List<String> list2=null;
	
	static 	{
		//Date date = new Date() ;
		  // SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd'T'HHmmssSSSSSS") ;
		 //long time = System.currentTimeMillis();
			
			//file= new File("/katta/ActionHBO/ACTHBORM_"+dateFormat.format(date)+".txt"); //Your file
			FileStore.RichMedia("richmedia");
			
	 }

	
	
	
	public void QualifierMatch(String name,String imgs)
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
						
						
						
						
						if(rownames.contains(splitter))
						{
							
						
							
							//content=Bytes.toString(kv.getValue());
							//System.out.println(rownames);
							uname=name;
							imag=imgs;
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
		String sk=imag;
		Spliturl1(sk);
		
		
		System.out.print("#<>#"+splitter1.substring(0, splitter1.length()-4)+"#<>#");
		
		
		/// Program _SK//////////// Value
		String p_sk=splitter;
		System.out.print(p_sk+"#<>#");
		
		
		/////////////Program_Type///////////
		System.out.print("Movie"+"#<>#");
		
		
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
		System.out.print(imag+"#<>#");
		
		
/////////////Reference_url///////////
		System.out.print(uname+"#<>#");
		
		
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
	
	
	
	///////////////////////////
	
	
	
	
	public void ImageUrls(String name)
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
							  QualifierMatch(attrh,imgs);
							  
							  
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
		String[] split=name.split("\\/");
		splitter1=split[split.length - 1];
		//System.out.println(splitter);
	}

}

