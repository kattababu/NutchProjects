 package com.Nutch.Crawl.Canal;

import java.io.File;
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

public class Sample {

	public Sample() {
		// TODO Auto-generated constructor stub
	}

	
	
	/**
	 * @param args
	 */
	
	static FileOutputStream fos=null;
	static PrintStream ps=null;
	static File file=null;
	
	static 
	{
		
		file=new File("/katta/CanalIN/SampleCont.txt");
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try
		{
			fos = new FileOutputStream(file,true);
			ps = new PrintStream(fos);
			  System.setOut(ps);
			
			
		Configuration config=HBaseConfiguration.create();
		HTable ht=new HTable(config,"canal_webpage");
		Scan sc=new Scan();
		ResultScanner rescan=ht.getScanner(sc);
		
		String rownames=null,family=null,qualifier=null;
		String content=null;
		for(Result res = rescan.next(); (res != null); res=rescan.next())
		{
			for(KeyValue kv:res.list())
			{
				
				rownames=Bytes.toString(kv.getRow());
				family=Bytes.toString(kv.getFamily());
				qualifier=Bytes.toString(kv.getQualifier());
				
				if(rownames.equals("ni.com.canal10.www:http/programa/accion-10-fin-de-semana"))
				{
					if(family.equals("f") && qualifier.equals("cnt"))
					{
						content=Bytes.toString(kv.getValue());
											
						System.out.println(content);
						//System.out.println(rownames);
					}
				}
			}
		}
		
		//accion-10-edicion-del-mediodia/accion-10-mediodia--programa-del-24-de-enero-2017-22642
		
		ht.close();
		rescan.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		
		
		
		
		

	

	}

}
