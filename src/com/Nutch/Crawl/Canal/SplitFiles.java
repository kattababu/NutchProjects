/**
 * 
 */
package com.Nutch.Crawl.Canal;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
//import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.io.FileUtils;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
//import com.jcraft.jsch.SftpException;

/**
 * @author surendra
 *
 */
public class SplitFiles {

	/**
	 * 
	 */
	public SplitFiles() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	static String DestSouc=null;
	public static void FileSplitS() {
		// TODO Auto-generated method stub
		
		
		 try{  
			 
			 File dir = new File("/katta/CanalIN/");
				String[] extensions = new String[] { "queries" };
				//System.out.println("Getting all .txt and .jsp files in " + dir.getCanonicalPath()
					//	+ " including those in subdirectories");
				List<File> files = (List<File>) FileUtils.listFiles(dir, extensions, true);
				for (File file : files) {
					System.out.println("file: " + file.getCanonicalPath());
					
					String fnamespath=file.getCanonicalPath();
					String filename=file.getName();
					RealSplit(fnamespath,filename);
				
				}
				
			 /*
			  // Reading file and getting no. of files to be generated  
			  String inputfile = file.getName();//  Source File Name.  
			  System.out.println(inputfile);
			  double nol = 3.0; //  No. of lines to be split and saved in each output file.  
			  File Infile = new File(inputfile);  
			  Scanner scanner = new Scanner(Infile);  
			  int count = 0;  
			  while (scanner.hasNextLine())   
			  {  
			   scanner.nextLine();  
			   count++;  
			  }  
			  System.out.println("Lines in the file: " + count);     // Displays no. of lines in the input file.  

			  double temp = (count/nol);  
			  int temp1=(int)temp;  
			  int nof=0;  
			  if(temp1==temp)  
			  {  
			   nof=temp1;  
			  }  
			  else  
			  {  
			   nof=temp1+1;  
			  }  
			  System.out.println("No. of files to be generated :"+nof); 
				// Displays no. of files to be generated.  

			  //-----------------------------------------------------------------------

	
		 
		 // Actual splitting of file into smaller files  

		  FileInputStream fstream = new FileInputStream(inputfile); DataInputStream in = new DataInputStream(fstream);  

		  BufferedReader br = new BufferedReader(new InputStreamReader(in)); String strLine;  

		  for (int j=1;j<=nof;j++)  
		  {  
		   FileWriter fstream1 = new FileWriter("/katta/CanalIN/"+"_"+j+".txt");     // Destination File Location  
		   BufferedWriter out = new BufferedWriter(fstream1);   
		   for (int i=1;i<=nol;i++)  
		   {  
		    strLine = br.readLine();   
		    if (strLine!= null)  
		    {  
		     out.write(strLine);   
		     if(i!=nol)  
		     {  
		      out.newLine();  
		     }  
		    }  
		   }  
		   out.close();  
		  }  

		  in.close();  
		  */
		 
		 }
				
		 catch (Exception e)  
		 {  
		  System.err.println("Error: " + e.getMessage());  
		 }  
		 
		 

		}  
		
	
	
	public static  void RealSplit(String name,String fname)
	{
		try
		{
			// Reading file and getting no. of files to be generated  
			  String inputfile = name;//  Source File Name.  
			  System.out.println(inputfile);
			  double nol = 1000.0; //  No. of lines to be split and saved in each output file.  
			  File Infile = new File(inputfile);  
			  Scanner scanner = new Scanner(Infile);  
			  int count = 0;  
			  while (scanner.hasNextLine())   
			  {  
			   scanner.nextLine();  
			   count++;  
			  }  
			 // System.out.println("Lines in the file: " + count);     // Displays no. of lines in the input file.  

			  double temp = (count/nol);  
			  int temp1=(int)temp;  
			  int nof=0;  
			  if(temp1==temp)  
			  {  
			   nof=temp1;  
			  }  
			  else  
			  {  
			   nof=temp1+1;  
			  }  
			 // System.out.println("No. of files to be generated :"+nof); 
				// Displays no. of files to be generated.  

			  //-----------------------------------------------------------------------
			  
//			   Remote Connection Files://///////////////////
			  
			  JSch jsch = new JSch();
			    Session session = jsch.getSession("hrb", "office.headrun.com");//.getSession("hrb", "10.152.232.1", 22); //port is usually 22
			    session.setPassword("satishdhawan16!");
			    java.util.Properties config = new java.util.Properties(); 
			    config.put("StrictHostKeyChecking", "no");
			    session.setConfig(config);
			    //session.
			  // Session.put("StrictHostKeyChecking", "no");
			    session.connect();
			    Channel channel = session.openChannel("sftp");
			    channel.connect();
			    ChannelSftp cFTP = (ChannelSftp) channel;
			    JSch.setConfig("StrictHostKeyChecking", "no");
			    cFTP.cd("/Users/hrb/sathwick_nutch_files");
			 

	
		 
		 // Actual splitting of file into smaller files  

		  FileInputStream fstream = new FileInputStream(inputfile); DataInputStream in = new DataInputStream(fstream);  

		  BufferedReader br = new BufferedReader(new InputStreamReader(in)); String strLine;  
		  
		 //String DestSource= 

		  for (int j=1;j<=nof;j++)  
		  {  
			  DestSouc=j+"_"+fname;
			  
			  OutputStream strm = cFTP.put(DestSouc);
		  //FileWriter fstream1 = new FileWriter(DestSouc); 
		  //cFTP.// Destination File Location  
		   BufferedWriter out = new BufferedWriter(new PrintWriter(strm));   
		   for (int i=1;i<=nol;i++)  
		   {  
		    strLine = br.readLine();   
		    if (strLine!= null)  
		    {  
		     out.write(strLine);   
		     if(i!=nol)  
		     {  
		      out.newLine();  
		     }  
		    }  
		   }  
		   out.close();  
		  }  

		  in.close();  
		  cFTP.disconnect();
		    session.disconnect();
		  
		 
		 }
				
		 catch (Exception e)  
		 { 
			 
			 e.printStackTrace();
		  //System.err.println("Error: " + e.getMessage());  
		 }  
		 
		 /*
		 finally
		 {
			 try
			 {
			 
			 JSch jsch = new JSch();
			    Session session = jsch.getSession("hrb", "office.headrun.com");//.getSession("hrb", "10.152.232.1", 22); //port is usually 22
			    session.setPassword("satishdhawan16!");
			    java.util.Properties config = new java.util.Properties(); 
			    config.put("StrictHostKeyChecking", "no");
			    session.setConfig(config);
			    //session.
			  // Session.put("StrictHostKeyChecking", "no");
			    session.connect();
			    Channel channel = session.openChannel("sftp");
			    channel.connect();
			    ChannelSftp cFTP = (ChannelSftp) channel;
			    JSch.setConfig("StrictHostKeyChecking", "no");
			    String sourceFile = DestSouc, targetFile = "/Users/hrb/sathwick_nutch_files";
			    

			        cFTP.put(sourceFile , targetFile );
			        
			        cFTP.disconnect();
				    session.disconnect();
			    } catch (SftpException e) {
			        // TODO Auto-generated catch block
			        e.printStackTrace();
			    }
			    catch (Exception e) {
			        // TODO Auto-generated catch block
			        e.printStackTrace();
			    }

			    
			}
			*/

		 }

	}
	



