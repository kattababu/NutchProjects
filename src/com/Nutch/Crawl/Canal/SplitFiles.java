/**
 * 
 */
package com.Nutch.Crawl.Canal;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.io.FileUtils;

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

	
		 
		 // Actual splitting of file into smaller files  

		  FileInputStream fstream = new FileInputStream(inputfile); DataInputStream in = new DataInputStream(fstream);  

		  BufferedReader br = new BufferedReader(new InputStreamReader(in)); String strLine;  

		  for (int j=1;j<=nof;j++)  
		  {  
		   FileWriter fstream1 = new FileWriter("/katta/CanalIN/"+j+"_"+fname);     // Destination File Location  
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
				 //fname.
			 }
		 }
*/
	}
	


}
