/**
 * 
 */
package com.Nutch.Crawl.Canal;

/**
 * @author surendra
 *
 */
public class CanalMain {

	/**
	 * 
	 */
	public CanalMain() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CanalCNT cnt=new CanalCNT();
		CanalCNTSer cntser=new CanalCNTSer();
		CanalCNTPer cntper=new CanalCNTPer();
		CanalCNTEpsiode ccntep=new CanalCNTEpsiode();
		
		
		//CanalCNTSERIND ccntsi=new CanalCNTSERIND();
		//ccntsi.ContRowsSerIND();
		//ccntp.ContRowsProg();
		
		cntper.ContRowsPer();
		ccntep.ContRowsProgEps();
		cntser.ContRowsSer();
		cnt.ContRows();
		
		SplitFiles.FileSplitS();
		
		
		

	}

}
