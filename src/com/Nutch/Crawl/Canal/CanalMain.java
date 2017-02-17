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
		
		cntser.ContRowsSer();
		cnt.ContRows();
		
		
		

	}

}
