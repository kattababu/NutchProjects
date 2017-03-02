import java.io.*;

public class JavaRunCommand {

    public static void main(String args[]) {

        String cmdline="hrb@office.headrun.com";
        String passwd="satishdhawan16!";
        String cp=cmdline+passwd;
        String cmd[]={"/bin/bash","-c",cp};
        
        
      // String filepath="/katta/CanalIN/FileToDb.sh";

        try {
        	 Process p = Runtime.getRuntime().exec(cmd);
             p.waitFor();
             InputStream in = p.getInputStream();
             ByteArrayOutputStream baos = new ByteArrayOutputStream();
              
             int c = -1;
             while((c = in.read()) != -1)
             {
                 baos.write(c);
                // System.out.println(c);
             }
              
             String response = new String(baos.toByteArray());
             System.out.println("Response From Exe : "+response);
            
	           }
        catch (Exception e) {
            e.printStackTrace();
                   }
    }
}