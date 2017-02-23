import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;


public class test {
public static void main(String args[]) throws JSchException {

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
    String sourceFile = "/katta/CanalIN/1_CanalIN_Terminal_movie_20170223T11313500637.queries", targetFile = "/Users/hrb/sathwick_nutch_files";
    try {
    	

        cFTP.put(sourceFile , targetFile );
    } catch (SftpException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    }

    cFTP.disconnect();
    session.disconnect();
}
}