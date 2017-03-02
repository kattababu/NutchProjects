
import java.util.Timer;
import java.util.TimerTask;

public class ScheduleClass {

    private static Timer timer = new Timer();

    public static void main(String...args){
           timer.schedule (new MyTask(),0,1000*60*15);
    }
}

class MyTask extends TimerTask {
        public void run() {
            System.out.println("hello");
        }
    }