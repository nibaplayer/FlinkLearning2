package Function;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;

public class generator {
    public static void main(String[] args) throws IOException {
        //Integer intervals = Integer.valueOf(args[0]);
        ServerSocket serverSocket = new ServerSocket(7777);
        Socket socket = serverSocket.accept();//这玩意只能连一遍   之后要重启

        OutputStream outputStream = socket.getOutputStream();
        PrintStream printStream = new PrintStream(outputStream);
        for(long i=0;i<1000000000;i++){//发送10e组数据
            printStream.println("s"+(int)(10*Math.random())+","+System.currentTimeMillis()+","+(int)(10*Math.random()));
        }
//        try {
//            OutputStream outputStream = socket.getOutputStream();
//            PrintStream printStream = new PrintStream(outputStream);
//
//            while (true) {
//                //WaterSensor a = new WaterSensor("s"+(int)(10*Math.random()), System.currentTimeMillis(),(int)(10*Math.random()));//生成一个随机数
//
//                printStream.println("s"+(int)(10*Math.random())+","+System.currentTimeMillis()+","+(int)(10*Math.random()));
//
//                Thread.sleep(intervals);//每隔0.1秒发送一组数据
//            }
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//    }

    }
}
