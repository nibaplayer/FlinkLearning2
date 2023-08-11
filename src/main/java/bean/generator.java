package bean;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;

public class generator {
    public static void main(String[] args) throws IOException {
        ServerSocket serverSocket = new ServerSocket(7777);
        Socket socket = serverSocket.accept();
        try{
            OutputStream outputStream = socket.getOutputStream();
            PrintStream printStream = new PrintStream(outputStream);
            while(true){
                int a=(int)(Math.random()*3);//生成一个0，1，2的随机数

                printStream.println("s"+a+","+a+","+a);

                Thread.sleep(10);//每隔1秒发送一组数据
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
