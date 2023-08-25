package MydefinedStream;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class readFromSocket {
    static ArrayList<Socket> online = new ArrayList<Socket>();

    public static void main(String[] args) throws Exception {
        //启动服务器
        ServerSocket sever = new ServerSocket(7777);
        //先实现单一的连接
        Socket accept = sever.accept();//它会阻塞   知道是谁连上server
        System.out.println("Connecting to server socket " + accept.getInetAddress().getHostAddress() + ':' + accept.getPort());
        //online.add(accept);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> ds = env.addSource(new MySocketStreamFunction("\n",0,accept),"MySocketStream");
        //SocketTextStreamFunction中有个新建立连接的过程   现在不需要建立直接使用accept替代
        ds.print();


        env.execute();
    }
}
