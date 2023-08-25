package MydefinedStream;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
        //online.add(accept);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> ds = env.socketTextStream(accept.getInetAddress().getHostAddress(), accept.getPort());
        ds.print();


        env.execute();
    }
}
