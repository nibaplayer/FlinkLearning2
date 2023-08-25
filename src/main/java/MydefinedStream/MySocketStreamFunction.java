package MydefinedStream;

import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;

import static org.apache.flink.util.Preconditions.checkArgument;

public class MySocketStreamFunction implements SourceFunction<String> {//这个string有什么作用

    private static final long serialVersionUID = 1L;//还需要找到这个被哪个函数调用进行序列化了 这需要将flink源码更改后部署到结点 或许使用kafka作为broker会更好
    //头大

    private static final Logger LOG = LoggerFactory.getLogger(SocketTextStreamFunction.class);

    /** Default delay between successive connection attempts. */
    private static final int DEFAULT_CONNECTION_RETRY_SLEEP = 500;

    /** Default connection timeout when connecting to the server socket (infinite). */
    private static final int CONNECTION_TIMEOUT_TIME = 0;

//    private final String hostname; 直接用accpet
//    private final int port;
    private final String delimiter;
    private final long maxNumRetries;
    private final long delayBetweenRetries;

    private final transient Socket currentSocket;

    private volatile boolean isRunning = true;


    public MySocketStreamFunction(String delimiter, long maxNumRetries, Socket currentSocket) {
        this(delimiter,maxNumRetries,DEFAULT_CONNECTION_RETRY_SLEEP,currentSocket);
    }

    public MySocketStreamFunction(
            String delimiter,
            long maxNumRetries,
            long delayBetweenRetries,
            Socket currentSocket) {
        checkArgument(
                maxNumRetries >= -1,
                "maxNumRetries must be zero or larger (num retries), or -1 (infinite retries)");
        checkArgument(delayBetweenRetries >= 0, "delayBetweenRetries must be zero or positive");
        this.delimiter = delimiter;
        this.maxNumRetries = maxNumRetries;
        this.delayBetweenRetries = delayBetweenRetries;
        this.currentSocket = currentSocket;
        //currentSocket.getPort();
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        final StringBuilder buffer = new StringBuilder();
        long attempt = 0;

        while (isRunning) {

            try (Socket socket = currentSocket) {
                //currentSocket = socket;
                //currentSocket.getInetAddress().getHostAddress();
                LOG.info("Connecting to server socket " + socket.getInetAddress().getHostAddress() + ':' + socket.getPort());
                //socket.connect(new InetSocketAddress(hostname, port), CONNECTION_TIMEOUT_TIME);
                try (BufferedReader reader =
                             new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                    char[] cbuf = new char[8192];
                    int bytesRead;
                    while (isRunning && (bytesRead = reader.read(cbuf)) != -1) {
                        buffer.append(cbuf, 0, bytesRead);
                        int delimPos;
                        while (buffer.length() >= delimiter.length()
                                && (delimPos = buffer.indexOf(delimiter)) != -1) {
                            String record = buffer.substring(0, delimPos);
                            // truncate trailing carriage return
                            if (delimiter.equals("\n") && record.endsWith("\r")) {
                                record = record.substring(0, record.length() - 1);
                            }
                            ctx.collect(record);
                            buffer.delete(0, delimPos + delimiter.length());
                        }
                    }
                }
            }

            // if we dropped out of this loop due to an EOF, sleep and retry
            if (isRunning) {
                attempt++;
                if (maxNumRetries == -1 || attempt < maxNumRetries) {
                    LOG.warn(
                            "Lost connection to server socket. Retrying in "
                                    + delayBetweenRetries
                                    + " msecs...");
                    Thread.sleep(delayBetweenRetries);
                } else {
                    // this should probably be here, but some examples expect simple exists of the
                    // stream source
                    // throw new EOFException("Reached end of stream and reconnects are not
                    // enabled.");
                    break;
                }
            }
        }

        // collect trailing data
        if (buffer.length() > 0) {
            ctx.collect(buffer.toString());
        }
    }

    @Override
    public void cancel() {
        isRunning = false;

        // we need to close the socket as well, because the Thread.interrupt() function will
        // not wake the thread in the socketStream.read() method when blocked.
        Socket theSocket = this.currentSocket;
        if (theSocket != null) {
            IOUtils.closeSocket(theSocket);
        }
    }
}
