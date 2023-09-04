package Kafka;

import java.io.FileWriter;
import java.io.IOException;

public class Demo {
    public static void main(String[] args) throws IOException, InterruptedException {
        FileWriter fw = new FileWriter("Results.txt",true);
        int count=1;//进行几轮
        fw.write("窗口大小\t");
        for(int i=1;i<=count;i++){
            fw.write("第"+i+"轮\t");
        }
        fw.write("均值\t\n");
        //fw.write("窗口大小\t第一轮\t第二轮\t第三轮\t第四轮\t第五轮\t均值\t\n");
        //fw.flush();
        for(int i: new int[]{1,2,5,10,20,50,100,200,500,1000,2000,5000,10000,20000,50000,100000}){//窗口从1-100000
            fw.write((i)+"\t\t");
            Long total= 0L;
            for(int j=1;j<=count;j++){
                //每个跑count轮
                Long record = System.currentTimeMillis();  //mydata里面有timestamp
                Long interval=0L;
                //另开线程  用于生产者  消费者在主线程中
                Thread te = new Thread(){
                    @Override
                    public void run() {
                        try {
                            SingleDataProducer.main(i);  //  开窗的大小
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
                te.start();

                try{
                    DataConsumer.main(args);
                } catch (RuntimeException e) {
                    //todo 2   这里需要另外处理一下   flink中抛出的异常并不是指定的那个    在算子中的异常抛出操作 本身就是个异常 所以最大的异常不是我们想要的
                    te.interrupt();//关掉生产者
                }catch (Exception e2){
                    throw new RuntimeException(e2);
                }

                interval = System.currentTimeMillis() - record;
                total+=interval;
                fw.write((interval)+"\t");
                System.out.println("窗口大小"+i+"第"+j+"轮Result:"+(interval));
            }
            fw.write((Math.round(total*1.0/count))+"\t\n");
            System.out.println("窗口大小"+i+"平均Result:"+(Math.round(total/count)));
            fw.flush();
        }
        fw.close();
    }
}
