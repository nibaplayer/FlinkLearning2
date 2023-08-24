package Function;

import java.io.FileWriter;
import java.io.IOException;

public class filetest {
    public static void main(String[] args) throws IOException {
        FileWriter fw = new FileWriter("Results.txt",true);
        fw.write("窗口大小\t第一轮\t第二轮\t第三轮\t第四轮\t第五轮\t均值\t\n");
        fw.close();
    }
}
