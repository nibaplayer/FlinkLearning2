package Kafka;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class MyData implements Serializable {
    Long count;
    Long timestamp;
    double value;

    public MyData() {
    }

    public MyData(Long count, Long timestamp, double value) {
        this.count = count;
        this.timestamp = timestamp;
        this.value = value;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "MyData{" +
                "count=" + count +
                ", timestamp=" + timestamp +
                ", value=" + value +
                '}';
    }

    public void Deserialization(byte[] message){


        this.count = getLong(message,0);
        this.timestamp = getLong(message,8);
        this.value = getDouble(message,16);

    }

    public byte[] Serialization(){
        byte[] intbytes = getByteArray(this.count);
        byte[] longbytes = getByteArray(this.timestamp);
        byte[] doublebytes = getByteArray(this.value);
        byte[] ans = new byte[24];
        System.arraycopy(intbytes,0,ans,0,8);
        System.arraycopy(longbytes,0,ans,8,8);
        System.arraycopy(doublebytes,0,ans,16,8);
        return ans;
    }


    // char转换为byte[2]数组
    public static byte[] getByteArray(char c) {
        byte[] b = new byte[2];
        b[0] = (byte) ((c & 0xff00) >> 8);
        b[1] = (byte) (c & 0x00ff);
        return b;
    }

    // 从byte数组的index处的连续两个字节获得一个char
    public static char getChar(byte[] arr, int index) {
        return (char) (0xff00 & arr[index] << 8 | (0xff & arr[index + 1]));
    }
    // short转换为byte[2]数组
    public static byte[] getByteArray(short s) {
        byte[] b = new byte[2];
        b[0] = (byte) ((s & 0xff00) >> 8);
        b[1] = (byte) (s & 0x00ff);
        return b;
    }
    // 从byte数组的index处的连续两个字节获得一个short
    public static short getShort(byte[] arr, int index) {
        return (short) (0xff00 & arr[index] << 8 | (0xff & arr[index + 1]));
    }
    // int转换为byte[4]数组
    public static byte[] getByteArray(int i) {
        byte[] b = new byte[4];
        b[0] = (byte) ((i & 0xff000000) >> 24);
        b[1] = (byte) ((i & 0x00ff0000) >> 16);
        b[2] = (byte) ((i & 0x0000ff00) >> 8);
        b[3] = (byte)  (i & 0x000000ff);
        return b;
    }
    // 从byte数组的index处的连续4个字节获得一个int
    public static int getInt(byte[] arr, int index) {
        return 	(0xff000000 	& (arr[index+0] << 24))  |
                (0x00ff0000 	& (arr[index+1] << 16))  |
                (0x0000ff00 	& (arr[index+2] << 8))   |
                (0x000000ff 	&  arr[index+3]);
    }
    // float转换为byte[4]数组
    public static byte[] getByteArray(float f) {
        int intbits = Float.floatToIntBits(f);//将float里面的二进制串解释为int整数
        return getByteArray(intbits);
    }
    // 从byte数组的index处的连续4个字节获得一个float
    public static float getFloat(byte[] arr, int index) {
        return Float.intBitsToFloat(getInt(arr, index));
    }
    // long转换为byte[8]数组
    public static byte[] getByteArray(long l) {
        byte b[] = new byte[8];
        b[0] = (byte)  (0xff & (l >> 56));
        b[1] = (byte)  (0xff & (l >> 48));
        b[2] = (byte)  (0xff & (l >> 40));
        b[3] = (byte)  (0xff & (l >> 32));
        b[4] = (byte)  (0xff & (l >> 24));
        b[5] = (byte)  (0xff & (l >> 16));
        b[6] = (byte)  (0xff & (l >> 8));
        b[7] = (byte)  (0xff & l);
        return b;
    }
    // 从byte数组的index处的连续8个字节获得一个long
    public static long getLong(byte[] arr, int index) {
        return 	(0xff00000000000000L 	& ((long)arr[index+0] << 56))  |
                (0x00ff000000000000L 	& ((long)arr[index+1] << 48))  |
                (0x0000ff0000000000L 	& ((long)arr[index+2] << 40))  |
                (0x000000ff00000000L 	& ((long)arr[index+3] << 32))  |
                (0x00000000ff000000L 	& ((long)arr[index+4] << 24))  |
                (0x0000000000ff0000L 	& ((long)arr[index+5] << 16))  |
                (0x000000000000ff00L 	& ((long)arr[index+6] << 8))   |
                (0x00000000000000ffL 	&  (long)arr[index+7]);
    }
    // double转换为byte[8]数组
    public static byte[] getByteArray(double d) {
        long longbits = Double.doubleToLongBits(d);
        return getByteArray(longbits);
    }
    // 从byte数组的index处的连续8个字节获得一个double
    public static double getDouble(byte[] arr, int index) {
        return Double.longBitsToDouble(getLong(arr, index));
    }

}
