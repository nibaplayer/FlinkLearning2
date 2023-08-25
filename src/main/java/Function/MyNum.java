package Function;

public class MyNum {
    Long count;//记录个数
    double value;//先实现平均值的计算
    Long ts=0L;//时间戳

    public MyNum() {
    }

    public MyNum(Long count, double value) {
        this.count = count;
        this.value = value;
    }

    public MyNum(Long count, double value, Long ts) {
        this.count = count;
        this.value = value;
        this.ts = ts;
    }

    //POJO要求有getter 与 setter
    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public void myprint(){
        System.out.println("("+count+","+value+")");
    }

    @Override
    public String toString() {
        return "MyNum{" +
                "count=" + count +
                ", value=" + value +
                '}';
    }


}
