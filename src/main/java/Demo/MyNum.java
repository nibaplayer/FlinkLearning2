package Demo;

public class MyNum {
    Long count;//记录个数
    double value;//先实现平均值的计算

    public MyNum() {
    }

    public MyNum(Long count, double value) {
        this.count = count;
        this.value = value;
    }

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

    public void myprint(){
        System.out.println("("+count+","+value+")");
    }
}
