package Kafka.Test;

public class ExceptionTest {
    public static void main(String[] args) {
        try {
            throw new Exception("test");
        }catch (Exception e ){
            System.out.println(e.getMessage());
        }
    }
}
