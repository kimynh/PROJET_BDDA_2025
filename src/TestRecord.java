import java.util.Arrays;

public class TestRecord {
    public static void main(String[] args) {
        Record r1 = new Record(new String[]{"1", "Alice", "17.5"});
        System.out.println("r1: " + r1);

        Record r2 = new Record();
        r2.addValue("2");
        r2.addValue("Bob");
        r2.addValue("15.8");
        System.out.println("r2: " + r2);

        System.out.println("r1 has " + r1.size() + " values.");
        System.out.println("Second value of r2 = " + r2.getValue(1));
    }
}

