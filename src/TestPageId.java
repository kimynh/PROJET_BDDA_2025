public class TestPageId {
    public static void main(String[] args) {
        PageId p1 = new PageId(0, 1);
        PageId p2 = new PageId(0, 1);
        PageId p3 = new PageId(1, 0);

        System.out.println(p1);  // should print PageId(fileIdx=0, pageIdx=1)
        System.out.println("p1 equals p2 ? " + p1.equals(p2)); // true
        System.out.println("p1 equals p3 ? " + p1.equals(p3)); // false
    }
}
