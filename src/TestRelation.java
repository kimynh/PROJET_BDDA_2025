public class TestRelation {
    public static void main(String[] args) {
        Relation student = new Relation("Student");
        student.addColumn("id", "int");
        student.addColumn("name", "string");
        student.addColumn("grade", "float");

        System.out.println(student);
        System.out.println("Record size = " + student.getRecordSize());
    }
}
