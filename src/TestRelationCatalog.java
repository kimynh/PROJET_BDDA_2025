public class TestRelationCatalog {
    public static void main(String[] args) {
        Relation student = new Relation("Student");
        student.addColumn("id", "int");
        student.addColumn("name", "string");
        student.addColumn("grade", "float");

        Relation teacher = new Relation("Teacher");
        teacher.addColumn("id", "int");
        teacher.addColumn("name", "string");

        RelationCatalog catalog = new RelationCatalog();
        catalog.addRelation(student);
        catalog.addRelation(teacher);

        System.out.println(catalog);

        Relation found = catalog.getRelation("Student");
        System.out.println("Found relation: " + found);
    }
}

