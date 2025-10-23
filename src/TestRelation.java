public class TestRelation {
    public static void main(String[] args) throws Exception {
        DBConfig cfg = DBConfig.LoadDBConfig("fichier_conf.json");
        DiskManager dm = new DiskManager(cfg);
        BufferManager bm = new BufferManager(cfg, dm);
        dm.Init();

        Relation student = new Relation("Student", dm, bm, cfg);
        student.addColumn("id", "int");
        student.addColumn("name", "string");
        student.addColumn("grade", "float");

        System.out.println(student);
        System.out.println("Record size = " + student.getRecordSize());

        dm.Finish();
        bm.FlushBuffers();
    }
}
