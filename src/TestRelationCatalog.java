import java.io.IOException;

public class TestRelationCatalog {
    public static void main(String[] args) throws IOException {
        DBConfig cfg = DBConfig.LoadDBConfig("src/fichier_conf.json");
        DiskManager dm = new DiskManager(cfg);
        BufferManager bm = new BufferManager(cfg, dm);
        dm.Init();

        RelationCatalog catalog = new RelationCatalog(dm, bm, cfg);

        Relation r1 = new Relation("students");
        r1.addColumn("id", "int");
        r1.addColumn("name", "string");
        r1.addColumn("grade", "float");

        catalog.addRelation(r1);

        System.out.println(catalog);

        dm.Finish();
        bm.FlushBuffers();
    }
}
