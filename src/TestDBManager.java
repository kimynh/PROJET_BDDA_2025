public class TestDBManager {
    public static void main(String[] args) {
        DBConfig config = new DBConfig("testdb", 4096, 100, 10, "LRU");
        DBManager db = new DBManager(config);

        System.out.println("Création...");
        Relation r = new Relation("R", null, null, config);
        r.addColumn("X", "INT");
        r.addColumn("Y", "FLOAT");
        db.addTable(r);

        System.out.println("Describe...");
        db.DescribeTable("R");

        System.out.println("Sauvegarde...");
        db.SaveState();

        // Redémarrage simulé
        DBManager db2 = new DBManager(config);
        db2.LoadState();

        System.out.println("Après rechargement :");
        db2.DescribeTable("R");
    }
}
