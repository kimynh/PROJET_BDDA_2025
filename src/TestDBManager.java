public class TestDBManager {
    public static void main(String[] args) {
        DBConfig config = new DBConfig("DB", 4096, 100, 10, "LRU");
        DiskManager disk = new DiskManager(config);
        BufferManager buffer = new BufferManager(config, disk);
        DBManager db = new DBManager(config);
        db.setManagers(disk, buffer);

        System.out.println("Création...");
       Relation r = new Relation("R", disk, buffer, config);
        r.addColumn("X", "INT");
        r.addColumn("Y", "FLOAT");

                // initialiser l'header si la méthode existe
        try {
            r.calculateNbSlotsPerPage();
            java.lang.reflect.Method init = Relation.class.getMethod("initializeHeaderPage");
            init.invoke(r);
        } catch (NoSuchMethodException ignored) { }
        catch (Throwable ignored) { }

        db.addTable(r);

        System.out.println("Describe...");
        db.DescribeTable("R");

        System.out.println("Sauvegarde...");
        db.SaveState();

        // Redémarrage simulé
        DBManager db2 = new DBManager(config);
        db2.setManagers(disk, buffer);
        db2.LoadState();

        System.out.println("Après rechargement :");
        db2.DescribeTable("R");
    }
}
