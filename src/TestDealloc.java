public class TestDealloc {
    public static void main(String[] args) throws Exception {
        DBConfig cfg = DBConfig.LoadDBConfig("src/fichier_conf.json");
        try (DiskManager dm = new DiskManager(cfg)) {
            dm.Init();

            // allocate 2 pages
            PageId p1 = dm.AllocPage();
            PageId p2 = dm.AllocPage();
            System.out.println("Allocated: " + p1 + " and " + p2);

            // free the first one
            dm.DeallocPage(p1);
            System.out.println("Deallocated: " + p1);

            // allocate again → should reuse p1
            PageId p3 = dm.AllocPage();
            System.out.println("Reallocated: " + p3);

            System.out.println("p3 equals p1 ? " + p3.equals(p1));
        }
    }
}
