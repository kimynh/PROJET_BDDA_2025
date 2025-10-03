public class TestAlloc {
    public static void main(String[] args) throws Exception {
        DBConfig cfg = DBConfig.LoadDBConfig("src/fichier_conf.json");
        try (DiskManager dm = new DiskManager(cfg)) {
            dm.Init();

            PageId p1 = dm.AllocPage();
            PageId p2 = dm.AllocPage();

            System.out.println("Allocated: " + p1);
            System.out.println("Allocated: " + p2);
            System.out.println("Distinct? " + !p1.equals(p2));
        }
    }
}
