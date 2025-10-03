public class TestDiskInit {
    public static void main(String[] args) throws Exception {
         DBConfig cfg = DBConfig.LoadDBConfig("src/fichier_conf.json");
        try (DiskManager dm = new DiskManager(cfg)) {
            dm.Init();
            System.out.println("DiskManager Init OK (BinData created + Data0.bin ready)");
        }
    }
}

