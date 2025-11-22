public class InitDisk {
    public static void main(String[] args) throws Exception {
        DBConfig cfg = DBConfig.LoadDBConfig("fichier_conf.json");
        try (DiskManager dm = new DiskManager(cfg)) {
            dm.Init();
            System.out.println("Disque initialisé avec succès");
        }
    }
}
