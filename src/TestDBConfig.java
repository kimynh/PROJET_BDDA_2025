 public class TestDBConfig {
    public static void main(String[] args) throws Exception {
        DBConfig cfg = DBConfig.LoadDBConfig("src/fichier_conf.json");
        System.out.println(cfg);
    }
}


