import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class DBConfig {
    private String dbpath;

    //Constructor
    public DBConfig(String dbpath) {
        this.dbpath = dbpath;
    }

    //getter
    public String getDbpath() {
        return dbpath;
    }

    //setter
    public void setDbpath(String dbpath) {
        this.dbpath = dbpath;
    }

    //Methods
    public static DBConfig LoadDBConfig(String fichierConfig) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(fichierConfig))) {
            String line;
            String dbpath = null;

            while ((line = reader.readLine()) != null) {
                line = line.trim();

                // Ignorer les lignes vides ou commentaires
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }

                // Exemple attendu : dbpath = ../DB
                if (line.startsWith("dbpath")) {
                    String[] parts = line.split("=", 2);
                    if (parts.length == 2) {
                        dbpath = parts[1].trim();
                    }
                }
            }

            if (dbpath == null) {
                throw new IllegalArgumentException("Le fichier de configuration ne contient pas 'dbpath'.");
            }

            return new DBConfig(dbpath);
        }
    }

    @Override
    public String toString() {
        return "DBConfig{dbpath='" + dbpath + "'}";
    }
}