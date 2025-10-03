import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class DBConfig {
    private String dbpath;
    private int pagesize;
    private int dm_maxfilecount;

    // Constructor
    public DBConfig(String dbpath, int pagesize, int dm_maxfilecount) {
        this.dbpath = dbpath;
        this.pagesize = pagesize;
        this.dm_maxfilecount = dm_maxfilecount;
    }

    // Getters
    public String getDbpath() {
        return dbpath;
    }

    public int getPagesize() {
        return pagesize;
    }

    public int getDm_maxfilecount() {
        return dm_maxfilecount;
    }

    // Setters
    public void setDbpath(String dbpath) {
        this.dbpath = dbpath;
    }

    public void setPagesize(int pagesize) {
        this.pagesize = pagesize;
    }

    public void setDm_maxfilecount(int dm_maxfilecount) {
        this.dm_maxfilecount = dm_maxfilecount;
    }

    // Methods
    public static DBConfig LoadDBConfig(String fichierConfig) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(fichierConfig))) {
            String line;
            String dbpath = null;
            Integer pagesize = null;
            Integer dm_maxfilecount = null;

            while ((line = reader.readLine()) != null) {
                line = line.trim();

                // Ignore empty lines or comments
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }

                // Expected format: key = value
                if (line.startsWith("dbpath")) {
                    String[] parts = line.split("=", 2);
                    if (parts.length == 2) {
                        dbpath = parts[1].trim();
                    }
                } else if (line.startsWith("pagesize")) {
                    String[] parts = line.split("=", 2);
                    if (parts.length == 2) {
                        pagesize = Integer.parseInt(parts[1].trim());
                    }
                } else if (line.startsWith("dm_maxfilecount")) {
                    String[] parts = line.split("=", 2);
                    if (parts.length == 2) {
                        dm_maxfilecount = Integer.parseInt(parts[1].trim());
                    }
                }
            }

            if (dbpath == null || pagesize == null || dm_maxfilecount == null) {
                throw new IllegalArgumentException("Le fichier de configuration est incomplet !");
            }

            return new DBConfig(dbpath, pagesize, dm_maxfilecount);
        }
    }

    @Override
    public String toString() {
        return "DBConfig{dbpath='" + dbpath + "', pagesize=" + pagesize +
               ", dm_maxfilecount=" + dm_maxfilecount + "}";
    }
}
