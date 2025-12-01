
import java.io.*;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

public class DBManager{
    private final DBConfig config;
    private final LinkedHashMap<String,Relation> tables;
    private DiskManager diskManager;
    private BufferManager bufferManager;

//Constructeur 
    public DBManager(DBConfig config){
        this.config=config;
        this.tables =new LinkedHashMap<>();
    }
    
    // Méthode pour définir les managers après construction
    public void setManagers(DiskManager diskManager, BufferManager bufferManager) {
        this.diskManager = diskManager;
        this.bufferManager = bufferManager;
    }
    
// Methode Pour ajouter une table
    public void addTable(Relation tab){
        if(tab==null || tab.getName()==null) return;
        tables.put(tab.getName(),tab);
    }
// Methode Pour recuperer une table

    public Relation getTable(String nomTable){
        if (nomTable==null) return null;
        return tables.get(nomTable);
    }
// Methode Pour supprimer une table
    public void RemoveTable(String nomTable){
        if (nomTable==null) return;
        tables.remove(nomTable);
    }
// Methode Pour supprimer toutes les tables
    public void RemoveAllTables(){
        tables.clear();
    }
    // Afficher le schema d'une table au format nomTable (colonne1:type1, colonne2:type2, ...)

    public void DescribeTable(String nomTable) {
        Relation r = tables.get(nomTable);
        if (r == null) return;
        StringBuilder sb = new StringBuilder();
        sb.append(r.getName()).append(" (");
        List<String> names = r.getColumnNames();
        List<String> types = r.getColumnTypes();
        for (int i = 0; i < names.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append(names.get(i)).append(":").append(types.get(i));
        }
        sb.append(")");
        System.out.println(sb.toString());
    }
    // Afficher le schema de toutes les tables

    public void DescribeAllTables(){
        for (String  nomtable : tables.keySet()){
            DescribeTable(nomtable);
      
        }
    }

    // Methode Pour sauvegarder l'etat de la base de donnees dans un fichier

     public void SaveState() {
        File dir = new File(config.getDbpath());
        if (!dir.exists()) dir.mkdirs();
        File saveFile = new File(dir, "database.save");

        try (BufferedWriter w = new BufferedWriter(new FileWriter(saveFile))) {
            for (Relation r : tables.values()) {
                w.write("START_TABLE");
                w.newLine();
                w.write("name=" + r.getName());
                w.newLine();
                // columns
                StringBuilder cols = new StringBuilder();
                List<String> names = r.getColumnNames();
                List<String> types = r.getColumnTypes();
                for (int i = 0; i < names.size(); i++) {
                    if (i > 0) cols.append(",");
                    cols.append(names.get(i)).append(":").append(types.get(i));
                }
                w.write("columns=" + cols.toString());
                w.newLine();
                // header page id
                PageId hp = r.getHeaderPageId();
                if (hp == null) {
                    w.write("header=null");
                                    } else {
                    w.write("header=" + hp.getFileIdx() + "," + hp.getPageIdx());
                }
                w.newLine();
                w.write("END_TABLE");
                w.newLine();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to save DB state: " + e.getMessage(), e);
        }
    }

    public void LoadState() {
        File dir = new File(config.getDbpath());
        File saveFile = new File(dir, "database.save");
        if (!saveFile.exists()) return;

        LinkedHashMap<String, Relation> loaded = new LinkedHashMap<>();

        try (BufferedReader r = new BufferedReader(new FileReader(saveFile))) {
            String line;
            String name = null;
            String columns = null;
            String header = null;
            while ((line = r.readLine()) != null) {
                if (line.equals("START_TABLE")) {
                    name = null;
                    columns = null;
                    header = null;
                } else if (line.startsWith("name=")) {
                    name = line.substring("name=".length());
                } else if (line.startsWith("columns=")) {
                    columns = line.substring("columns=".length());
                } else if (line.startsWith("header=")) {
                    header = line.substring("header=".length());
                 } else if (line.equals("END_TABLE")) {
                    if (name == null) continue;
                    // create Relation with proper managers if available
                    Relation rel = new Relation(name, diskManager, bufferManager, config);
                    if (columns != null && !columns.isEmpty()) {
                        String[] cols = columns.split(",");
                        for (String c : cols) {
                            int idx = c.indexOf(':');
                            if (idx > 0) {
                                String cname = c.substring(0, idx);
                                String ctype = c.substring(idx + 1);
                                rel.addColumn(cname, ctype);
                            }
                        }
                        rel.calculateNbSlotsPerPage();
                    }
                    boolean headerSet = false;
                    // set headerPageId via reflection if present
                    if (header != null && !header.equals("null")) {
                        String[] parts = header.split(",");
                        if (parts.length == 2) {
                            try {
                                 int fileIdx = Integer.parseInt(parts[0]);
                                int pageIdx = Integer.parseInt(parts[1]);
                                PageId pid = new PageId(fileIdx, pageIdx);
                                try {
                                    Field f = Relation.class.getDeclaredField("headerPageId");
                                    f.setAccessible(true);
                                    f.set(rel, pid);
                                    headerSet = true;
                                } catch (NoSuchFieldException | IllegalAccessException ex) {
                                    // ignore reflection issues, continue without header set
                                }
                            } catch (NumberFormatException ignore) { }
                        }
                    }
                    
                                        // if header was not set from the file, try to initialize a header page if possible
                    if (!headerSet) {
                        // Prefer calling a proper initializer if Relation provides it
                        try {
                            if (diskManager != null && bufferManager != null) {
                                try {
                                    java.lang.reflect.Method init = Relation.class.getMethod("initializeHeaderPage");
                                    init.invoke(rel);
                                    headerSet = true;
                                } catch (NoSuchMethodException ns) {
                                    // method not present, fall back to field set
                                }
                            }
                        } catch (Throwable t) {
                            // ignore init errors and fall back
                        }

                        if (!headerSet) {
                            // final fallback: set a placeholder PageId so relation is consistent
                            try {
                                Field f = Relation.class.getDeclaredField("headerPageId");
                                f.setAccessible(true);
                                f.set(rel, new PageId(-1, -1));
                                headerSet = true;
                            } catch (NoSuchFieldException | IllegalAccessException ex) {
                                // if even this fails, leave relation without header and continue
                            }
                        }
                    }
                    
                    loaded.put(name, rel);
                }
            }
            // replace current tables with loaded
            tables.clear();
            tables.putAll(loaded);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load DB state: " + e.getMessage(), e);
        }
    }

    // For tests or external use
    public Collection<Relation> getAllRelations() {
        return Collections.unmodifiableCollection(tables.values());
    }

        public Set<String> getAllTableNames() {
        return Collections.unmodifiableSet(tables.keySet());
    }
}