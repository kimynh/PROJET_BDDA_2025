import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RelationCatalog {
    private final List<Relation> relations;
    private final DiskManager diskManager;
    private final BufferManager bufferManager;
    private final DBConfig config;
    private final HeapFile catalogFile;

    // Constructor: initializes managers and loads catalog if exists
    public RelationCatalog(DiskManager dm, BufferManager bm, DBConfig cfg) throws IOException {
        this.diskManager = dm;
        this.bufferManager = bm;
        this.config = cfg;
        this.relations = new ArrayList<>();

        // System catalog relation
        Relation catalogRelation = new Relation("catalog", dm, bm, cfg);
        catalogRelation.addColumn("name", "string");

        this.catalogFile = new HeapFile(catalogRelation, dm, bm);

        // Load catalog data from disk (if any)
        loadCatalog();
    }

    // ------------------- Add a new relation -------------------
    public void addRelation(Relation r) throws IOException {
        // Avoid duplicates
        if (getRelation(r.getName()) != null) {
            System.out.println("⚠️ Relation already exists: " + r.getName());
            return;
        }

        relations.add(r);

        // Persist this relation name on disk
        Record rec = new Record(List.of(r.getName()));
        PageId pid = catalogFile.createNewPage();
        catalogFile.writeRecord(pid, rec, 0);

        System.out.println("✅ Relation added and saved: " + r.getName());
    }

    // ------------------- Load existing catalog from disk -------------------
    private void loadCatalog() throws IOException {
        System.out.println("🔄 Loading catalog from disk...");

        try {
            // Scan the first 10 pages (simplified)
            for (int pageIdx = 1; pageIdx < 10; pageIdx++) {
                PageId pid = new PageId(0, pageIdx);
                byte[] page = bufferManager.GetPage(pid);
                ByteBuffer bb = ByteBuffer.wrap(page);

                // Read one relation name (20 bytes max)
                byte[] bytes = new byte[20];
                bb.get(bytes);
                String name = new String(bytes).trim();

                if (!name.isEmpty()) {
                    // Only add if not already in memory
                    if (getRelation(name) == null) {
                        Relation r = new Relation(name, diskManager, bufferManager, config);
                        relations.add(r);
                        System.out.println("✅ Loaded relation: " + name);
                    }
                }

                bufferManager.FreePage(pid, false);
            }
        } catch (Exception e) {
            
        }
    }

    // ------------------- Get a relation by name -------------------
    public Relation getRelation(String name) {
        for (Relation r : relations) {
            if (r.getName().equalsIgnoreCase(name)) {
                return r;
            }
        }
        return null;
    }

    // ------------------- List all relations -------------------
    public List<Relation> getAllRelations() {
        return relations;
    }

    // ------------------- Print catalog content -------------------
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("=== Relation Catalog ===\n");
        for (Relation r : relations) {
            sb.append(r).append("\n");
        }
        return sb.toString();
    }
}


