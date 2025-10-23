public class TestHeapFile {
    public static void main(String[] args) throws Exception {
        // --- Load configuration ---
        DBConfig cfg = DBConfig.LoadDBConfig("src/fichier_conf.json");
        DiskManager dm = new DiskManager(cfg);
        BufferManager bm = new BufferManager(cfg, dm); // ✅ fix: pass dm too

        // --- Create a relation (table schema) ---
        Relation rel = new Relation("Students", dm, bm, cfg);
        rel.addColumn("ID", "int");
        rel.addColumn("Name", "string");
        rel.addColumn("Grade", "float");

        // --- Initialize heap file ---
        HeapFile heapFile = new HeapFile(rel, dm, bm);

        // --- Create page + record ---
        PageId pid = heapFile.createNewPage();
        Record rec = new Record(new String[]{"1", "Alice", "17.5"});

        heapFile.writeRecord(pid, rec, 0);
        Record recRead = heapFile.readRecord(pid);

        System.out.println("✅ Verification: Read record = " + recRead);
    }
}



