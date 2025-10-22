import java.io.IOException;
import java.nio.ByteBuffer;

public class HeapFile {
    private String name;           // name of the relation/table
    private Relation relation;     // schema of the relation
    private DiskManager diskManager;
    private BufferManager bufferManager;

    // --- Constructor ---
    public HeapFile(Relation relation, DiskManager dm, BufferManager bm) {
        this.name = relation.getName();
        this.relation = relation;
        this.diskManager = dm;
        this.bufferManager = bm;
    }

    // --- Create a new page ---
    public PageId createNewPage() throws IOException {
        PageId pid = diskManager.AllocPage();
        System.out.println(" New page created: " + pid);
        return pid;
    }

    // --- Write one record into the page ---
    public void writeRecord(PageId pid, Record rec, int pos) throws IOException {
        // Allocate full page-sized buffer
        int pageSize = diskManager.getConfig().getPagesize();
        byte[] buff = new byte[pageSize];
        ByteBuffer bb = ByteBuffer.wrap(buff);

        // Write the record at offset 0
        relation.writeRecordToBuffer(rec, bb, 0);

        // Save the page to disk
        diskManager.WritePage(pid, buff);
        System.out.println(" Record written to " + pid);
    }

    // --- Read one record from the page ---
    public Record readRecord(PageId pid) throws IOException {
        int pageSize = diskManager.getConfig().getPagesize();
        byte[] buff = new byte[pageSize];
        diskManager.ReadPage(pid, buff);

        ByteBuffer bb = ByteBuffer.wrap(buff);
        Record rec = new Record();
        relation.readFromBuffer(rec, bb, 0);

        System.out.println(" Record read from " + pid + ": " + rec);
        return rec;
    }
}


