import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class DiskManager implements AutoCloseable {
    private final DBConfig cfg;
    private final Path binDataDir;

    // optional caches (will use later)
    private RandomAccessFile[] rafs;
    private FileChannel[] chans;

    public DiskManager(DBConfig cfg) {
        this.cfg = cfg;
        String raw = cfg.getDbpath();
        // safety: strip inline comment like "DB // ..."
        int commentPos = raw.indexOf("//");
        if (commentPos >= 0) raw = raw.substring(0, commentPos);
        raw = raw.trim();
        this.binDataDir = Paths.get(raw).resolve("BinData");
        this.rafs = new RandomAccessFile[cfg.getDm_maxfilecount()];
        this.chans = new FileChannel[cfg.getDm_maxfilecount()];
    }

    public void Init() throws IOException {
        // Ensure DB/BinData exists
        Files.createDirectories(binDataDir);
        // Ensure Data0.bin exists and has at least 1 page (page 0 = meta/bitmap)
        ensureFileInitialized(0);
    }

    public void Finish() throws IOException {
        // flush + close any opened channels/files
        if (chans != null) {
            for (int i = 0; i < chans.length; i++) {
                if (chans[i] != null && chans[i].isOpen()) {
                    chans[i].force(true);
                    chans[i].close();
                }
                chans[i] = null;
            }
        }
        if (rafs != null) {
            for (int i = 0; i < rafs.length; i++) {
                if (rafs[i] != null) rafs[i].close();
                rafs[i] = null;
            }
        }
    }

    @Override
    public void close() throws IOException {
        Finish();
    }

    // ---------- helpers (we’ll add more later) ----------
    private Path filePath(int fileIdx) {
        return binDataDir.resolve("Data" + fileIdx + ".bin");
    }

    private void ensureFileInitialized(int fileIdx) throws IOException {
        Path p = filePath(fileIdx);
        if (Files.exists(p)) return;
        if (fileIdx >= cfg.getDm_maxfilecount())
            throw new IOException("fileIdx >= dm_maxfilecount");
        // create file with one page (page 0 reserved for meta/bitmap)
        try (RandomAccessFile raf = new RandomAccessFile(p.toFile(), "rw")) {
            raf.setLength(cfg.getPagesize());
        }
    }
}
