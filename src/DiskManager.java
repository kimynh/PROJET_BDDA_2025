import java.io.EOFException;
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

    private final RandomAccessFile[] rafs;
    private final FileChannel[] chans;

    public DiskManager(DBConfig cfg) {
        this.cfg = cfg;
        String raw = cfg.getDbpath();
        int commentPos = raw.indexOf("//");
        if (commentPos >= 0) raw = raw.substring(0, commentPos);
        raw = raw.trim();
        this.binDataDir = Paths.get(raw).resolve("BinData");
        this.rafs = new RandomAccessFile[cfg.getDm_maxfilecount()];
        this.chans = new FileChannel[cfg.getDm_maxfilecount()];
    }

    // ----------- Init / Finish -----------
    public void Init() throws IOException {
        Files.createDirectories(binDataDir);
        ensureFileInitialized(0); // make sure Data0.bin exists with meta page
    }

    public void Finish() throws IOException {
        for (int i = 0; i < chans.length; i++) {
            if (chans[i] != null && chans[i].isOpen()) {
                chans[i].force(true);
                chans[i].close();
            }
            chans[i] = null;
        }
        for (int i = 0; i < rafs.length; i++) {
            if (rafs[i] != null) rafs[i].close();
            rafs[i] = null;
        }
    }

    @Override
    public void close() throws IOException {
        Finish();
    }

    // ----------- AllocPage (Step 3B) -----------
    public PageId AllocPage() throws IOException {
        // 1) try to reuse a freed page in existing files
        for (int f = 0; f < cfg.getDm_maxfilecount(); f++) {
            if (!Files.exists(filePath(f))) continue;
            int freeIdx = findFreePageInFile(f);
            if (freeIdx >= 1) {
                markPageUsed(f, freeIdx, true);
                return new PageId(f, freeIdx);
            }
        }
        // 2) else create/init file and allocate next page
        for (int f = 0; f < cfg.getDm_maxfilecount(); f++) {
            ensureFileInitialized(f);
            int freeIdx = findFreePageInFile(f);
            if (freeIdx >= 1) {
                markPageUsed(f, freeIdx, true);
                return new PageId(f, freeIdx);
            }
        }
        throw new IOException("No more files available (dm_maxfilecount reached).");
    }

    // ----------- Step 3C: WritePage / ReadPage -----------
    public void WritePage(PageId pid, byte[] buff) throws IOException {
        if (buff.length != cfg.getPagesize()) {
            throw new IllegalArgumentException("Buffer size must equal pagesize (" + cfg.getPagesize() + ")");
        }
        ensureFileInitialized(pid.getFileIdx());
        ensurePageCapacity(pid.getFileIdx(), pid.getPageIdx());

        FileChannel ch = channel(pid.getFileIdx());
        ByteBuffer bb = ByteBuffer.wrap(buff);
        long offset = ((long) pid.getPageIdx()) * cfg.getPagesize();
        ch.write(bb, offset);
        ch.force(false);
    }

    public void ReadPage(PageId pid, byte[] buff) throws IOException {
        if (buff.length != cfg.getPagesize()) {
            throw new IllegalArgumentException("Buffer size must equal pagesize (" + cfg.getPagesize() + ")");
        }
        FileChannel ch = channel(pid.getFileIdx());
        ByteBuffer bb = ByteBuffer.wrap(buff);
        long offset = ((long) pid.getPageIdx()) * cfg.getPagesize();
        int r = ch.read(bb, offset);
        if (r != cfg.getPagesize()) {
            throw new EOFException("Failed to read full page at " + pid);
        }
    }

    // ----------- Step 3D: DeallocPage -----------
public void DeallocPage(PageId pid) throws IOException {
    if (pid.getPageIdx() == 0) {
        throw new IllegalArgumentException("Cannot deallocate meta page (page 0)");
    }
    if (!Files.exists(filePath(pid.getFileIdx()))) {
        throw new IOException("File does not exist for " + pid);
    }
    // mark page as free in bitmap
    markPageUsed(pid.getFileIdx(), pid.getPageIdx(), false);
}

    // ----------- Helpers for files & bitmap -----------
    private Path filePath(int fileIdx) {
        return binDataDir.resolve("Data" + fileIdx + ".bin");
    }

    private RandomAccessFile raf(int fileIdx) throws IOException {
        if (rafs[fileIdx] == null)
            rafs[fileIdx] = new RandomAccessFile(filePath(fileIdx).toFile(), "rw");
        return rafs[fileIdx];
    }

    private FileChannel channel(int fileIdx) throws IOException {
        if (chans[fileIdx] == null || !chans[fileIdx].isOpen())
            chans[fileIdx] = raf(fileIdx).getChannel();
        return chans[fileIdx];
    }

    private void ensureFileInitialized(int fileIdx) throws IOException {
        Path p = filePath(fileIdx);
        if (Files.exists(p)) return;
        if (fileIdx >= cfg.getDm_maxfilecount())
            throw new IOException("fileIdx >= dm_maxfilecount");
        try (RandomAccessFile raf = new RandomAccessFile(p.toFile(), "rw")) {
            raf.setLength(cfg.getPagesize()); // page 0 = meta/bitmap
        }
    }

    private int computeNextPageIdx(int fileIdx) throws IOException {
        long size = channel(fileIdx).size();
        return (int) (size / cfg.getPagesize());
    }

    private void ensurePageCapacity(int fileIdx, int pageIdx) throws IOException {
        FileChannel ch = channel(fileIdx);
        long need = ((long) (pageIdx + 1)) * cfg.getPagesize();
        if (ch.size() < need) {
            ByteBuffer zeros = ByteBuffer.allocate(cfg.getPagesize());
            while (ch.size() < need) {
                ch.write(zeros, ch.size());
                zeros.clear();
            }
            ch.force(false);
        }
    }

    private boolean readBitmapBit(int fileIdx, int pageIdx) throws IOException {
        if (pageIdx <= 0) return true; // meta always used
        int k = pageIdx - 1;
        int byteIdx = k / 8;
        int bit = k % 8;
        ByteBuffer one = ByteBuffer.allocate(1);
        int r = channel(fileIdx).read(one, byteIdx);
        if (r <= 0) return false; // default free
        int b = one.get(0) & 0xFF;
        return (b & (1 << bit)) != 0;
    }

    private void writeBitmapBit(int fileIdx, int pageIdx, boolean used) throws IOException {
        int k = pageIdx - 1;
        int byteIdx = k / 8;
        int bit = k % 8;
        FileChannel ch = channel(fileIdx);
        ByteBuffer one = ByteBuffer.allocate(1);
        int r = ch.read(one, byteIdx);
        int b = (r > 0) ? (one.get(0) & 0xFF) : 0;
        if (used) b |= (1 << bit); else b &= ~(1 << bit);
        one.clear();
        one.put((byte) (b & 0xFF)).flip();
        ch.write(one, byteIdx);
        ch.force(false);
    }

    private void markPageUsed(int fileIdx, int pageIdx, boolean used) throws IOException {
        if (pageIdx == 0)
            throw new IllegalArgumentException("cannot mark meta page");
        writeBitmapBit(fileIdx, pageIdx, used);
    }

    private int findFreePageInFile(int fileIdx) throws IOException {
        FileChannel ch = channel(fileIdx);
        ByteBuffer meta = ByteBuffer.allocate(cfg.getPagesize());
        int r = ch.read(meta, 0);
        if (r != cfg.getPagesize()) throw new EOFException("meta page incomplete");
        meta.flip();

        for (int byteIdx = 0; byteIdx < meta.limit(); byteIdx++) {
            int b = meta.get(byteIdx) & 0xFF;
            if (b != 0xFF) {
                for (int bit = 0; bit < 8; bit++) {
                    if ((b & (1 << bit)) == 0) {
                        int candidate = 1 + (byteIdx * 8 + bit);
                        ensurePageCapacity(fileIdx, candidate);
                        return candidate;
                    }
                }
            }
        }
        int next = computeNextPageIdx(fileIdx);
        ensurePageCapacity(fileIdx, next);
        return next;
    }

    public DBConfig getConfig(){
        return cfg;
    }
}
