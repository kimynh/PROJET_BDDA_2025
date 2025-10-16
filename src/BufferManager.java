import java.io.IOException;
import java.util.*;

public class BufferManager {

    private final DBConfig config;
    private final DiskManager diskManager;

    // --- Données du buffer pool ---
    private final PageId[] pageIds;       // quelle page est chargée dans chaque buffer
    private final byte[][] data;          // contenu binaire de chaque page
    private final int[] pinCount;         // nb de "verrous" sur la page
    private final boolean[] dirty;        // indique si la page a été modifiée
    private final long[] lastAccessTime;  // pour LRU/MRU

    private String currentPolicy;         // "LRU" ou "MRU"

    public BufferManager(DBConfig config, DiskManager diskManager) {
        this.config = config;
        this.diskManager = diskManager;
        this.currentPolicy = config.getBm_policy();

        int n = config.getBm_buffercount();
        int pageSize = config.getPagesize();

        pageIds = new PageId[n];
        data = new byte[n][pageSize];
        pinCount = new int[n];
        dirty = new boolean[n];
        lastAccessTime = new long[n];
    }

    // ---------------------------------------------------
    // GET PAGE
    // ---------------------------------------------------
    public byte[] GetPage(PageId pageId) throws IOException{
        // 1️⃣ Vérifier si la page est déjà chargée
        int index = findPageIndex(pageId);
        if (index != -1) {
            pinCount[index]++;
            lastAccessTime[index] = System.currentTimeMillis();
            return data[index];
        }

        // 2️⃣ Trouver une case libre
        index = findFreeIndex();
        if (index == -1) {
            // 3️⃣ Si pas libre, appliquer politique de remplacement
            index = selectVictimIndex();
            if (index == -1) {
                throw new RuntimeException("Aucune frame disponible (toutes les pages sont pinnées)");
            }

            // Si la case est dirty => écrire sur disque
            if (dirty[index]) {
                diskManager.WritePage(pageIds[index], data[index]);
            }
        }

        // Charger la page depuis le disque
        diskManager.ReadPage(pageId, data[index]);
        pageIds[index] = pageId;
        pinCount[index] = 1;
        dirty[index] = false;
        lastAccessTime[index] = System.currentTimeMillis();

        return data[index];
    }

    // ---------------------------------------------------
    // FREE PAGE
    // ---------------------------------------------------
    public void FreePage(PageId pageId, boolean valdirty) {
        int index = findPageIndex(pageId);
        if (index == -1)
            throw new RuntimeException("Page non trouvée dans le buffer pool : " + pageId);

        if (pinCount[index] > 0)
            pinCount[index]--;

        if (valdirty)
            dirty[index] = true;

        lastAccessTime[index] = System.currentTimeMillis();
    }

    // ---------------------------------------------------
    // FLUSH BUFFERS
    // ---------------------------------------------------
    public void FlushBuffers() throws IOException{
        int n = config.getBm_buffercount();
        for (int i = 0; i < n; i++) {
            if (pageIds[i] != null && dirty[i]) {
                diskManager.WritePage(pageIds[i], data[i]);
            }

            // Réinitialisation
            pageIds[i] = null;
            dirty[i] = false;
            pinCount[i] = 0;
            lastAccessTime[i] = 0;
            data[i] = new byte[config.getPagesize()];
        }
    }

    // ---------------------------------------------------
    // CHANGEMENT DE POLITIQUE
    // ---------------------------------------------------
    public void SetCurrentReplacementPolicy(String policy) {
        this.currentPolicy = policy.toUpperCase();
    }

    // ---------------------------------------------------
    // OUTILS INTERNES
    // ---------------------------------------------------

    private int findPageIndex(PageId pid) {
        for (int i = 0; i < pageIds.length; i++) {
            if (pid.equals(pageIds[i])) return i;
        }
        return -1;
    }

    private int findFreeIndex() {
        for (int i = 0; i < pageIds.length; i++) {
            if (pageIds[i] == null) return i;
        }
        return -1;
    }

    private int selectVictimIndex() {
        int victim = -1;
        long bestTime = (currentPolicy.equalsIgnoreCase("LRU")) ? Long.MAX_VALUE : Long.MIN_VALUE;

        for (int i = 0; i < pageIds.length; i++) {
            if (pinCount[i] == 0 && pageIds[i] != null) {
                if (currentPolicy.equalsIgnoreCase("LRU")) {
                    if (lastAccessTime[i] < bestTime) {
                        bestTime = lastAccessTime[i];
                        victim = i;
                    }
                } else if (currentPolicy.equalsIgnoreCase("MRU")) {
                    if (lastAccessTime[i] > bestTime) {
                        bestTime = lastAccessTime[i];
                        victim = i;
                    }
                }
            }
        }
        return victim;
    }

    // ---------------------------------------------------
    // DEBUG / TESTS
    // ---------------------------------------------------
    public void printState() {
        System.out.println("=== BufferManager State ===");
        for (int i = 0; i < pageIds.length; i++) {
            System.out.println("Slot " + i + ": " +
                    (pageIds[i] == null ? "EMPTY" : pageIds[i]) +
                    " pin=" + pinCount[i] +
                    " dirty=" + dirty[i]);
        }
    }
}
