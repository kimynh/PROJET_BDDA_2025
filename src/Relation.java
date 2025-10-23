import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class Relation {
    private String name;
    private List<String> columnNames;
    private List<String> columnTypes;
    private int recordSize; // total bytes per record
    
    // Ajout des variables membres
    private PageId headerPageId;
    private int nbSlotsPerPage;
    private DiskManager diskManager;
    private BufferManager bufferManager;
    private DBConfig config;

    private List<PageId> pagesWithFreeSpace = new ArrayList<>();
    private Map<PageId, Integer> freeSlots = new HashMap<>();
    private Map<PageId, Integer> nextFreeSlotIndex = new HashMap<>();

    public Relation(String name, DiskManager diskManager, BufferManager bufferManager, DBConfig config) {
        this.name = name;
        this.columnNames = new ArrayList<>();
        this.columnTypes = new ArrayList<>();
        this.recordSize = 0;
        
        // Références vers les instances existantes
        this.diskManager = diskManager;
        this.bufferManager = bufferManager;
        this.config = config;
        this.headerPageId = null; // sera initialisé plus tard
        this.nbSlotsPerPage = 0; // sera calculé en fonction de recordSize et pageSize
    }


    // --- Add a column ---
    public void addColumn(String columnName, String type) {
        columnNames.add(columnName);
        columnTypes.add(type);
        recordSize += getTypeSize(type);
    }

    // --- Compute type size (simplified) ---
    private int getTypeSize(String type) {
        return switch (type.toLowerCase()) {
            case "int" -> 4;
            case "float" -> 4;
            case "double" -> 8;
            case "char" -> 1;
            case "string" -> 20; // fixed length assumption
            default -> 0;
        };
    }

    // --- Getters ---
    public String getName() {
        return name;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<String> getColumnTypes() {
        return columnTypes;
    }

    public int getRecordSize() {
        return recordSize;
    }

    public PageId getHeaderPageId() {
        return headerPageId;
    }

    public int getNbSlotsPerPage() {
        return nbSlotsPerPage;
    }

    // --- Calcule le nombre de slots par page ---
    public void calculateNbSlotsPerPage() {
        if (recordSize > 0 && config != null) {
            int pageSize = config.getPagesize();
            // Reserve some space for page metadata (e.g., 4 bytes for slot count)
            int usableSpace = pageSize - 4;
            this.nbSlotsPerPage = usableSpace / recordSize;
        }
    }

    // --- Initialise la header page ---
    public void initializeHeaderPage() throws IOException {
        if (diskManager != null && headerPageId == null) {
            this.headerPageId = diskManager.AllocPage();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Relation ").append(name).append(" (");
        for (int i = 0; i < columnNames.size(); i++) {
            sb.append(columnNames.get(i)).append(":").append(columnTypes.get(i));
            if (i < columnNames.size() - 1) sb.append(", ");
        }
        sb.append(") recordSize=").append(recordSize);
        return sb.toString();
    }

    // --- Write record data to ByteBuffer ---
    public void writeRecordToBuffer(Record rec, ByteBuffer bb, int offset) {
        bb.position(offset);
        for (int i = 0; i < columnTypes.size(); i++) {
            String type = columnTypes.get(i).toLowerCase();
            String value = rec.getValues().get(i);

            switch (type) {
                case "int" -> bb.putInt(Integer.parseInt(value));
                case "float" -> bb.putFloat(Float.parseFloat(value));
                case "double" -> bb.putDouble(Double.parseDouble(value));
                case "char" -> bb.put((byte) value.charAt(0));
                case "string" -> {
                    byte[] strBytes = new byte[20];
                    byte[] valBytes = value.getBytes();
                    System.arraycopy(valBytes, 0, strBytes, 0, Math.min(20, valBytes.length));
                    bb.put(strBytes);
                }
            }
        }
    }

    // --- Read record data back from ByteBuffer ---
    public void readFromBuffer(Record rec, ByteBuffer bb, int offset) {
        bb.position(offset);
        rec.getValues().clear();

        for (int i = 0; i < columnTypes.size(); i++) {
            String type = columnTypes.get(i).toLowerCase();
            switch (type) {
                case "int" -> rec.addValue(String.valueOf(bb.getInt()));
                case "float" -> rec.addValue(String.valueOf(bb.getFloat()));
                case "double" -> rec.addValue(String.valueOf(bb.getDouble()));
                case "char" -> rec.addValue(String.valueOf((char) bb.get()));
                case "string" -> {
                    byte[] strBytes = new byte[20];
                    bb.get(strBytes);
                    rec.addValue(new String(strBytes).trim());
                }
            }
        }
    }

    public void addDataPage() throws IOException{
        PageId newPid = diskManager.AllocPage();
        pagesWithFreeSpace.add(newPid);
        freeSlots.put(newPid, nbSlotsPerPage);
        nextFreeSlotIndex.put(newPid, 0);
        System.out.println("addDataPage : allocated " + newPid + " with " + nbSlotsPerPage + " slots.");
    }

    public RecordId writeRecordToDataPage(Record record, PageId pageId) throws IOException {
    // Convertir le byte[] en ByteBuffer
    byte[] pageContent = bufferManager.GetPage(pageId);
    ByteBuffer buffer = ByteBuffer.wrap(pageContent);
    
    // Le reste de la méthode reste identique
    int slotIndex = nextFreeSlotIndex.get(pageId);
    int offset = slotIndex * recordSize;
    
    writeRecordToBuffer(record, buffer, offset);
    
    nextFreeSlotIndex.put(pageId, slotIndex + 1);
    
    int remainingSlots = freeSlots.get(pageId);
    freeSlots.put(pageId, remainingSlots - 1);
    
    bufferManager.FreePage(pageId, true);
    
    return new RecordId(pageId, slotIndex);
    }

    public ArrayList<Record> getRecordsInDataPage(PageId pageId) throws IOException {
    ArrayList<Record> records = new ArrayList<>();
    
    // Récupérer le contenu de la page
    byte[] pageContent = bufferManager.GetPage(pageId);
    ByteBuffer buffer = ByteBuffer.wrap(pageContent);
    
    // Récupérer le nombre de records dans cette page
    int nbRecords = nextFreeSlotIndex.get(pageId);
    
    // Lire chaque record de la page
    for (int i = 0; i < nbRecords; i++) {
        Record record = new Record();
        int offset = i * recordSize;
        readFromBuffer(record, buffer, offset);
        records.add(record);
    }
    
    // Libérer la page après lecture
        bufferManager.FreePage(pageId, false);
    
        return records;
        }

    public PageId getFreeDataPageId(int sizeRecord) {
    // on suppose que sizeRecord == recordSize ; sinon il faudrait convertir en slots
        if (sizeRecord > recordSize) {
        // record trop gros pour tenir dans une case standard
            return null;
        }
        for (PageId pid : pagesWithFreeSpace) {
            Integer remaining = freeSlots.get(pid);
            if (remaining != null && remaining > 0) {
                return pid;
            }
        }
        return null; // aucune page avec de la place
    }


}

