import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Relation {
    private String name;
    private List<String> columnNames;
    private List<String> columnTypes;
    private int recordSize;

    private PageId headerPageId;
    private int nbSlotsPerPage;
    private DiskManager diskManager;
    private BufferManager bufferManager;
    private DBConfig config;

    private final PageId DUMMY_PAGE_ID = new PageId(-1, -1);
    private final int PAGEID_SIZE = 8;

    private final int HP_OFFSET_FIRST_FULL = 0;
    private final int HP_OFFSET_FIRST_FREE = PAGEID_SIZE;

    private final int DP_OFFSET_PREV = 0;
    private final int DP_OFFSET_NEXT = PAGEID_SIZE;
    private final int DP_OFFSET_BYTEMAP = PAGEID_SIZE * 2;

    public Relation(String name, DiskManager diskManager, BufferManager bufferManager, DBConfig config) {
        this.name = name;
        this.columnNames = new ArrayList<>();
        this.columnTypes = new ArrayList<>();
        this.recordSize = 0;
        this.diskManager = diskManager;
        this.bufferManager = bufferManager;
        this.config = config;
        this.headerPageId = null;
        this.nbSlotsPerPage = 0;
    }

    private PageId readPageIdFromBuffer(ByteBuffer buffer, int offset) {
        int fileIdx = buffer.getInt(offset);
        int pageIdx = buffer.getInt(offset + 4);
        return new PageId(fileIdx, pageIdx);
    }

    private void writePageIdToBuffer(ByteBuffer buffer, int offset, PageId pageId) {
        buffer.putInt(offset, pageId.getFileIdx());
        buffer.putInt(offset + 4, pageId.getPageIdx());
    }

    private boolean isSlotFree(ByteBuffer buffer, int slotIdx) {
        return buffer.get(DP_OFFSET_BYTEMAP + slotIdx) == (byte) 0;
    }

    private int findFirstFreeSlot(ByteBuffer buffer) {
        for (int i = 0; i < nbSlotsPerPage; i++) {
            if (isSlotFree(buffer, i)) {
                return i;
            }
        }
        return -1;
    }

    public boolean isPageFull(PageId pageId) throws IOException {
        byte[] content = bufferManager.GetPage(pageId);
        ByteBuffer buffer = ByteBuffer.wrap(content);
        int freeSlot = findFirstFreeSlot(buffer);
        bufferManager.FreePage(pageId, false);
        return freeSlot == -1;
    }

    public boolean isPageEmpty(PageId pageId) throws IOException {
        byte[] content = bufferManager.GetPage(pageId);
        ByteBuffer buffer = ByteBuffer.wrap(content);
        for (int i = 0; i < nbSlotsPerPage; i++) {
            if (buffer.get(DP_OFFSET_BYTEMAP + i) != (byte) 0) {
                bufferManager.FreePage(pageId, false);
                return false;
            }
        }
        bufferManager.FreePage(pageId, false);
        return true;
    }

    private void deleteRecordFromDataPage(RecordId rid) throws IOException {
        ByteBuffer dataBuffer = ByteBuffer.wrap(bufferManager.GetPage(rid.getPageId()));
        dataBuffer.put(DP_OFFSET_BYTEMAP + rid.getSlotIdx(), (byte) 0);
        bufferManager.FreePage(rid.getPageId(), true);
    }

    private PageId getFirstFullPageId() throws IOException {
        byte[] headerContent = bufferManager.GetPage(headerPageId);
        ByteBuffer buffer = ByteBuffer.wrap(headerContent);
        PageId pid = readPageIdFromBuffer(buffer, HP_OFFSET_FIRST_FULL);
        bufferManager.FreePage(headerPageId, false);
        return pid;
    }

    private PageId getFirstFreePageId() throws IOException {
        byte[] headerContent = bufferManager.GetPage(headerPageId);
        ByteBuffer buffer = ByteBuffer.wrap(headerContent);
        PageId pid = readPageIdFromBuffer(buffer, HP_OFFSET_FIRST_FREE);
        bufferManager.FreePage(headerPageId, false);
        return pid;
    }

    private PageId getNextPageId(PageId pageId) throws IOException {
        byte[] content = bufferManager.GetPage(pageId);
        ByteBuffer buffer = ByteBuffer.wrap(content);
        PageId pid = readPageIdFromBuffer(buffer, DP_OFFSET_NEXT);
        bufferManager.FreePage(pageId, false);
        return pid;
    }

    private PageId findPrevPageInList(PageId targetId, PageId headId) throws IOException {
        PageId currentPageId = headId;
        PageId prevPageId = headerPageId;
        while (!currentPageId.equals(DUMMY_PAGE_ID)) {
            if (currentPageId.equals(targetId)) {
                return prevPageId;
            }
            prevPageId = currentPageId;
            currentPageId = getNextPageId(currentPageId);
        }
        throw new RuntimeException("Page not found in the expected list: " + targetId);
    }

    private void unlinkPage(PageId pageId, PageId prevId, PageId nextId) throws IOException {
        if (prevId.equals(headerPageId)) {
            byte[] headerContent = bufferManager.GetPage(headerPageId);
            ByteBuffer headerBuffer = ByteBuffer.wrap(headerContent);
            PageId currentFull = readPageIdFromBuffer(headerBuffer, HP_OFFSET_FIRST_FULL);
            if (pageId.equals(currentFull)) {
                writePageIdToBuffer(headerBuffer, HP_OFFSET_FIRST_FULL, nextId);
            } else {
                writePageIdToBuffer(headerBuffer, HP_OFFSET_FIRST_FREE, nextId);
            }
            bufferManager.FreePage(headerPageId, true);
        } else {
            byte[] prevContent = bufferManager.GetPage(prevId);
            ByteBuffer prevBuffer = ByteBuffer.wrap(prevContent);
            writePageIdToBuffer(prevBuffer, DP_OFFSET_NEXT, nextId);
            bufferManager.FreePage(prevId, true);
        }
        if (!nextId.equals(DUMMY_PAGE_ID)) {
            byte[] nextContent = bufferManager.GetPage(nextId);
            ByteBuffer nextBuffer = ByteBuffer.wrap(nextContent);
            writePageIdToBuffer(nextBuffer, DP_OFFSET_PREV, prevId);
            bufferManager.FreePage(nextId, true);
        }
    }

    private void movePageFromListToNewHead(PageId pageId, int headerOffset) throws IOException {
        byte[] headerContent = bufferManager.GetPage(headerPageId);
        ByteBuffer headerBuffer = ByteBuffer.wrap(headerContent);
        PageId oldHeadId = readPageIdFromBuffer(headerBuffer, headerOffset);
        writePageIdToBuffer(headerBuffer, headerOffset, pageId);
        bufferManager.FreePage(headerPageId, true);
        byte[] pageContent = bufferManager.GetPage(pageId);
        ByteBuffer pageBuffer = ByteBuffer.wrap(pageContent);
        writePageIdToBuffer(pageBuffer, DP_OFFSET_PREV, headerPageId);
        writePageIdToBuffer(pageBuffer, DP_OFFSET_NEXT, oldHeadId);
        bufferManager.FreePage(pageId, true);
        if (!oldHeadId.equals(DUMMY_PAGE_ID)) {
            byte[] oldHeadContent = bufferManager.GetPage(oldHeadId);
            ByteBuffer oldHeadBuffer = ByteBuffer.wrap(oldHeadContent);
            writePageIdToBuffer(oldHeadBuffer, DP_OFFSET_PREV, pageId);
            bufferManager.FreePage(oldHeadId, true);
        }
    }

    private void movePageFromFreeToFull(PageId pageId) throws IOException {
        PageId prevId = findPrevPageInList(pageId, getFirstFreePageId());
        PageId nextId = getNextPageId(pageId);
        unlinkPage(pageId, prevId, nextId);
        movePageFromListToNewHead(pageId, HP_OFFSET_FIRST_FULL);
    }

    private void movePageFromFullToFree(PageId pageId) throws IOException {
        PageId prevId = findPrevPageInList(pageId, getFirstFullPageId());
        PageId nextId = getNextPageId(pageId);
        unlinkPage(pageId, prevId, nextId);
        movePageFromListToNewHead(pageId, HP_OFFSET_FIRST_FREE);
    }

    private void removePageFromListAndDeallocate(PageId pageId) throws IOException {
        PageId headId = isPageInList(pageId, getFirstFreePageId()) ? getFirstFreePageId() : getFirstFullPageId();
        PageId prevId = findPrevPageInList(pageId, headId);
        PageId nextId = getNextPageId(pageId);
        unlinkPage(pageId, prevId, nextId);
        diskManager.DeallocPage(pageId);
    }

    private boolean isPageInList(PageId targetId, PageId headId) throws IOException {
        PageId currentPageId = headId;
        while (!currentPageId.equals(DUMMY_PAGE_ID)) {
            if (currentPageId.equals(targetId))
                return true;
            currentPageId = getNextPageId(currentPageId);
        }
        return false;
    }

    public String getName() { return name; }
    public List<String> getColumnNames() { return columnNames; }
    public List<String> getColumnTypes() { return columnTypes; }
    public int getRecordSize() { return recordSize; }
    public PageId getHeaderPageId() { return headerPageId; }
    public int getNbSlotsPerPage() { return nbSlotsPerPage; }

    public void addColumn(String columnName, String type) {
        columnNames.add(columnName);
        columnTypes.add(type);
        recordSize += getTypeSize(type);
    }

    private int getTypeSize(String type) {
        String lowerType = type.toLowerCase();
        
        // Gestion des types avec taille variable comme CHAR(n), VARCHAR(n)
        if (lowerType.startsWith("char(") && lowerType.endsWith(")")) {
            try {
                String sizeStr = lowerType.substring(5, lowerType.length() - 1);
                return Integer.parseInt(sizeStr);
            } catch (NumberFormatException e) {
                return 1; // fallback
            }
        }
        
        if (lowerType.startsWith("varchar(") && lowerType.endsWith(")")) {
            try {
                String sizeStr = lowerType.substring(8, lowerType.length() - 1);
                return Integer.parseInt(sizeStr);
            } catch (NumberFormatException e) {
                return 20; // fallback
            }
        }
        
        return switch (lowerType) {
            case "int" -> 4;
            case "float" -> 4;
            case "double" -> 8;
            case "real" -> 4;
            case "char" -> 1;
            case "string" -> 20;
            default -> 0;
        };
    }

    public void calculateNbSlotsPerPage() {
        if (recordSize > 0 && config != null) {
            int pageSize = config.getPagesize();
            int metadataOverhead = DP_OFFSET_BYTEMAP;
            this.nbSlotsPerPage = (pageSize - metadataOverhead) / (1 + recordSize);
        }
    }

    public void initializeHeaderPage() throws IOException {
        if (diskManager != null && headerPageId == null) {
            this.headerPageId = diskManager.AllocPage();
            calculateNbSlotsPerPage();
            byte[] headerContent = bufferManager.GetPage(headerPageId);
            ByteBuffer buffer = ByteBuffer.wrap(headerContent);
            writePageIdToBuffer(buffer, HP_OFFSET_FIRST_FULL, DUMMY_PAGE_ID);
            writePageIdToBuffer(buffer, HP_OFFSET_FIRST_FREE, DUMMY_PAGE_ID);
            bufferManager.FreePage(headerPageId, true);
        }
    }

        public void addDataPage() throws IOException {
        PageId newPid = diskManager.AllocPage();
        byte[] content = bufferManager.GetPage(newPid);
        ByteBuffer buffer = ByteBuffer.wrap(content);

        // --- FIX: Zero-initialize entire data page (important for persistence) ---
        for (int i = 0; i < content.length; i++) {
            buffer.put(i, (byte) 0);
        }

        // --- Initialize metadata in the new data page ---
        writePageIdToBuffer(buffer, DP_OFFSET_PREV, DUMMY_PAGE_ID);
        writePageIdToBuffer(buffer, DP_OFFSET_NEXT, DUMMY_PAGE_ID);

        // --- Initialize BYTEMAP (mark all slots empty) ---
        for (int i = 0; i < nbSlotsPerPage; i++) {
            buffer.put(DP_OFFSET_BYTEMAP + i, (byte) 0);
        }

        bufferManager.FreePage(newPid, true);

        // --- Insert this new page at the head of FREE list ---
        byte[] headerContent = bufferManager.GetPage(headerPageId);
        ByteBuffer headerBuffer = ByteBuffer.wrap(headerContent);

        PageId oldFirstFree = readPageIdFromBuffer(headerBuffer, HP_OFFSET_FIRST_FREE);
        writePageIdToBuffer(headerBuffer, HP_OFFSET_FIRST_FREE, newPid);
        bufferManager.FreePage(headerPageId, true);

        // --- Fix links between pages ---
        if (!oldFirstFree.equals(DUMMY_PAGE_ID)) {
            byte[] oldContent = bufferManager.GetPage(oldFirstFree);
            ByteBuffer oldBuffer = ByteBuffer.wrap(oldContent);
            writePageIdToBuffer(oldBuffer, DP_OFFSET_PREV, newPid);
            bufferManager.FreePage(oldFirstFree, true);
        }
    }



    public PageId getFreeDataPageId(int sizeRecord) throws IOException {
        if (sizeRecord > recordSize)
            return DUMMY_PAGE_ID;
        PageId currentPageId = getFirstFreePageId();
        while (!currentPageId.equals(DUMMY_PAGE_ID)) {
            if (!isPageFull(currentPageId)) {
                return currentPageId;
            }
            currentPageId = getNextPageId(currentPageId);
        }
        return DUMMY_PAGE_ID;
    }

    public RecordId writeRecordToDataPage(Record record, PageId pageId) throws IOException {
        byte[] pageContent = bufferManager.GetPage(pageId);
        ByteBuffer buffer = ByteBuffer.wrap(pageContent);
        int slotIndex = findFirstFreeSlot(buffer);
        if (slotIndex == -1) {
            bufferManager.FreePage(pageId, false);
            throw new RuntimeException("Page is full (pre-check failed).");
        }
        int dataOffset = DP_OFFSET_BYTEMAP + nbSlotsPerPage + (slotIndex * recordSize);
        writeRecordToBuffer(record, buffer, dataOffset);
        buffer.put(DP_OFFSET_BYTEMAP + slotIndex, (byte) 1);
        bufferManager.FreePage(pageId, true);
        return new RecordId(pageId, slotIndex);
    }

    public ArrayList<Record> getRecordsInDataPage(PageId pageId) throws IOException {
        ArrayList<Record> records = new ArrayList<>();
        byte[] content = bufferManager.GetPage(pageId);
        ByteBuffer buffer = ByteBuffer.wrap(content);
        for (int slotIndex = 0; slotIndex < nbSlotsPerPage; slotIndex++) {
            if (!isSlotFree(buffer, slotIndex)) {
                Record record = new Record();
                int dataOffset = DP_OFFSET_BYTEMAP + nbSlotsPerPage + (slotIndex * recordSize);
                readFromBuffer(record, buffer, dataOffset);
                records.add(record);
            }
        }
        bufferManager.FreePage(pageId, false);
        return records;
    }

    public ArrayList<PageId> getDataPages() throws IOException {
        ArrayList<PageId> pageIds = new ArrayList<>();
        PageId firstFullPageId = getFirstFullPageId();
        PageId firstFreePageId = getFirstFreePageId();
        for (PageId startId : new PageId[] { firstFullPageId, firstFreePageId }) {
            PageId currentPageId = startId;
            while (!currentPageId.equals(DUMMY_PAGE_ID)) {
                pageIds.add(currentPageId);
                currentPageId = getNextPageId(currentPageId);
            }
        }
        return pageIds;
    }

    public RecordId InsertRecord(Record record) throws IOException {
        PageId freePageId = getFreeDataPageId(recordSize);
        if (freePageId.equals(DUMMY_PAGE_ID)) {
            addDataPage();
            freePageId = getFreeDataPageId(recordSize);
        }
        RecordId rid = writeRecordToDataPage(record, freePageId);
        if (isPageFull(freePageId)) {
            movePageFromFreeToFull(freePageId);
        }
        return rid;
    }

    public ArrayList<Record> GetAllRecords() throws IOException {
        ArrayList<Record> allRecords = new ArrayList<>();
        ArrayList<PageId> allPageIds = getDataPages();
        for (PageId pageId : allPageIds) {
            allRecords.addAll(getRecordsInDataPage(pageId));
        }
        return allRecords;
    }

    public void DeleteRecord(RecordId rid) throws IOException {
        PageId pageId = rid.getPageId();
        boolean wasFull = isPageFull(pageId);
        deleteRecordFromDataPage(rid);
        if (isPageEmpty(pageId)) {
            removePageFromListAndDeallocate(pageId);
        } else if (wasFull) {
            movePageFromFullToFree(pageId);
        }
    }

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

    // Retrieves a specific record using its RecordId
    public Record getRecord(RecordId rid) throws IOException {
        byte[] content = bufferManager.GetPage(rid.getPageId());
        ByteBuffer buffer = ByteBuffer.wrap(content);
        
        // Safety check
        if (isSlotFree(buffer, rid.getSlotIdx())) {
            bufferManager.FreePage(rid.getPageId(), false);
            return null;
        }

        Record rec = new Record();
        int dataOffset = DP_OFFSET_BYTEMAP + nbSlotsPerPage + (rid.getSlotIdx() * recordSize);
        readFromBuffer(rec, buffer, dataOffset);
        
        bufferManager.FreePage(rid.getPageId(), false);
        return rec;
    }

    // Overwrites a record at a specific slot with new data
    public void updateRecord(RecordId rid, Record newRec) throws IOException {
        byte[] content = bufferManager.GetPage(rid.getPageId());
        ByteBuffer buffer = ByteBuffer.wrap(content);
        
        int dataOffset = DP_OFFSET_BYTEMAP + nbSlotsPerPage + (rid.getSlotIdx() * recordSize);
        writeRecordToBuffer(newRec, buffer, dataOffset);
        
        // Mark page as dirty (true) so it saves to disk later
        bufferManager.FreePage(rid.getPageId(), true);
    }

    // Returns a list of ALL RecordIds in the relation
    public ArrayList<RecordId> getAllRecordIds() throws IOException {
        ArrayList<RecordId> rids = new ArrayList<>();
        ArrayList<PageId> pages = getDataPages();
        
        for (PageId pid : pages) {
            byte[] content = bufferManager.GetPage(pid);
            ByteBuffer buffer = ByteBuffer.wrap(content);
            
            for (int i = 0; i < nbSlotsPerPage; i++) {
                if (!isSlotFree(buffer, i)) {
                    rids.add(new RecordId(pid, i));
                }
            }
            bufferManager.FreePage(pid, false);
        }
        return rids;
    }
}