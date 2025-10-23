public class RecordId {
    private PageId pageId;
    private int slotIdx;

    public RecordId(PageId pageId, int slotIdx) {
        this.pageId = pageId;
        this.slotIdx = slotIdx;
    }

    // Getters
    public PageId getPageId() { return pageId; }
    public int getSlotIdx() { return slotIdx; }

    @Override
    public String toString() {
        return "Rid(" + pageId.getFileIdx() + ", " + pageId.getPageIdx() + ", slot=" + slotIdx + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        RecordId recordId = (RecordId) obj;
        return slotIdx == recordId.slotIdx && pageId.equals(recordId.pageId);
    }

    @Override
    public int hashCode() {
        // Combines PageId's hash and slotIdx
        return 31 * pageId.hashCode() + slotIdx;
    }
}
