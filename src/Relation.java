import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;

public class Relation {
    private String name;
    private List<String> columnNames;
    private List<String> columnTypes;
    private int recordSize; // total bytes per record

    // --- Constructor ---
    public Relation(String name) {
        this.name = name;
        this.columnNames = new ArrayList<>();
        this.columnTypes = new ArrayList<>();
        this.recordSize = 0;
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
}

