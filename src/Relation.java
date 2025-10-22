import java.util.ArrayList;
import java.util.List;

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
}

