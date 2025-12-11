import java.util.ArrayList;
import java.util.List;

public class Record {
    private List<String> values;

    // --- Constructors ---
    public Record() {
        this.values = new ArrayList<>();
    }

    // ✅ Allow creating from array of Strings
    public Record(String[] valuesArray) {
        this.values = new ArrayList<>();
        for (String v : valuesArray) {
            this.values.add(v);
        }
    }

    public Record(List<String> values) {
        this.values = values;
    }

    // --- Methods ---
    public void addValue(String value) {
        values.add(value);
    }

    public int size() {
        return values.size();
    }

    public String getValue(int index) {
        return values.get(index);
    }

    public List<String> getValues() {
        return values;
    }

    @Override
    public String toString() {
        return String.join(" ; ", values) + ".";
    }
}



