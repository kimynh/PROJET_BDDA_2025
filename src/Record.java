import java.util.ArrayList;
import java.util.List;

public class Record {
    private List<String> values;

    // --- Constructors ---
    public Record() {
        this.values = new ArrayList<>();
    }

    public Record(List<String> values) {
        this.values = new ArrayList<>(values);
    }

    public Record(String[] valuesArray) {
        this.values = new ArrayList<>();
        for (String v : valuesArray) {
            this.values.add(v);
        }
    }

    // --- Getters & Setters ---
    public List<String> getValues() {
        return values;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }

    public void addValue(String value) {
        this.values.add(value);
    }

    public String getValue(int index) {
        return values.get(index);
    }

    public int size() {
        return values.size();
    }

    @Override
    public String toString() {
        return "Record" + values;
    }
}
