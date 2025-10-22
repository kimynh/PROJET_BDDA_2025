import java.util.ArrayList;
import java.util.List;

public class RelationCatalog {
    private List<Relation> relations;

    public RelationCatalog() {
        this.relations = new ArrayList<>();
    }

    // Add a relation
    public void addRelation(Relation r) {
        relations.add(r);
    }

    // Get a relation by name
    public Relation getRelation(String name) {
        for (Relation r : relations) {
            if (r.getName().equalsIgnoreCase(name)) {
                return r;
            }
        }
        return null;
    }

    // List all relations
    public List<Relation> getAllRelations() {
        return relations;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("=== Relation Catalog ===\n");
        for (Relation r : relations) {
            sb.append(r).append("\n");
        }
        return sb.toString();
    }
}
