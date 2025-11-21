
import java.util.LinkedHashMap;

public class DBManager{
    private final DBConfig config;
    private  final LinkedHashMap<String,Relation> tables;

//Constructeur 
    public DBManager(DBConfig config){
        this.config=config;
        this.tables =new LinkedHashMap<>();
            }
    
// Methode Pour ajouter une table
    public void addTable(Relation tab){
        if(tab==null || tab.getName()==null) return;
        tables.put(tab.getName(),tab);
    }
// Methode Pour recuperer une table

    public Relation getTable(String nomTable){
        if (nomTable==null) return null;
        return tables.get(nomTable);
    }
// Methode Pour supprimer une table
    public void RemoveTable(String nomTable){
        if (nomTable==null) return;
        tables.remove(nomTable);
    }
// Methode Pour supprimer toutes les tables
    public void RemoveAllTables(){
        tables.clear();
    }
}