import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * Classe principale du SGBD - Point d'entrée de l'application
 */
public class SGBD {
    
    // === Variables membres ===
    private final DBConfig config;
    private final DiskManager diskManager;
    private final BufferManager bufferManager;
    private final DBManager dbManager;
    
    private boolean running = true;
    
    // === Constructeur ===
    public SGBD(DBConfig config) throws IOException {
        this.config = config;
        this.diskManager = new DiskManager(config);
        
        // Initialiser le disque pour s'assurer que la meta page existe
        this.diskManager.Init();
        
        this.bufferManager = new BufferManager(config, diskManager);
        this.dbManager = new DBManager(config);
        
        // Définir les managers dans DBManager pour qu'ils soient disponibles lors du chargement
        this.dbManager.setManagers(diskManager, bufferManager);
        
        // Charger l'état de la base de données si existant
        dbManager.LoadState();
        
        System.out.println("SGBD initialisé avec succès");
    }
    
    // === Méthode principale d'exécution ===
    public void Run() {
        Scanner scanner = new Scanner(System.in);
        
        while (running) {
            try {
                // Attendre la saisie utilisateur (sans prompt)
                if (scanner.hasNextLine()) {
                    String command = scanner.nextLine().trim();
                    
                    if (!command.isEmpty()) {
                        processCommand(command);
                    }
                }
            } catch (Exception e) {
                System.err.println("Erreur lors du traitement de la commande: " + e.getMessage());
            }
        }
        
        scanner.close();
    }
    
    // === Parsing et dispatch des commandes ===
    private void processCommand(String commandLine) {
        String[] tokens = commandLine.trim().split("\\s+");
        if (tokens.length == 0) return;
        
        String mainCommand = tokens[0].toUpperCase();
        
        switch (mainCommand) {
            case "EXIT" -> ProcessExitCommand(tokens);
            case "CREATE" -> {
                if (tokens.length > 1 && tokens[1].equalsIgnoreCase("TABLE")) {
                    ProcessCreateTableCommand(tokens);
                } else {
                    System.err.println("Commande CREATE inconnue");
                }
            }
            case "DROP" -> {
                if (tokens.length > 1 && tokens[1].equalsIgnoreCase("TABLE")) {
                    ProcessDropTableCommand(tokens);
                } else if (tokens.length > 1 && tokens[1].equalsIgnoreCase("TABLES")) {
                    ProcessDropTablesCommand(tokens);
                } else {
                    System.err.println("Commande DROP inconnue");
                }
            }
            case "INSERT" -> ProcessInsertCommand(tokens);
            case "SELECT" -> ProcessSelectCommand(tokens);
            case "DELETE" -> ProcessDeleteCommand(tokens);
            case "DESCRIBE" -> ProcessDescribeCommand(tokens);
            case "LIST" -> ProcessListCommand(tokens);
            case "BMSETTINGS" -> ProcessBmSettingsCommand(tokens);
            case "BMSTATE" -> ProcessBmStateCommand(tokens);
            case "IMPORT" -> ProcessImportCommand(tokens);
            case "APPEND" -> ProcessAppendCommand(tokens);
            case "UPDATE" -> ProcessUpdateCommand(tokens);
            default -> System.err.println("Commande inconnue: " + mainCommand);
        }
    }
    
    // === EXIT : Sauvegarde et arrêt ===
    public void ProcessExitCommand(String[] tokens) {
        try {
            System.out.println("Sauvegarde en cours...");
            
            // Flush des buffers
            bufferManager.FlushBuffers();
            
            // Sauvegarde de l'état de la base
            dbManager.SaveState();
            
            System.out.println("Arrêt du SGBD");
            running = false;
            
        } catch (IOException e) {
            System.err.println("Erreur lors de la sauvegarde: " + e.getMessage());
            running = false;
        }
    }
    
    // === CREATE TABLE nomTable (col1:type1, col2:type2, ...) ===
    public void ProcessCreateTableCommand(String[] tokens) {
        try {
            if (tokens.length < 3) {
                System.err.println("Syntaxe: CREATE TABLE nomTable (col1:type1, col2:type2, ...)");
                return;
            }
            
            String tableName = tokens[2];
            
            // Vérifier si la table existe déjà
            if (dbManager.getTable(tableName) != null) {
                System.err.println("La table '" + tableName + "' existe déjà");
                return;
            }
            
            // Parser la définition des colonnes
            String columnsDefinition = String.join(" ", java.util.Arrays.copyOfRange(tokens, 3, tokens.length));
            
            // Enlever les parenthèses
            if (!columnsDefinition.startsWith("(") || !columnsDefinition.endsWith(")")) {
                System.err.println("Syntaxe invalide: les colonnes doivent être entre parenthèses");
                return;
            }
            
            columnsDefinition = columnsDefinition.substring(1, columnsDefinition.length() - 1);
            
            // Créer la relation
            Relation relation = new Relation(tableName, diskManager, bufferManager, config);
            
            // Parser et ajouter les colonnes
            String[] columns = columnsDefinition.split(",");
            for (String column : columns) {
                String[] colDef = column.trim().split(":");
                if (colDef.length != 2) {
                    System.err.println("Syntaxe de colonne invalide: " + column);
                    return;
                }
                
                String columnName = colDef[0].trim();
                String columnType = colDef[1].trim();
                
                relation.addColumn(columnName, columnType);
            }
            
            // Initialiser la header page
            relation.initializeHeaderPage();
            
            // Ajouter la relation au DBManager
            dbManager.addTable(relation);
            
            System.out.println("Table '" + tableName + "' créée avec succès");
            
        } catch (Exception e) {
            System.err.println("Erreur lors de la création de la table: " + e.getMessage());
        }
    }
    
    // === DROP TABLE nomTable ===
    public void ProcessDropTableCommand(String[] tokens) {
        try {
            if (tokens.length < 3) {
                System.err.println("Syntaxe: DROP TABLE nomTable");
                return;
            }
            
            String tableName = tokens[2];
            Relation relation = dbManager.getTable(tableName);
            
            if (relation == null) {
                System.err.println("La table '" + tableName + "' n'existe pas");
                return;
            }
            
            // Désallouer toutes les pages de la relation
            if (relation.getHeaderPageId() != null) {
                // Désallouer la header page
                diskManager.DeallocPage(relation.getHeaderPageId());
                
                // Désallouer toutes les data pages
                ArrayList<PageId> dataPages = relation.getDataPages();
                for (PageId pageId : dataPages) {
                    diskManager.DeallocPage(pageId);
                }
            }
            
            // Supprimer la relation du DBManager
            dbManager.RemoveTable(tableName);
            
            System.out.println("Table '" + tableName + "' supprimée avec succès");
            
        } catch (Exception e) {
            System.err.println("Erreur lors de la suppression de la table: " + e.getMessage());
        }
    }
    
    // === DROP TABLES ===
    public void ProcessDropTablesCommand(String[] tokens) {
        try {
            // Supprimer toutes les tables
            for (String tableName : new ArrayList<>(dbManager.getAllTableNames())) {
                Relation relation = dbManager.getTable(tableName);
                
                if (relation != null && relation.getHeaderPageId() != null) {
                    // Désallouer la header page
                    diskManager.DeallocPage(relation.getHeaderPageId());
                    
                    // Désallouer toutes les data pages
                    ArrayList<PageId> dataPages = relation.getDataPages();
                    for (PageId pageId : dataPages) {
                        diskManager.DeallocPage(pageId);
                    }
                }
            }
            
            // Supprimer toutes les relations du DBManager
            dbManager.RemoveAllTables();
            
            System.out.println("Toutes les tables ont été supprimées");
            
        } catch (Exception e) {
            System.err.println("Erreur lors de la suppression des tables: " + e.getMessage());
        }
    }
    
    // === INSERT INTO nomTable VALUES (val1, val2, ...) ===
    public void ProcessInsertCommand(String[] tokens) {
        try {
            if (tokens.length < 5 || !tokens[1].equalsIgnoreCase("INTO") || !tokens[3].equalsIgnoreCase("VALUES")) {
                System.err.println("Syntaxe: INSERT INTO nomTable VALUES (val1, val2, ...)");
                return;
            }
            
            String tableName = tokens[2];
            Relation relation = dbManager.getTable(tableName);
            
            if (relation == null) {
                System.err.println("La table '" + tableName + "' n'existe pas");
                return;
            }
            
            // Parser les valeurs
            String valuesStr = String.join(" ", java.util.Arrays.copyOfRange(tokens, 4, tokens.length));
            
            if (!valuesStr.startsWith("(") || !valuesStr.endsWith(")")) {
                System.err.println("Syntaxe invalide: les valeurs doivent être entre parenthèses");
                return;
            }
            
            valuesStr = valuesStr.substring(1, valuesStr.length() - 1);
            String[] values = valuesStr.split(",");
            
            // Créer un record
            Record record = new Record();
            for (String value : values) {
                record.addValue(cleanValue(value));
            }
            
            // Insérer le record
            RecordId recordId = relation.InsertRecord(record);
            
            System.out.println("Record inséré avec succès. ID: " + recordId);
            
        } catch (Exception e) {
            System.err.println("Erreur lors de l'insertion: " + e.getMessage());
        }
    }
    
    // === SELECT * FROM nomTable ===
    public void ProcessSelectCommand(String[] tokens) {
        try {
            if (tokens.length < 4 || !tokens[1].equals("*") || !tokens[2].equalsIgnoreCase("FROM")) {
                System.err.println("Syntaxe: SELECT * FROM Table1 [, Table2] [WHERE ...]");
                return;
            }

            // Reconstruct command to parse easier
            StringBuilder sb = new StringBuilder();
            for (int i = 3; i < tokens.length; i++) sb.append(tokens[i]).append(" ");
            String rest = sb.toString().trim();

            String fromClause = rest;
            String whereClause = "";

            if (rest.toUpperCase().contains(" WHERE ")) {
                String[] parts = rest.toUpperCase().split(" WHERE ");
                fromClause = rest.substring(0, rest.toUpperCase().indexOf(" WHERE ")).trim();
                whereClause = rest.substring(rest.toUpperCase().indexOf(" WHERE ") + 7).trim();
            }

            String[] tableNames = fromClause.split(",");
            for(int i=0; i<tableNames.length; i++) tableNames[i] = tableNames[i].trim();

            // --- SINGLE TABLE SELECT ---
            if (tableNames.length == 1) {
                String tableName = tableNames[0];
                Relation relation = dbManager.getTable(tableName);
                if (relation == null) {
                    System.err.println("Table inconnue: " + tableName);
                    return;
                }
                ArrayList<Record> records = relation.GetAllRecords();
                int count = 0;
                
                for (Record r : records) {
                    if (evaluateCondition(r, relation, whereClause)) {
                        System.out.println(r);
                        count++;
                    }
                }
                System.out.println("Total selected records=" + count);
            }

            // --- JOIN (NESTED LOOP) ---
            else if (tableNames.length == 2) {
                String name1 = tableNames[0];
                String name2 = tableNames[1];
                Relation rel1 = dbManager.getTable(name1);
                Relation rel2 = dbManager.getTable(name2);

                if (rel1 == null || rel2 == null) {
                    System.err.println("Une des tables n'existe pas.");
                    return;
                }

                // Parse WHERE: T1.Col = T2.Col
                int idx1 = -1, idx2 = -1;
                
                if (whereClause.contains("=")) {
                    String[] cond = whereClause.split("=");
                    String left = cond[0].trim();  // e.g. S.C1
                    String right = cond[1].trim(); // e.g. R.C1

                    // Helper to find index
                    if(left.startsWith(name1 + ".")) idx1 = rel1.getColumnNames().indexOf(left.split("\\.")[1]);
                    else if(left.startsWith(name2 + ".")) idx2 = rel2.getColumnNames().indexOf(left.split("\\.")[1]);

                    if(right.startsWith(name1 + ".")) idx1 = rel1.getColumnNames().indexOf(right.split("\\.")[1]);
                    else if(right.startsWith(name2 + ".")) idx2 = rel2.getColumnNames().indexOf(right.split("\\.")[1]);
                }

                if (idx1 == -1 || idx2 == -1) {
                    System.err.println("Erreur: Colonnes de jointure introuvables ou syntaxe incorrecte (utilisez Table.Col = Table.Col)");
                    return;
                }

                // Execute Nested Loop
                ArrayList<Record> recs1 = rel1.GetAllRecords();
                ArrayList<Record> recs2 = rel2.GetAllRecords();
                int count = 0;

                for (Record r1 : recs1) {
                    for (Record r2 : recs2) {
                        if (r1.getValues().get(idx1).equals(r2.getValues().get(idx2))) {
                            System.out.println(r1 + " ; " + r2);
                            count++;
                        }
                    }
                }
                System.out.println("Total selected records = " + count);
            }

        } catch (Exception e) {
            System.err.println("Erreur Select: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    // === DELETE FROM nomTable WHERE ... ===
    public void ProcessDeleteCommand(String[] tokens) {
        try {
            // DELETE FROM nomRelation [WHERE ...]
            if (tokens.length < 3 || !tokens[1].equalsIgnoreCase("FROM")) {
                System.err.println("Syntaxe: DELETE FROM nomRelation [WHERE condition]");
                return;
            }

            String tableName = tokens[2];
            Relation relation = dbManager.getTable(tableName);
            if (relation == null) {
                System.err.println("Table inconnue: " + tableName);
                return;
            }

            String whereClause = "";
            // Reconstruct command line to find WHERE
            String cmd = String.join(" ", tokens);
            if (cmd.toUpperCase().contains(" WHERE ")) {
                whereClause = cmd.substring(cmd.toUpperCase().indexOf(" WHERE ") + 7).trim();
            }

            ArrayList<RecordId> rids = relation.getAllRecordIds();
            int deletedCount = 0;

            for (RecordId rid : rids) {
                Record r = relation.getRecord(rid);
                if (evaluateCondition(r, relation, whereClause)) {
                    relation.DeleteRecord(rid);
                    deletedCount++;
                }
            }
            System.out.println("Total deleted records=" + deletedCount);

        } catch (Exception e) {
            System.err.println("Erreur Delete: " + e.getMessage());
        }
    }
    
    // === DESCRIBE TABLE nomTable ou DESCRIBE TABLES ===
    public void ProcessDescribeCommand(String[] tokens) {
        try {
            if (tokens.length < 2) {
                System.err.println("Syntaxe: DESCRIBE TABLE nomTable ou DESCRIBE TABLES");
                return;
            }
            
            if (tokens.length >= 3 && tokens[1].equalsIgnoreCase("TABLE")) {
                // DESCRIBE TABLE nomTable
                String tableName = tokens[2];
                dbManager.DescribeTable(tableName);
            } else if (tokens[1].equalsIgnoreCase("TABLES")) {
                // DESCRIBE TABLES
                dbManager.DescribeAllTables();
            } else {
                // Pour compatibilité : DESCRIBE nomTable ou DESCRIBE *
                String tableName = tokens[1];
                if (tableName.equals("*")) {
                    dbManager.DescribeAllTables();
                } else {
                    dbManager.DescribeTable(tableName);
                }
            }
            
        } catch (Exception e) {
            System.err.println("Erreur lors de la description: " + e.getMessage());
        }
    }
    
    // === LIST TABLES ===
    public void ProcessListCommand(String[] tokens) {
        try {
            if (tokens.length < 2 || !tokens[1].equalsIgnoreCase("TABLES")) {
                System.err.println("Syntaxe: LIST TABLES");
                return;
            }
            
            System.out.println("Tables existantes:");
            for (String tableName : dbManager.getAllTableNames()) {
                System.out.println("- " + tableName);
            }
            
        } catch (Exception e) {
            System.err.println("Erreur lors du listage: " + e.getMessage());
        }
    }
    
    // === BMSETTINGS policy ===
    public void ProcessBmSettingsCommand(String[] tokens) {
        try {
            if (tokens.length < 2) {
                System.err.println("Syntaxe: BMSETTINGS <LRU|MRU>");
                return;
            }
            
            String policy = tokens[1].toUpperCase();
            
            if (!policy.equals("LRU") && !policy.equals("MRU")) {
                System.err.println("Politique invalide. Utilisez LRU ou MRU");
                return;
            }
            
            bufferManager.SetCurrentReplacementPolicy(policy);
            System.out.println("Politique de remplacement changée vers: " + policy);
            
        } catch (Exception e) {
            System.err.println("Erreur lors du changement de politique: " + e.getMessage());
        }
    }
    
    // === BMSTATE ===
    public void ProcessBmStateCommand(String[] tokens) {
        try {
            bufferManager.printState();
        } catch (Exception e) {
            System.err.println("Erreur lors de l'affichage de l'état du buffer: " + e.getMessage());
        }
    }

    public void ProcessImportCommand(String[] tokens) {
        try {
            if (tokens.length < 4 || !tokens[1].equalsIgnoreCase("INTO")) {
                System.err.println("Syntaxe: IMPORT INTO nomRelation nomFichier.csv");
                return;
            }

            String tableName = tokens[2];
            String fileName = tokens[3];
            Relation relation = dbManager.getTable(tableName);

            if (relation == null) {
                System.err.println("La table '" + tableName + "' n'existe pas");
                return;
            }

            try (java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(fileName))) {
                String line;
                int count = 0;
                while ((line = br.readLine()) != null) {
                    String[] values = line.split(",");
                    // Basic validation: check if value count matches column count
                    if (values.length != relation.getColumnNames().size()) {
                        System.err.println("Erreur ligne " + (count + 1) + ": nombre de valeurs incorrect");
                        continue;
                    }
                    
                    Record record = new Record();
                    for (String val : values) {
                        record.addValue(val.trim());
                    }
                    relation.InsertRecord(record);
                    count++;
                }
                System.out.println("Importation terminée : " + count + " enregistrements ajoutés.");
            } catch (java.io.IOException e) {
                System.err.println("Erreur de lecture du fichier : " + e.getMessage());
            }

        } catch (Exception e) {
            System.err.println("Erreur lors de l'import : " + e.getMessage());
        }
    }

    public void ProcessAppendCommand(String[] tokens) {
        try {
            // Syntax: APPEND INTO S ALLRECORDS (S.csv)
            if (tokens.length < 5 || !tokens[1].equalsIgnoreCase("INTO") || !tokens[3].equalsIgnoreCase("ALLRECORDS")) {
                System.err.println("Syntaxe: APPEND INTO nomRelation ALLRECORDS (nomFichier.csv)");
                return;
            }

            String tableName = tokens[2];
            String filenameRaw = tokens[4];
            
            // Remove parentheses if present: (S.csv) -> S.csv
            String filename = filenameRaw.replaceAll("[()]", "");

            Relation relation = dbManager.getTable(tableName);
            if (relation == null) {
                System.err.println("La table '" + tableName + "' n'existe pas");
                return;
            }

            try (java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(filename))) {
                String line;
                int count = 0;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;

                    // Split by comma
                    String[] values = line.split(",");
                    
                    if (values.length != relation.getColumnNames().size()) {
                        System.err.println("Erreur ligne " + (count + 1) + ": attendu " + relation.getColumnNames().size() + " colonnes, trouvé " + values.length);
                        continue;
                    }

                    Record record = new Record();
                    for (String val : values) {
                        record.addValue(cleanValue(val));
                    }
                    relation.InsertRecord(record);
                    count++;
                }
                System.out.println("Importation terminée : " + count + " enregistrements ajoutés.");
            } catch (java.io.IOException e) {
                System.err.println("Erreur de lecture du fichier : " + e.getMessage());
            }

        } catch (Exception e) {
            System.err.println("Erreur lors de l'import : " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void ProcessUpdateCommand(String[] tokens) {
        try {
            // UPDATE nomRel SET col=val [WHERE ...]
            if (tokens.length < 4 || !tokens[2].equalsIgnoreCase("SET")) {
                System.err.println("Syntaxe: UPDATE nomRel SET col=val [WHERE ...]");
                return;
            }

            String tableName = tokens[1];
            Relation relation = dbManager.getTable(tableName);
            if (relation == null) {
                System.err.println("Table inconnue: " + tableName);
                return;
            }

            String cmd = String.join(" ", tokens);
            String setClause = "";
            String whereClause = "";

            // Extract SET and WHERE parts
            int setIndex = cmd.toUpperCase().indexOf(" SET ") + 5;
            int whereIndex = cmd.toUpperCase().indexOf(" WHERE ");

            if (whereIndex != -1) {
                setClause = cmd.substring(setIndex, whereIndex).trim();
                whereClause = cmd.substring(whereIndex + 7).trim();
            } else {
                setClause = cmd.substring(setIndex).trim();
            }

            // Parse assignments (col1=val1, col2=val2)
            String[] assignments = setClause.split(",");
            
            ArrayList<RecordId> rids = relation.getAllRecordIds();
            int updatedCount = 0;

            for (RecordId rid : rids) {
                Record r = relation.getRecord(rid);
                if (evaluateCondition(r, relation, whereClause)) {
                    // Update record values
                    for (String assign : assignments) {
                        String[] parts = assign.split("=");
                        String colName = parts[0].trim();
                        String newVal = parts[1].trim().replace("\"", ""); // Remove quotes if string

                        // Handle Aliases
                        if (colName.contains(".")) colName = colName.split("\\.")[1];

                        int colIdx = relation.getColumnNames().indexOf(colName);
                        if (colIdx != -1) {
                            r.getValues().set(colIdx, newVal);
                        }
                    }
                    // Write updates to disk
                    relation.updateRecord(rid, r);
                    updatedCount++;
                }
            }
            System.out.println("Total updated records=" + updatedCount);

        } catch (Exception e) {
            System.err.println("Erreur Update: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // --- Condition Evaluator Helper ---
    private boolean evaluateCondition(Record record, Relation relation, String whereClause) {
        if (whereClause == null || whereClause.isEmpty()) return true;

        // Split by AND
        String[] conditions = whereClause.split(" AND ");

        for (String cond : conditions) {
            cond = cond.trim();
            String operator = "";
            if (cond.contains("<=")) operator = "<=";
            else if (cond.contains(">=")) operator = ">=";
            else if (cond.contains("<>")) operator = "<>";
            else if (cond.contains("=")) operator = "=";
            else if (cond.contains("<")) operator = "<";
            else if (cond.contains(">")) operator = ">";
            else return false; // Invalid operator

            String[] parts = cond.split(operator);
            String colName = parts[0].trim();
            String valStr = parts[1].trim();

            // Handle Aliases (e.g., s.C1 -> C1)
            if (colName.contains(".")) colName = colName.split("\\.")[1];

            int colIdx = relation.getColumnNames().indexOf(colName);
            if (colIdx == -1) return false; // Column not found

            String recVal = record.getValues().get(colIdx);
            String type = relation.getColumnTypes().get(colIdx).toLowerCase();

            // Compare based on type
            try {
                if (type.equals("int")) {
                    int v1 = Integer.parseInt(recVal);
                    int v2 = Integer.parseInt(valStr);
                    if (!compareInt(v1, v2, operator)) return false;
                } else if (type.equals("float") || type.equals("real")) {
                    float v1 = Float.parseFloat(recVal);
                    float v2 = Float.parseFloat(valStr);
                    if (!compareFloat(v1, v2, operator)) return false;
                } else {
                    // String comparison
                    if (!compareString(recVal, valStr.replace("\"", ""), operator)) return false;
                }
            } catch (Exception e) {
                return false; // Error parsing types
            }
        }
        return true;
    }

    // Helper to remove surrounding quotes from strings
    private String cleanValue(String val) {
        val = val.trim();
        if (val.length() >= 2 && val.startsWith("\"") && val.endsWith("\"")) {
            return val.substring(1, val.length() - 1);
        }
        return val;
    }

    private boolean compareInt(int v1, int v2, String op) {
        return switch (op) {
            case "=" -> v1 == v2;
            case "<" -> v1 < v2;
            case ">" -> v1 > v2;
            case "<=" -> v1 <= v2;
            case ">=" -> v1 >= v2;
            case "<>" -> v1 != v2;
            default -> false;
        };
    }

    private boolean compareFloat(float v1, float v2, String op) {
        return switch (op) {
            case "=" -> v1 == v2;
            case "<" -> v1 < v2;
            case ">" -> v1 > v2;
            case "<=" -> v1 <= v2;
            case ">=" -> v1 >= v2;
            case "<>" -> v1 != v2;
            default -> false;
        };
    }

    private boolean compareString(String v1, String v2, String op) {
        int cmp = v1.compareTo(v2);
        return switch (op) {
            case "=" -> cmp == 0;
            case "<" -> cmp < 0;
            case ">" -> cmp > 0;
            case "<=" -> cmp <= 0;
            case ">=" -> cmp >= 0;
            case "<>" -> cmp != 0;
            default -> false;
        };
    }    

    // === Méthode main ===
    public static void main(String[] args) {
        try {
            // Vérifier les arguments
            if (args.length != 1) {
                System.err.println("Usage: java SGBD <chemin_fichier_config>");
                System.exit(1);
            }
            
            // Charger la configuration
            String configPath = args[0];
            DBConfig config = DBConfig.LoadDBConfig(configPath);
            
            if (config == null) {
                System.err.println("Erreur: Impossible de charger la configuration depuis " + configPath);
                System.exit(1);
            }
            
            // Créer et lancer le SGBD
            SGBD sgbd = new SGBD(config);
            sgbd.Run();
            
        } catch (Exception e) {
            System.err.println("Erreur fatale: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

}
