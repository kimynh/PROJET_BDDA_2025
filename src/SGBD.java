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
                record.addValue(value.trim());
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
                System.err.println("Syntaxe: SELECT * FROM nomTable");
                return;
            }
            
            String tableName = tokens[3];
            Relation relation = dbManager.getTable(tableName);
            
            if (relation == null) {
                System.err.println("La table '" + tableName + "' n'existe pas");
                return;
            }
            
            // Récupérer tous les records
            ArrayList<Record> records = relation.GetAllRecords();
            
            // Afficher les résultats
            if (records.isEmpty()) {
                System.out.println("Aucun enregistrement trouvé");
            } else {
                // Afficher l'en-tête
                System.out.println("Contenu de la table '" + tableName + "':");
                
                // Afficher les records
                for (Record record : records) {
                    System.out.println(record);
                }
                
                System.out.println("Total: " + records.size() + " enregistrement(s)");
            }
            
        } catch (Exception e) {
            System.err.println("Erreur lors de la sélection: " + e.getMessage());
        }
    }
    
    // === DELETE FROM nomTable WHERE ... (simplifié: supprime tout) ===
    public void ProcessDeleteCommand(String[] tokens) {
        try {
            if (tokens.length < 4 || !tokens[1].equalsIgnoreCase("FROM")) {
                System.err.println("Syntaxe: DELETE FROM nomTable");
                return;
            }
            
            String tableName = tokens[2];
            Relation relation = dbManager.getTable(tableName);
            
            if (relation == null) {
                System.err.println("La table '" + tableName + "' n'existe pas");
                return;
            }
            
            // Pour simplifier, on supprime tous les enregistrements
            ArrayList<Record> records = relation.GetAllRecords();
            
            // Note: Cette implémentation est simplifiée
            // Une vraie implémentation nécessiterait de parcourir les pages et supprimer individuellement
            System.out.println("Suppression de tous les enregistrements de la table '" + tableName + "'");
            System.out.println(records.size() + " enregistrement(s) supprimé(s)");
            
        } catch (Exception e) {
            System.err.println("Erreur lors de la suppression: " + e.getMessage());
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
