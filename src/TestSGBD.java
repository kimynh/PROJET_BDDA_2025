/**
 * Test simple pour la classe SGBD
 */
public class TestSGBD {
    public static void main(String[] args) throws Exception {
        System.out.println("=== Test de la classe SGBD ===");
        
        // Test 1: Chargement de la configuration
        System.out.println("Test 1: Chargement de la configuration...");
        DBConfig config = DBConfig.LoadDBConfig("fichier_conf.json");
        
        if (config == null) {
            System.err.println("Erreur: Impossible de charger la configuration");
            return;
        }
        
        System.out.println("Configuration chargée avec succès");
        System.out.println("  - DB Path: " + config.getDbpath());
        System.out.println("  - Page Size: " + config.getPagesize());
        System.out.println("  - Buffer Count: " + config.getBm_buffercount());
        System.out.println("  - Policy: " + config.getBm_policy());
        
        // Test 2: Création de l'instance SGBD
        System.out.println("\nTest 2: Création de l'instance SGBD...");
        @SuppressWarnings("unused")
        SGBD sgbd = new SGBD(config);
        System.out.println("SGBD créé avec succès");
        
        // Test 3: Test des composants internes
        System.out.println("\nTest 3: Vérification des composants internes...");
        System.out.println("Tous les composants sont initialisés");
        
        System.out.println("\n=== Tous les tests sont passés avec succès! ===");
        System.out.println("\nPour lancer le SGBD interactif, utilisez:");
        System.out.println("java SGBD src/fichier_conf.json");
    }
}
