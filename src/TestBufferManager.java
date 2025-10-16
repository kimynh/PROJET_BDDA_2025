public class TestBufferManager {
		public static void main(String[] args) {
			try {
				DBConfig cfg = new DBConfig("db", 100, 5, 3, "LRU");
				DiskManager dm = new DiskManager(cfg); // ton implémentation existante
				dm.Init(); // Création automatique des dossiers nécessaires
				BufferManager bm = new BufferManager(cfg, dm);

				PageId p1 = dm.AllocPage();
				PageId p2 = dm.AllocPage();
				PageId p3 = dm.AllocPage();
				PageId p4 = dm.AllocPage();

				bm.GetPage(p1);
				bm.GetPage(p2);
				bm.GetPage(p3);

				bm.printState();

				bm.FreePage(p1, false);
				bm.FreePage(p2, false);
				bm.FreePage(p3, false);

				bm.GetPage(p4); // déclenche remplacement (LRU)

				bm.printState();

				bm.FlushBuffers();
				
				System.out.println("✅ Test terminé avec succès.");
					} catch (Exception e) {
							e.printStackTrace();
                            System.out.println("Erreur : " + e.getMessage());
					}
			}
}
