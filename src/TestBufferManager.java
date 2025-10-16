public class TestBufferManager {
		public static void main(String[] args) {
			try {
				DBConfig cfg = new DBConfig("/tmp/db", 512, 5, 3, "LRU");
				DiskManager dm = new DiskManager(cfg); // ton implémentation existante
				BufferManager bm = new BufferManager(cfg, dm);

				PageId p1 = new PageId(1, 1);
				PageId p2 = new PageId(1, 2);
				PageId p3 = new PageId(1, 3);
				PageId p4 = new PageId(1, 4);

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
