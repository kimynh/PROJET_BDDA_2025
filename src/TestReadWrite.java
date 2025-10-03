import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class TestReadWrite {
    public static void main(String[] args) throws Exception {
        DBConfig cfg = DBConfig.LoadDBConfig("src/fichier_conf.json");
        try (DiskManager dm = new DiskManager(cfg)) {
            dm.Init();

            // allocate a new page
            PageId pid = dm.AllocPage();
            System.out.println("Allocated: " + pid);

            // prepare a buffer with "HELLO" in it
            byte[] writeBuff = new byte[cfg.getPagesize()];
            byte[] msg = "HELLO".getBytes(StandardCharsets.UTF_8);
            System.arraycopy(msg, 0, writeBuff, 0, msg.length);

            // write page
            dm.WritePage(pid, writeBuff);
            System.out.println("Wrote page with message HELLO");

            // read back into new buffer
            byte[] readBuff = new byte[cfg.getPagesize()];
            dm.ReadPage(pid, readBuff);

            // compare the content
            String readMsg = new String(readBuff, 0, msg.length, StandardCharsets.UTF_8);
            System.out.println("Read back: " + readMsg);
            System.out.println("Equal? " + Arrays.equals(writeBuff, readBuff));
        }
    }
}
