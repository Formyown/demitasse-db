import io.dynabiz.demitassedb.DB;
import io.dynabiz.demitassedb.MapDB;
import org.junit.Test;

public class MapDBTest {
    @Test
    public void dbTest() throws InterruptedException {
        DB db = new MapDB();


        for(int i = 0; i < 50; i++){
            db.set("key" + i, "Value" + i);
        }

        for(int i = 0; i < 50; i++){
            db.set("key" + i + "ttl" + i * 10, "Value" + i,i  * 100);
        }

        for (int i = 0; i < 50; i++){
            if(null != db.get("key" + i + "ttl" + (i * 10)))
                throw new RuntimeException("Test error");
            Thread.sleep(101);
        }


    }
}
