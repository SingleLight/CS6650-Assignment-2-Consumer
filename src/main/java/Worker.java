import com.google.gson.Gson;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Worker {

  private static final Map<Integer, Integer> liftIDtoTime = new ConcurrentHashMap<>();
  private static final Gson gson = new Gson();
  private static final String TASK_QUEUE_NAME = "task_queue";

  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    final Connection connection = factory.newConnection();
    int num = 1;
    if (argv != null && argv.length != 0) {
      try {
        num = Integer.parseInt(argv[0]);
      } catch (NumberFormatException e) {
        e.printStackTrace();
      }
    }
    ExecutorService threadPool = Executors.newFixedThreadPool(128);
    for (int i = 0; i < num; i++) {
      Thread thread = new Thread(new Consume(connection, TASK_QUEUE_NAME, gson, liftIDtoTime));
      threadPool.execute(thread);
    }
  }
}