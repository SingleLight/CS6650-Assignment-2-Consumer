import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import redis.clients.jedis.JedisPool;

public class WorkerResort {

  private static final Gson gson = new Gson();

  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    factory.setUsername("user");
    factory.setPassword("user");
    final Connection connection = factory.newConnection();
    int num = 1;
    if (argv != null && argv.length != 0) {
      try {
        num = Integer.parseInt(argv[0]);
      } catch (NumberFormatException e) {
        e.printStackTrace();
      }
    }
    final Channel channel = connection.createChannel();
    String queueName = channel.queueDeclare().getQueue();
    JedisPool pool = new JedisPool("localhost", 6379);
    ExecutorService threadPool = Executors.newFixedThreadPool(128);
    for (int i = 0; i < num; i++) {
      Thread thread = new Thread(new ConsumeResort(connection, gson, pool, queueName));
      threadPool.execute(thread);
    }
  }
}