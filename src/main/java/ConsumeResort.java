import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class ConsumeResort implements Runnable {

  private final Connection connection;
  private final Gson gson;
  private final JedisPool jedisPool;
  private final String queueName;

  public ConsumeResort(Connection connection, Gson gson, JedisPool jedisPool, String queueName) {
    this.connection = connection;
    this.gson = gson;
    this.jedisPool = jedisPool;
    this.queueName = queueName;
  }


  @Override
  public void run() {
    try {
      final Channel channel = connection.createChannel();

      channel.exchangeDeclare("direct_exchange", "direct");
      channel.queueBind(queueName, "direct_exchange", "RESORT");

      System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

      DeliverCallback deliverCallback = (consumerTag, delivery) -> {
        String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
        //System.out.println(" [x] Received '" + message + "'");
        try {
          doWork(message);
        } finally {
          //System.out.println(" [x] Done");
          channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        }
      };
      channel.basicConsume(queueName, false, deliverCallback, consumerTag -> {
      });
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void doWork(String task) {

    JsonSkierPost jsonSkierPost = gson.fromJson(task, JsonSkierPost.class);
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.sadd("resort",
          jsonSkierPost.resortID + ":" + jsonSkierPost.seasonsID + ":" + jsonSkierPost.dayID + ":"
              + jsonSkierPost.dayID + ":" + jsonSkierPost.waitTime);
    }
  }
}