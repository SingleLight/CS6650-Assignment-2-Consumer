import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class Consume implements Runnable{

  private final Connection connection;
  private final String TASK_QUEUE_NAME;
  private final Gson gson;
  private final Map<Integer, Integer> liftIDtoTime;

  public Consume(Connection connection, String TASK_QUEUE_NAME, Gson gson,
      Map<Integer, Integer> liftIDtoTime) {
    this.connection = connection;
    this.TASK_QUEUE_NAME = TASK_QUEUE_NAME;
    this.gson = gson;
    this.liftIDtoTime = liftIDtoTime;
  }


  @Override
  public void run() {
      try {
        final Channel channel = connection.createChannel();

        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        channel.basicQos(0);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
          String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
          System.out.println(" [x] Received '" + message + "'");
          try {
            doWork(message);
          } finally {
            System.out.println(" [x] Done");
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
          }
        };
        channel.basicConsume(TASK_QUEUE_NAME, false, deliverCallback, consumerTag -> {
        });
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  private void doWork(String task) {
    JsonLift jsonLift = gson.fromJson(task, JsonLift.class);
    liftIDtoTime.put(jsonLift.liftID, jsonLift.time);
  }
}
