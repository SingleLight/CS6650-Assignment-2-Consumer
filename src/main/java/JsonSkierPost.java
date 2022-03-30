public class JsonSkierPost {

  String resortID;
  String seasonsID;
  String dayID;
  String skierID;
  Integer time;
  Integer liftID;
  Integer waitTime;

  public JsonSkierPost(String resortID, String seasonsID, String dayID, String skierID,
      Integer time, Integer liftID, Integer waitTime) {
    this.resortID = resortID;
    this.seasonsID = seasonsID;
    this.dayID = dayID;
    this.skierID = skierID;
    this.time = time;
    this.liftID = liftID;
    this.waitTime = waitTime;
  }

}
