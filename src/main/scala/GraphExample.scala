import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}


object GraphExample extends App {



  Logger.getLogger(classOf[RackResolver]).getLevel
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val conf = new SparkConf().setAppName("GraphX Example").setMaster("local")
  val sc = new SparkContext(conf)

  val trips = sc.textFile("trip.csv")
  val weather = sc.textFile("weather.csv")
  val stations = sc.textFile("station.csv")

  case class Trip(
                  trip_id: Int,
                  starttime: String,
                  stoptime: String,
                  bikeid: String,
                  tripduration: Double,
                  from_start_name: String,
                  to_station_name: String,
                  from_station_id: String,
                  to_station_id: String
                 )

  case class Station(
                    station_id: String,
                    name: String
                    )

  val df1 = trips.filter(!_.startsWith("\"trip_id\""))
    .map(_.replace("\"", ""))
    .map(_.split(","))
    .map(array => Trip(array(0).toInt, array(1), array(2), array(3), array(4).toDouble, array(5), array(6), array(7), array(8)))

  val df2 = stations.filter(!_.startsWith("\"station_id\""))
    .map(_.replace("\"", ""))
    .map(_.split(","))
    .map(array => Station(array(0), array(1)))

  val v1 = df2.map(station => (scala.util.hashing.MurmurHash3.stringHash(station.station_id).toLong, station))
  val e1 = df1.map(trip => Edge(scala.util.hashing.MurmurHash3.stringHash(trip.from_station_id).toLong, scala.util.hashing.MurmurHash3.stringHash(trip.to_station_id).toLong, trip))

  val g1 = Graph(v1, e1)

  val count1 = g1.edges.map(edge => (edge.attr.bikeid, 1)).reduceByKey(_+_).max()(new Ordering[Tuple2[String, Int]]() {
    override def compare(x: (String, Int), y: (String, Int)): Int =
      Ordering[Int].compare(x._2, y._2)})


//  val filtered = g1.vertices.filter
//  val validGraph = g1.subgraph(vpred = (id, attr) => attr._2 != "Missing")
  // Restrict the answer to the valid subgraph
//  val validCCGraph = ccGraph.mask(validGraph)







  val users: RDD[(VertexId, (String, String))] =
    sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
  // Create an RDD for edges
  val relationships: RDD[Edge[String]] =
    sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
  val defaultUser = ("John Doe", "Missing")

  val graph = Graph(users, relationships, defaultUser)

  graph.vertices.mapValues(_._1).collect().foreach(println)

}
