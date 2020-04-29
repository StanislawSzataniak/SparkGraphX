import model.{Station, Trip}
import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.util.hashing.MurmurHash3


object GraphExample extends App {
    override def main(args: Array[String]): Unit = {
        setLibsLoggingLevel()
        val sc: SparkContext = createSparkContext()

        val df1: RDD[Station] = loadStations(sc)
        val df2: RDD[Trip] = loadTrips(sc)

        val g1: Graph[Station, Trip] = createGraph(df1, df2)

        val (bikeId: String, count: Int) = findBikeWithMaxNumberOfTrips(g1)
        println(s"The most commonly used bike: $bikeId  count: $count")

        /* remove unnecessary edges */
        val g2: Graph[Station, Trip] = g1.subgraph((epred: EdgeTriplet[Station, Trip]) => epred.attr.bikeId == bikeId,
            (_: VertexId, _: Station) => true)

        /* remove vertices without incoming and outgoing edges */
        val g3: Graph[Station, Trip] = g2.filter(graph => {
            val degrees: VertexRDD[Int] = graph.degrees
            graph.outerJoinVertices(degrees) {
                (_: VertexId, _: Station, deg: Option[Int]) => deg.getOrElse(0)
            }
        }, vpred = (_: VertexId, deg: Int) => deg > 0)

        /* Find starting station */
        val maxInDegree: (VertexId, Int) = g3.inDegrees.reduce(max)
        val startStation: Station = g3.vertices.filter(pred => pred._1 == maxInDegree._1).collect()(0)._2
        println(s"The most frequently used station as starting station: ${startStation.name}  count: ${maxInDegree._2}")

        /* Find final station */
        val maxOutDegree: (VertexId, Int) = g3.outDegrees.reduce(max)
        val finalStation: Station = g3.vertices.filter(pred => pred._1 == maxOutDegree._1).collect()(0)._2
        println(s"The most frequently used station as final station: ${finalStation.name}  count: ${maxOutDegree._2}")
    }

    def setLibsLoggingLevel(): Unit = {
        Logger.getLogger(classOf[RackResolver]).getLevel
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)
    }

    def createSparkContext(): SparkContext = {
        val conf: SparkConf = new SparkConf()
            .setAppName("GraphX Example")
            .setMaster("local")
        new SparkContext(conf)
    }

    def loadStations(spark: SparkContext): RDD[Station] = {
        val stations = spark.textFile("station.csv")
        stations.filter(!_.startsWith("\"station_id\""))
            .map(_.replaceAll("\"", ""))
            .map(_.split(","))
            .map(array => Station(array(0), array(1)))
    }

    def loadTrips(spark: SparkContext): RDD[Trip] = {
        val trips = spark.textFile("trip.csv")
        trips.filter(!_.startsWith("\"trip_id\""))
            .map(_.replaceAll("\"", ""))
            .map(_.split(","))
            .map(array => Trip(array(0).toInt, array(1), array(2), array(3),
                array(4).toDouble, array(5), array(6), array(7), array(8)))
    }

    def createGraph(stations: RDD[Station], trips: RDD[Trip]): Graph[Station, Trip] = {
        val vertices: RDD[(VertexId, Station)] = stations
            .map(station => (MurmurHash3.stringHash(station.id).toLong, station))

        val edges: RDD[Edge[Trip]] = trips.map(trip => Edge(
            MurmurHash3.stringHash(trip.fromStationId).toLong,
            MurmurHash3.stringHash(trip.toStationId).toLong,
            trip))

        Graph(vertices, edges)
    }

    def findBikeWithMaxNumberOfTrips(graph: Graph[Station, Trip]): (String, Int) = {
        graph.edges
            .map(edge => (edge.attr.bikeId, 1))
            .reduceByKey(_ + _)
            .max()((x: (String, Int), y: (String, Int)) => Ordering[Int].compare(x._2, y._2))
    }

    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
        if (a._2 > b._2) a else b
    }
}
