package com.stratio.spark.sql.examples

import com.datastax.spark.connector.cql.CassandraConnector
import com.stratio.datasource.mongodb.config.MongodbConfig
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.{SQLContext, _}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.elasticsearch.spark.sql._

object SparkSqlExamples extends App with Logging {

  private def executeCommand(conn: CassandraConnector, command: String): Unit = {
    conn.withSessionDo(session => session.execute(command))
  }

  /**
   * Spark Configuration and Create Spark Contexts
   */
  val sparkConf = new SparkConf().setAppName("sparkSQLExamples").setMaster("local[*]")
    .setIfMissing("hive.execution.engine", "spark")
    .setIfMissing("spark.cassandra.connection.host", "127.0.0.1")
    .setIfMissing("spark.cassandra.connection.port", "9042")
  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sparkContext)

  /**
   * You need install mysql and create one user stratio with password stratio, specified in hive-site.xml
   */
  val hiveContext = new HiveContext(sparkContext)

  /**
   * Variables to connect and save into DataSources
   */
  val cassandraFormat = "org.apache.spark.sql.cassandra"
  val cassandraKeyspace = "testkeyspace"
  val cassandraTable = "cassandradf"

  val elasticFormat = "org.elasticsearch.spark.sql"
  val elasticIndex = "elasticindex"
  val elasticMapping = "elasticdf"
  val elasticMappingLib = "elasticlibrary"

  val mongoDbFormat = "com.stratio.datasource.mongodb"
  val mongoDbDatabase = "mongodatabase"
  val mongoDbCollection = "mongodf"

  /**
   * Create Cassandra keyspace and Table, MongoDB and Elastic can create the tables or indexes
   */
  val connector = CassandraConnector(sparkContext.getConf)
  executeCommand(connector,
    s"CREATE KEYSPACE IF NOT EXISTS $cassandraKeyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
  executeCommand(connector,
    s"CREATE TABLE IF NOT EXISTS $cassandraKeyspace.$cassandraTable (id varchar PRIMARY KEY)")

  /**
   * Schema + RDD[Row] -> DataFrame or RDD[Case class] -> DataFrame
   */
  val schema = new StructType(Array(StructField("id", StringType, false)))
  val registers = for (a <- 0 to 10000) yield a.toString
  val rdd = sparkContext.parallelize(registers)

  //With RDD[Row]
  val rddOfRow = rdd.map(Row(_))
  val dataFrame = sqlContext.createDataFrame(rddOfRow, schema)

  //With RDD[Case class]
  case class DfClass(id: String)

  val rddOfClass = rdd.map(data => DfClass(data))
  val dataFrameClass = sqlContext.createDataFrame(rddOfClass)


  /**
   * WRITE in DataSources: ElasticSearch, Cassandra and Mongodb
   */

  /**
   * ElasticSearch
   */
  val elasticOptions = Map("es.mapping.id" -> "id",
    "es.nodes" -> "localhost",
    "es.port" -> "9200",
    "es.index.auto.create" -> "yes"
  )

  //with DataFrame methods
  dataFrame.write.format(elasticFormat)
    .mode(SaveMode.Append)
    .options(elasticOptions)
    .save(s"$elasticIndex/$elasticMapping")

  //with library method
  dataFrame.saveToEs(s"$elasticIndex/$elasticMappingLib", elasticOptions)

  /**
   * MongoDB
   */
  val mongoDbOptions = Map(
    MongodbConfig.Host -> "localhost:27017",
    MongodbConfig.Database -> mongoDbDatabase,
    MongodbConfig.Collection -> mongoDbCollection
  )

  //with DataFrame methods
  dataFrame.write
    .format(mongoDbFormat)
    .mode(SaveMode.Append)
    .options(mongoDbOptions)
    .save()

  /**
   * Cassandra
   */
  val cassandraOptions = Map("table" -> cassandraTable, "keyspace" -> cassandraKeyspace)

  //with DataFrame methods
  dataFrame.write
    .format(cassandraFormat)
    .mode(Append)
    .options(cassandraOptions)
    .save()


  /**
   * READ from all DataSources: ElasticSearch, Cassandra and Mongodb
   */

  log.info("Printing library tables ....")

  sqlContext.read.format(elasticFormat)
    .options(elasticOptions)
    .load(s"$elasticIndex/$elasticMappingLib")
    .show()

  log.info("Printing temporal tables ....")

  val dataFrameSelectElastic = sqlContext.read.format(elasticFormat)
    .options(elasticOptions)
    .load(s"$elasticIndex/$elasticMapping")
    .select("id")

  val dataFrameSelectMongo = sqlContext.read.format(mongoDbFormat)
    .options(mongoDbOptions)
    .load()
    .select("id")

  val dataFrameSelectCassandra = sqlContext.read.format(cassandraFormat)
    .options(cassandraOptions)
    .load()
    .select("id")

  /**
   * Create temporary tables from Dataframes and then using as a table with SQLContext
   */
  dataFrameSelectElastic.registerTempTable("tempelastic")
  dataFrameSelectMongo.registerTempTable("tempmongo")
  dataFrameSelectCassandra.registerTempTable("tempcassandra")

  sqlContext.sql("select * from tempelastic").show()
  sqlContext.sql("select * from tempmongo").show()
  sqlContext.sql("select * from tempcassandra").show()

  /**
   * Register External tables with SQLContext not working, now are experimental use xDContext instead
   */
  /*
  xDContext.createExternalTable("externalelastic", elasticFormat, schema, elasticOptions)
  xDContext.createExternalTable("externalmongo", mongoDbFormat, schema, mongoDbOptions)
  xDContext.createExternalTable("externalcassandra", cassandraFormat, schema, cassandraOptions)

  xDContext.sql("select * from externalelastic").show()
  xDContext.sql("select * from externalmongo").show()
  xDContext.sql("select * from externalcassandra").show()
  */

  /**
   * Using Hive Context and persist tables from Cassandra, MongoDB and Elastic into HIVE MetaStore
   */
  hiveContext.sql(
    s"""CREATE TABLE IF NOT EXISTS hiveElastic(id STRING)
        |USING $elasticFormat
        |OPTIONS (
        |   path '$elasticIndex/$elasticMapping', readMetadata 'true', nodes '127.0.0.1', port '9200', cluster 'default'
        | )
     """.stripMargin)

  hiveContext.sql(
    s"""CREATE TABLE IF NOT EXISTS hiveCassandra(id STRING)
        |USING $cassandraFormat
        |OPTIONS (
        |   table '$cassandraTable', keyspace '$cassandraKeyspace'
        | )
     """.stripMargin)

  hiveContext.sql(
    s"""CREATE TABLE IF NOT EXISTS hiveMongo(id STRING)
        |USING $mongoDbFormat
        |OPTIONS (
        |  host 'localhost:27017', database '$mongoDbDatabase', collection '$mongoDbCollection'
        | )
     """.stripMargin)

  /**
   * Read data with Hive Context from external tables created in the MetaStore
   */
  val queryElastic = hiveContext.sql(s"SELECT id FROM hiveElastic limit 100")
  queryElastic.show()
  log.info("Elastic Count: " + queryElastic.count())

  val queryMongo = hiveContext.sql(s"SELECT id FROM hiveMongo limit 100")
  queryMongo.show()
  log.info("Mongo Count: " + queryMongo.count())

  val queryCassandra = hiveContext.sql(s"SELECT id FROM hiveCassandra limit 100")
  queryCassandra.show()
  log.info("Cassandra Count: " + queryCassandra.count())

  /**
   * JOIN with three DataSources
   */
  val joinElasticCassandraMongo = hiveContext.sql(
    s"SELECT tc.id from hiveCassandra as tc" +
      s" JOIN hiveElastic as te ON tc.id = te.id" +
      s" JOIN hiveMongo tm on tm.id = te.id"
  )
  joinElasticCassandraMongo.show()

  log.info("Join Count: " + joinElasticCassandraMongo.count())

  sys.exit(0)
}
