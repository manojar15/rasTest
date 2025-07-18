package rastest

import org.apache.spark.sql.{Dataset, SparkSession}
import rastest.handlers._

import java.io.File
import java.nio.file.{Files, Paths}
import java.sql.Date

object PartitionedCustomerUpdatesJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Create CustomerUpdates")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val logicalDate = "2025-04-03"
    val date = Date.valueOf(logicalDate)
    val tenantId = if (args.length > 0) args(0) else "42"
    val tenantDataPath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/tenant_data"
    val snapshotPath = "C:/Users/e5655076/RAS_RPT/obrandrastest/customer/initial_customers_avro"
    val deltaCustomerPath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/customer_snapshot_delta"
    val customerUpdateOutputPath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/customerupdates"

    // Register available handlers in a registry (could also use a ServiceLoader or reflection)
    val handlers: Map[String, EventHandler] = {
      val list = Seq(
        new AddressChangeHandler,
        new NameUpdateHandler,
        new IdentificationChangeHandler
      )
      list.map(h => h.eventType -> h).toMap
    }

    val eventDS = spark.read.format("parquet")
      .load(s"$tenantDataPath/tenant_id=$tenantId/partition_id=*/logical_date=$logicalDate")
      .as[CustomerEvent]

    val snapshot = findLatestSnapshot(spark, snapshotPath, deltaCustomerPath, logicalDate)

    // Key datasets by customer ID
    val customersById = snapshot.groupByKey(_.customer_id)
    val eventsById = eventDS.groupByKey(_.customer_id)

    // Join customers with their events
    val customerUpdates = customersById.cogroup(eventsById) { (id, custIter, eventsIter) =>
      // This function is called per customerId key
      var customer = if (custIter.hasNext) custIter.next() else createNewCustomer(id, date)
      val eventList = eventsIter.toList

      if (eventList.nonEmpty) {
        val allCustomerUpdates = scala.collection.mutable.ListBuffer[CustomerUpdate]()

        eventList.foreach { event =>
          handlers.get(event.event_type) match {
            case Some(handler) =>
              val updates = handler.handle(customer, event)
              customer = mergeSubObjects(id, customer, updates)
              allCustomerUpdates ++= createCustomerUpdates(id, updates.deltaEntries)
            case None => // ignore
          }
        }
        allCustomerUpdates.toIterator // << key change: flat output
      } else {
        Iterator.empty
      }
    }

    customerUpdates.write
          .format("avro")
          .mode("overwrite")
          .save(s"$customerUpdateOutputPath/logical_date=$logicalDate")

    spark.stop()
  }

  def findLatestSnapshot(spark: SparkSession, snapshotPath: String, deltaBasePath: String, currentDate: String): Dataset[Customer] = {
    import spark.implicits._
    val fsPath = Paths.get(deltaBasePath.stripPrefix("file:///"))
    val baseDS = spark.read.format("avro").load(snapshotPath).as[Customer]

    if (!Files.exists(fsPath)) return baseDS

    val logicalDates = new File(fsPath.toString).listFiles()
      .filter(_.isDirectory)
      .map(_.getName.stripPrefix("logical_date="))
      .filter(_ < currentDate)
      .sorted

    val deltaDSs = logicalDates.map(date => spark.read.format("avro").load(s"$deltaBasePath/logical_date=$date").as[Customer])

    if (deltaDSs.nonEmpty) {
      deltaDSs.foldLeft(baseDS)((acc, delta) => acc.unionByName(delta).dropDuplicates("customer_id"))
    } else {
      baseDS
    }
  }

  def createNewCustomer(customerId: String, date: Date): Customer = {
    Customer("42", customerId.split("_")(0), customerId, date, null, Seq.empty, Seq.empty)
  }

  def mergeSubObjects(customerId: String, customer: Customer, updates: UpdateOutput): Customer = {
    updates.masterRecordUpdates.foldLeft(customer) {
      case (cust, ("name", newName: Name)) => cust.copy(name = newName)
      case (cust, ("addresses", newAddr: Seq[Address])) => cust.copy(addresses = newAddr)
      case (cust, ("identification", newId: Seq[Identification])) => cust.copy(identifications = newId)
      case (cust, _) => cust // ignore unknown fields
    }
  }

  def createCustomerUpdates(currentCustomerId: String, deltaEntries : Seq[DeltaEntry]) : Seq[CustomerUpdate] = {
    deltaEntries.map { entry =>
      entry.objectType match {
        case "name" =>
          CustomerUpdate("42", currentCustomerId.split("_")(0), customer_id = currentCustomerId, oldName = entry.oldValue.asInstanceOf[Name], newName = entry.newValue.asInstanceOf[Name])

        case "address" =>
          CustomerUpdate("42", currentCustomerId.split("_")(0), customer_id = currentCustomerId, oldAddress = entry.oldValue.asInstanceOf[Address], newAddress = entry.newValue.asInstanceOf[Address])

        case "identification" =>
          CustomerUpdate("42", currentCustomerId.split("_")(0), customer_id = currentCustomerId, oldId = entry.oldValue.asInstanceOf[Identification], newId = entry.newValue.asInstanceOf[Identification])

        case other =>
          throw new IllegalArgumentException(s"Unsupported objectType: $other")
      }
    }
  }
}


