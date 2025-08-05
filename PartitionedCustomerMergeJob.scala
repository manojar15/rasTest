package rastest

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel
import rastest.handlers._

import java.io.File
import java.nio.file.{Files, Paths}
import java.sql.Date

object PartitionedCustomerMergeJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Merge Customer Delta Events")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val logicalDate = "2025-04-03"
    val date = Date.valueOf(logicalDate)
    val tenantId = if (args.length > 0) args(0) else "42"
    val writeMode = if (args.length > 1) args(1) else "modified" // default: modified
    val produceCustomerUpdates = false
    val tenantDataPath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/tenant_data"
    //val snapshotPath = "C:/Users/e5655076/RAS_RPT/obrandrastest/customer/initial_customers_avro"
    val snapshotPath = "C:/Users/e5655076/RAS_RPT/obrandrastest/customer/customer_snapshot_avro"
    val snapshotOutputPath = "C:/Users/e5655076/RAS_RPT/obrandrastest/customer/customer_snapshot_avro"
    val deltaCustomerPath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/customer_snapshot_delta"
    val deltaOutputPath = "file:///C:/Users/e5655076/RAS_RPT/obrandrastest/customer/customerupdates"

    // Register available handlers in a registry
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

    // Join customers with their events (left join to include customers without events)
    val merged = customersById.cogroup(eventsById) { (id, custIter, eventsIter) =>
      // This function is called per customerId key
      val customerOpt = custIter.toList.headOption // the customer record for this ID or create a new one
      val baseCustomer = customerOpt.getOrElse(createNewCustomer(id, date))

      val eventList = eventsIter.toList

      if (eventList.nonEmpty) {
        var customer = baseCustomer
        val allCustomerUpdates = scala.collection.mutable.ListBuffer[CustomerUpdate]()
        var wasModified = false

        eventList.foreach { event =>
          handlers.get(event.event_type) match {
            case Some(handler) =>
              val updates = handler.handle(customer, event)
              if (updates.masterRecordUpdates.nonEmpty || updates.deltaEntries.nonEmpty) {
                wasModified = true
              }
              customer = mergeSubObjects(id, customer, updates)
            case None => // ignore
          }
        }
        Iterator((customer, allCustomerUpdates.toSeq, wasModified))
      } else {
        Iterator((baseCustomer, Seq.empty[CustomerUpdate], false))
      }
    }.persist(StorageLevel.DISK_ONLY)

    //split data sets
    val customerDS = merged.map(_._1)
    val customerUpdatesDS = merged.flatMap(_._2)
    val modifiedCustomers = merged.filter(_._3).map(_._1)

    if (writeMode == "full" || writeMode == "both") {
      customerDS.write
        .format("avro")
        .mode("overwrite")
        .save(s"$snapshotOutputPath/logical_date=$logicalDate")
    }

    if (writeMode == "modified" || writeMode == "both") {
      modifiedCustomers.write
        .format("avro")
        .mode("overwrite")
        .save(s"$deltaCustomerPath/logical_date=$logicalDate")

      if(produceCustomerUpdates) {
        customerUpdatesDS.write
          .format("parquet")
          .mode("overwrite")
          .save(s"$deltaOutputPath/logical_date=$logicalDate")
      }
    }

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
}


