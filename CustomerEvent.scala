package rastest

import java.time.LocalDate

case class CustomerEvent(
                          //partition_id: Int,
                          //tenant_id: String,
                          customer_id: String,
                          event_timestamp: Long,
                          //logical_date: LocalDate,
                          event_type: String,
                          value: Array[Byte]
                        )
