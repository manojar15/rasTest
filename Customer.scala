package rastest

import java.sql.Date

case class Customer(
                     tenant_id: String,
                     partition_id: String,
                     customer_id: String,
                     logical_date: Date,
                     name: Name,
                     addresses: Seq[Address],
                     identifications: Seq[Identification]
                   )

case class CustomerUpdate(
                           tenant_id: String,
                           partition_id: String,
                           customer_id: String,
                           event_timestamp: Long = 0,
                           event_type: String = null,
                           oldName: Name = null,
                           newName: Name = null,
                           oldAddress: Address = null,
                           newAddress: Address = null,
                           oldId: Identification = null,
                           newId: Identification = null
                         )

case class Name(
                 firstName: String,
                 middleName: String,
                 lastName: String,
               )

case class Address(
                    `type`: AddressType.Value,
                    street: String,
                    city: String,
                    postal_code: String,
                    country: String
                  )

object AddressType extends Enumeration {
  val HOME, WORK = Value
}

case class Identification(
                           `type`: String,
                           number: String,
                           issuer: String
                         )
