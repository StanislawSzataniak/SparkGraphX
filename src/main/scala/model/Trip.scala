package model

case class Trip(
                   id: Int,
                   startTime: String,
                   stopTime: String,
                   bikeId: String,
                   tripDuration: Double,
                   fromStartName: String,
                   toStationName: String,
                   fromStationId: String,
                   toStationId: String
               )
