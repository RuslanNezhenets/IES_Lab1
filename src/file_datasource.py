from csv import reader
from datetime import datetime
from domain.aggregated_data import AggregatedData
from domain.accelerometer import Accelerometer
from domain.gps import Gps
from domain.parking import Parking


class FileDatasource:
    def __init__(self, accelerometer_filename: str, gps_filename: str, parking_filename: str) -> None:
        self.accelerometer_filename = accelerometer_filename
        self.gps_filename = gps_filename
        self.parking_filename = parking_filename
        self.accelerometer_data = []
        self.gps_data = []
        self.parking_data = []
        self.current_index = 0

    def read(self, batch_size=1) -> list[AggregatedData]:
        batch_data = []
        for _ in range(int(batch_size)):
            accelerometer_index = self.current_index % len(self.accelerometer_data)
            accelerometer_row = self.accelerometer_data[accelerometer_index]
            accelerometer_data = Accelerometer(x=float(accelerometer_row[0]),
                                               y=float(accelerometer_row[1]),
                                               z=float(accelerometer_row[2]))

            gps_index = self.current_index % len(self.gps_data)
            gps_row = self.gps_data[gps_index]
            gps_data = Gps(longitude=float(gps_row[0]), latitude=float(gps_row[1]))

            self.current_index += 1

            batch_data.append(AggregatedData(accelerometer=accelerometer_data, gps=gps_data, time=datetime.now()))

        return batch_data

    def read_parking(self, batch_size=1) -> list[Parking]:
        batch_data = []
        for _ in range(int(batch_size)):
            parking_index = self.current_index % len(self.parking_data)
            parking_row = self.parking_data[parking_index]
            gps_data = Gps(longitude=float(parking_row[1]), latitude=float(parking_row[2]))
            batch_data.append(Parking(empty_count=parking_row[0], gps=gps_data))

        return batch_data

    def startReading(self, *args, **kwargs):
        with open(self.accelerometer_filename, 'r') as file:
            csv_reader = reader(file)
            next(csv_reader)
            self.accelerometer_data = [row for row in csv_reader]

        with open(self.gps_filename, 'r') as file:
            csv_reader = reader(file)
            next(csv_reader)
            self.gps_data = [row for row in csv_reader]

        with open(self.parking_filename, 'r') as file:
            csv_reader = reader(file)
            next(csv_reader)
            self.parking_data = [row for row in csv_reader]

    def stopReading(self, *args, **kwargs):
        pass
