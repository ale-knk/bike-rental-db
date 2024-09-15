class StationStatusPipelines:

    def __init__(self):
        self.daily_status_pipeline = self.get_groupstatus_pipeline(freq="daily")
        self.daily_status_stations_pipeline = self.get_groupstatus_pipeline(freq="daily", by_station=True)
        self.monthly_status_pipeline = self.get_groupstatus_pipeline(freq="monthly")
        self.monthly_status_stations_pipeline = self.get_groupstatus_pipeline(freq="monthly", by_station=True)

    @staticmethod
    def add_date_freq(date_field: str = "time", freq: str = "daily"):
        return [{
            "$addFields": {
                "date": {
                    "$dateToString": {
                        "format": "%Y-%m-%d" if freq == "daily" else "%Y-%m",
                        "date": f"${date_field}"
                    }
                }
            }
        }]

    @staticmethod
    def group_by(freq: str = "daily", by_station: bool = False):
        fields = {"date": "$date"}
        if by_station:
            fields["station"] = "$station_mongoid"

        return [
            {
                "$group": {
                    "_id": fields,
                    "docks_available": {"$push": "$docks_available"},
                    "bikes_available": {"$push": "$bikes_available"},
                }
            }
        ]

    @staticmethod
    def project_fields(by_station: bool = False):
        step = {
            "$project": {
                "_id": 0,
                "date": "$_id.date",
                "docks_available": 1,
                "bikes_available": 1,
            }
        }
        if by_station:
            step["$project"]["station"] = "$_id.station"

        return [step]

    @staticmethod
    def sort_by(freq: str = "daily", by_station: bool = False):
        sort_fields = {"date": 1}
        if by_station:
            sort_fields["station"] = 1
        return [{"$sort": sort_fields}]

    def get_groupstatus_pipeline(self, freq: str = "daily", by_station: bool = False):
        steps = []
        steps += self.add_date_freq(freq=freq)
        steps += self.group_by(freq=freq, by_station=by_station)
        steps += self.project_fields(by_station=by_station)
        steps += self.sort_by(freq=freq, by_station=by_station)
        return steps
