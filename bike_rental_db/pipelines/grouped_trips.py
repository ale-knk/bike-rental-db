from pymongo import MongoClient
from datetime import datetime
from bson import ObjectId
import calendar

class GroupedTripsPipelines:
    def __init__(self):
        self.daily_pipeline = self._create_daily_pipeline(group_by_station=False)
        self.daily_with_station_pipeline = self._create_daily_pipeline(group_by_station=True)
        self.monthly_pipeline = self._create_monthly_pipeline(group_by_station=False)
        self.monthly_with_station_pipeline = self._create_monthly_pipeline(group_by_station=True)
        self.convert_station_id_pipeline = self._create_convert_station_id_pipeline()

    def _create_add_fields_day_stage(self):
        return {
            "$addFields": {
                "date_name": {
                    "$switch": {
                        "branches": [
                            {"case": {"$eq": [{"$dayOfWeek": "$date"}, 1]}, "then": "Sunday"},
                            {"case": {"$eq": [{"$dayOfWeek": "$date"}, 2]}, "then": "Monday"},
                            {"case": {"$eq": [{"$dayOfWeek": "$date"}, 3]}, "then": "Tuesday"},
                            {"case": {"$eq": [{"$dayOfWeek": "$date"}, 4]}, "then": "Wednesday"},
                            {"case": {"$eq": [{"$dayOfWeek": "$date"}, 5]}, "then": "Thursday"},
                            {"case": {"$eq": [{"$dayOfWeek": "$date"}, 6]}, "then": "Friday"},
                            {"case": {"$eq": [{"$dayOfWeek": "$date"}, 7]}, "then": "Saturday"}
                        ],
                        "default": "Unknown"
                    }
                }
            }
        }

    def _create_add_fields_month_stage(self):
        return {
            "$addFields": {
                "date_name": {
                    "$switch": {
                        "branches": [
                            {"case": {"$eq": [{"$month": "$date"}, 1]}, "then": "January"},
                            {"case": {"$eq": [{"$month": "$date"}, 2]}, "then": "February"},
                            {"case": {"$eq": [{"$month": "$date"}, 3]}, "then": "March"},
                            {"case": {"$eq": [{"$month": "$date"}, 4]}, "then": "April"},
                            {"case": {"$eq": [{"$month": "$date"}, 5]}, "then": "May"},
                            {"case": {"$eq": [{"$month": "$date"}, 6]}, "then": "June"},
                            {"case": {"$eq": [{"$month": "$date"}, 7]}, "then": "July"},
                            {"case": {"$eq": [{"$month": "$date"}, 8]}, "then": "August"},
                            {"case": {"$eq": [{"$month": "$date"}, 9]}, "then": "September"},
                            {"case": {"$eq": [{"$month": "$date"}, 10]}, "then": "October"},
                            {"case": {"$eq": [{"$month": "$date"}, 11]}, "then": "November"},
                            {"case": {"$eq": [{"$month": "$date"}, 12]}, "then": "December"}
                        ],
                        "default": "Unknown"
                    }
                }
            }
        }

    def _create_group_stage_day(self, group_by_station):
        group_fields = {
            "_id": {
                "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$date"}},
                "date_name": "$date_name"
            },
            "n_trips_start": {"$sum": "$n_trips_start"},
            "n_trips_end": {"$sum": "$n_trips_end"}
        }
        if group_by_station:
            group_fields["_id"]["station"] = "$station"
        return {"$group": group_fields}

    def _create_group_stage_month(self, group_by_station):
        group_fields = {
            "_id": {
                "year": {"$year": "$date"},
                "month": {"$month": "$date"},
                "date_name": "$date_name"
            },
            "n_trips_start": {"$sum": "$n_trips_start"},
            "n_trips_end": {"$sum": "$n_trips_end"}
        }
        if group_by_station:
            group_fields["_id"]["station"] = "$station"
        return {"$group": group_fields}

    def _create_project_stage(self, group_by_station, freq):
        project_fields = {
            "_id": 0,
            "freq": 1,
            "n_trips_start": 1,
            "n_trips_end": 1,
            "date": {"$dateFromParts": {
                "year": "$_id.year", 
                "month": "$_id.month"
            }} if freq == "monthly" else "$_id.date",
            "date_name": "$_id.date_name"
        }
        if group_by_station:
            project_fields["station"] = "$_id.station"


        return {"$project": project_fields}

    def _create_daily_pipeline(self, group_by_station):
        return [
            self._create_add_fields_day_stage(),
            self._create_group_stage_day(group_by_station),
            {
                "$addFields": {
                    "freq": "daily"
                }
            },
            self._create_project_stage(group_by_station, "daily")
        ]

    def _create_monthly_pipeline(self, group_by_station):
        return [
            self._create_add_fields_month_stage(),
            self._create_group_stage_month(group_by_station),
            {
                "$addFields": {
                    "freq": "monthly"
                }
            },
            self._create_project_stage(group_by_station, "monthly")
        ]

    def _create_convert_station_id_pipeline(self):
        return [
            {
                "$lookup": {
                    "from": "stations",
                    "localField": "station",
                    "foreignField": "_id",
                    "as": "station"
                }
            },
            {
                "$unwind": {
                    "path": "$station",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$project": {
                    "station": "$station",
                    "date": 1,
                    "n_trips_start": 1,
                    "n_trips_end": 1,
                }
            }
        ]