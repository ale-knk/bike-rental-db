class TripsPipelines:
    def __init__(self):
        self.group_trips_pipeline = [
            {
                "$facet": {
                    "trips_start": [
                        {
                            "$group": {
                                "_id": {
                                    "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$start_date"}},
                                    "station": "$start_station"
                                },
                                "n_trips_start": {"$sum": 1}
                            }
                        }
                    ],
                    
                    "trips_end": [
                        {
                            "$group": {
                                "_id": {
                                    "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$end_date"}},
                                    "station": "$end_station"
                                },
                                "n_trips_end": {"$sum": 1}
                            }
                        }
                    ]
                }
            },

            {
                "$project": {
                    "combined": {
                        "$concatArrays": ["$trips_start", "$trips_end"]
                    }
                }
            },

            {
                "$unwind": "$combined"
            },

            {
                "$group": {
                    "_id": {
                        "date": "$combined._id.date",
                        "station": "$combined._id.station"
                    },
                    "n_trips_start": {
                        "$sum": {
                            "$cond": [{"$ifNull": ["$combined.n_trips_start", False]}, "$combined.n_trips_start", 0]
                        }
                    },
                    "n_trips_end": {
                        "$sum": {
                            "$cond": [{"$ifNull": ["$combined.n_trips_end", False]}, "$combined.n_trips_end", 0]
                        }
                    }
                }
            },

            {
                "$project": {
                    "_id": 0,
                    "date": "$_id.date",
                    "station": "$_id.station",
                    "n_trips_start": 1,
                    "n_trips_end": 1
                }
            }
        ]
        # self.group_trips_pipeline = self._create_group_trips_pipeline()
        self.replace_station_ids_pipeline = self._create_replace_station_ids_pipeline()

    def _create_add_fields_stage(self, trip_type, date_field):
        return {
            "$addFields": {
                "trip_type": trip_type,
                "date": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": f"${date_field}"
                    }
                }
            }
        }

    def _create_group_stage(self, station_field):
        return {
            "$group": {
                "_id": {
                    "date": "$date",
                    "station": f"${station_field}",
                    "trip_type": "$trip_type"
                },
                "n_trips": {"$sum": 1},
            }
        }
    
    def _create_project_stage(self):
        return {
            "$project": {
                "_id": 0,
                "date": "$_id.date",
                "station": "$_id.station",
                "n_trips": 1,
                "trip_type": "$_id.trip_type",
            }
        }

    def _create_facet_stage(self):
        start_trips = [
            self._create_add_fields_stage("start", "start_date"),
            self._create_group_stage("start_station"),
            self._create_project_stage()
        ]

        end_trips = [
            self._create_add_fields_stage("end", "end_date"),
            self._create_group_stage("end_station"),
            self._create_project_stage()
        ]

        return {
            "$facet": {
                "start_trips": start_trips,
                "end_trips": end_trips
            }
        }

    def _create_group_trips_pipeline(self):
        return [
            self._create_facet_stage(),
            {
                "$project": {
                    "combined": {
                        "$concatArrays": ["$start_trips", "$end_trips"]
                    }
                }
            },
            {
                "$unwind": "$combined"
            },
            {
                "$replaceRoot": {
                    "newRoot": "$combined"
                }
            },
            {
                "$addFields": {
                    "freq": "daily"
                }
            }
        ]

    def _create_lookup_stage(self, local_field, from_collection, as_field):
        return {
            "$lookup": {
                "from": from_collection,
                "localField": local_field,
                "foreignField": "_id",
                "as": as_field
            }
        }

    def _create_unwind_stage(self, path, preserve_null=False):
        return {
            "$unwind": {
                "path": path,
                "preserveNullAndEmptyArrays": preserve_null
            }
        }

    def _create_replace_station_ids_pipeline(self):
        return [
            self._create_lookup_stage("start_station", "stations", "start_station"),
            self._create_unwind_stage("$start_station", True),
            self._create_lookup_stage("end_station", "stations", "end_station"),
            self._create_unwind_stage("$end_station", True),
            {
                "$project": {
                    "start_station": 1,
                    "end_station": 1,
                    "start_date": 1,
                    "end_date": 1,
                    "duration": 1,
                    "bike_id": 1,
                    "subscription_type": 1,
                    "zip_code": 1,
                    "split": 1
                }
            }
        ]

