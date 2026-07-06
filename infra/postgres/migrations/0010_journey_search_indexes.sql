CREATE INDEX IF NOT EXISTS stop_times_origin_routing_idx
  ON stop_times (stop_id, departure_time, trip_id, stop_sequence)
  WHERE COALESCE(pickup_type, 0) = 0;

CREATE INDEX IF NOT EXISTS stop_times_destination_routing_idx
  ON stop_times (stop_id, arrival_time, trip_id, stop_sequence)
  WHERE COALESCE(drop_off_type, 0) = 0;
