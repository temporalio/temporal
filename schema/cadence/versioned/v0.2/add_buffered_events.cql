CREATE TYPE serialized_event_batch (
  encoding_type text,
  version       int,
  data          blob,
);

ALTER TABLE executions ADD buffered_events_list list<frozen<serialized_event_batch>>;
