CREATE TABLE agreements(
  `correlation_id` BIGINT,
  `schedule_ids` STRING,
  PRIMARY KEY (correlation_id) NOT ENFORCED
) WITH (
  'topic' = 'aggregated-agreements',
  'connector' = 'upsert-kafka',
  'properties.bootstrap.servers' = 'broker:29092',
  'key.format' = 'json',
  'value.format' = 'json',
  'properties.group.id' = 'agreementsTest',
  'properties.auto.offset.reset' = 'earliest'
);
