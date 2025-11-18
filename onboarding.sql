CREATE TABLE onboarding(
  `correlation_id` BIGINT,
  `total_parts` BIGINT,
  `schedule_id` STRING,
  `ts` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'topic' = 'source-event-stream',
  'connector' = 'kafka',
  'properties.bootstrap.servers' = 'broker:29092',
  'format' = 'json',
  'value.format' = 'json',
  'properties.group.id' = 'onboardingTesting',
  'properties.auto.offset.reset' = 'earliest'
)
