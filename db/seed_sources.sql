-- Seed: Statistics Canada as primary data source
INSERT INTO sources (name, api_base_url, api_type, licence_url, config_json)
VALUES (
    'Statistics Canada',
    'https://www150.statcan.gc.ca/t1/wds/rest',
    'statcan_wds',
    'https://open.canada.ca/en/open-government-licence-canada',
    '{"rate_limit_per_second": 25, "data_release_time": "08:30", "timezone": "America/Toronto"}'::jsonb
)
ON CONFLICT DO NOTHING;
