INSERT INTO domains_by_name_v2 (
    domains_partition,
    name,
    domain,
    config,
    is_global_domain,
    config_version,
    failover_version,
    failover_notification_version,
    notification_version
) VALUES (
    0,
    'cadence-system',
    {
        id: 32049b68-7872-4094-8e63-d0dd59896a83,
        name: 'cadence-system',
        description: 'cadence system workflow domain',
        owner_email: 'cadence-dev-group@uber.com'
    },
    {
        retention:3,
        emit_metric:False
    },
    False,
    0,
    -24,
    -1,
    -1
) IF NOT EXISTS;