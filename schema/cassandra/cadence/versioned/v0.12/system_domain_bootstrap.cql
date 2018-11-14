INSERT INTO domains_by_name (
    name,
    domain,
    config
) VALUES (
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
    }
) IF NOT EXISTS;

INSERT INTO domains (
    id,
    domain
) VALUES (
    32049b68-7872-4094-8e63-d0dd59896a83,
    {
        name: 'cadence-system'
    }
) IF NOT EXISTS;

