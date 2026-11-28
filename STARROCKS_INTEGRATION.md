# StarRocks Integration for Temporal

This document describes the integration of StarRocks as a persistence layer for the Temporal workflow engine.

## Overview

StarRocks is a MySQL-wire-protocol-compatible OLAP database that has been integrated as a new persistence option for Temporal. This integration leverages StarRocks' MySQL compatibility to provide a high-performance analytical database backend for Temporal.

## Architecture

The StarRocks integration follows the same pattern as other SQL-based persistence layers in Temporal:

- **Plugin System**: Implements the `sqlplugin.Plugin` interface
- **Database Interface**: Provides CRUD operations for all Temporal data types
- **Session Management**: Handles connections using MySQL-compatible drivers
- **Schema Management**: Includes database schemas for both main and visibility stores

## Files Added/Modified

### Core Plugin Files
- `common/persistence/sql/sqlplugin/starrocks/plugin.go` - Plugin registration and initialization
- `common/persistence/sql/sqlplugin/starrocks/db.go` - Database interface implementation
- `common/persistence/sql/sqlplugin/starrocks/session/session.go` - Connection management
- `common/persistence/sql/sqlplugin/starrocks/typeconv.go` - Data type conversions
- All CRUD operation files (execution.go, shard.go, etc.)

### Schema Files
- `schema/starrocks/v1/version.go` - Version definitions
- `schema/starrocks/v1/temporal/` - Main database schema
- `schema/starrocks/v1/visibility/` - Visibility database schema

### Configuration
- `config/development-starrocks.yaml` - Example configuration for StarRocks

## Configuration

To use StarRocks as the persistence layer, configure Temporal with:

```yaml
persistence:
  defaultStore: starrocks-default
  visibilityStore: starrocks-visibility
  datastores:
    starrocks-default:
      sql:
        pluginName: "starrocks"
        databaseName: "temporal"
        connectAddr: "localhost:9030"
        user: "root"
        password: ""
        # ... other connection settings
```

## Key Features

1. **MySQL Compatibility**: Uses the MySQL wire protocol for seamless integration
2. **Full CRUD Support**: Implements all required operations for Temporal data types
3. **Transaction Support**: Leverages StarRocks' transaction capabilities
4. **Schema Management**: Includes versioned schema migrations
5. **Connection Pooling**: Efficient connection management with configurable pools

## Considerations

### Performance
- StarRocks is optimized for OLAP workloads, which may differ from Temporal's OLTP requirements
- Consider performance testing under your specific workload patterns
- Monitor query performance and optimize indexes as needed

### Data Types
- The integration uses MySQL-compatible data types
- Some StarRocks-specific optimizations may be possible for analytical queries
- Binary data is handled using VARBINARY/BLOB types

### Limitations
- StarRocks' OLAP focus may not provide the same transactional guarantees as traditional OLTP databases
- Consider the implications for workflow consistency and durability
- Test thoroughly under failure scenarios

## Testing

To test the integration:

1. Start a StarRocks instance
2. Create the required databases:
   ```sql
   CREATE DATABASE temporal;
   CREATE DATABASE temporal_visibility;
   ```
3. Run Temporal with the StarRocks configuration
4. Execute workflow operations and verify data persistence

## Future Enhancements

Potential areas for improvement:

1. **StarRocks-Specific Optimizations**: Leverage StarRocks' columnar storage and indexing
2. **Analytical Queries**: Add support for complex analytical queries on workflow data
3. **Performance Tuning**: Optimize for StarRocks' specific performance characteristics
4. **Monitoring**: Add StarRocks-specific metrics and monitoring

## Support

This integration is experimental and may require additional testing and optimization for production use. Consider:

- Engaging with the StarRocks community for best practices
- Monitoring performance metrics closely
- Having fallback options available
- Contributing improvements back to the Temporal community

## Dependencies

The integration requires:
- StarRocks database instance
- MySQL-compatible Go driver (already included in Temporal)
- Proper network connectivity between Temporal and StarRocks

## Version Compatibility

- Tested with StarRocks versions supporting MySQL wire protocol
- Compatible with Temporal's current persistence interface
- Schema version: 1.0 (initial implementation)
