---
layout: default
title: Configuration
nav_order: 3
---

# Configuration
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Connection Properties

When adding a Pinot database in Metabase, you can configure the following properties:

| Property | Required | Description |
|:---------|:---------|:------------|
| Controller endpoint | Yes | URL of your Pinot controller (e.g., `http://localhost:9000`) |
| Database name | No | Database name for multi-tenant setups |
| Authentication | No | Enable Basic or Bearer token authentication |
| Query options | No | Semicolon-separated query options |

## Example Configuration

```yaml
controller-endpoint: http://localhost:9000
database-name: my_database
auth-enabled: true
auth-token-type: Bearer
auth-token-value: your-token-here
query-options: timeoutMs=10000;useMultistageEngine=false
```

## Authentication

The driver supports two authentication modes:

### Basic Authentication

Use a username and password to authenticate with the Pinot controller.

### Bearer Token Authentication

Use a Bearer token (e.g., a JWT) to authenticate. This is common in production deployments with an identity provider.

## Query Options

Query options are passed directly to Apache Pinot as semicolon-separated key-value pairs. Common options include:

| Option | Description |
|:-------|:------------|
| `timeoutMs` | Query timeout in milliseconds |
| `useMultistageEngine` | Enable/disable the multistage query engine (`true`/`false`) |

Example:

```
timeoutMs=10000;useMultistageEngine=false
```

## Temporal Grouping

Metabase time-grain grouping works with Pinot date/time columns discovered from `dateTimeFieldSpecs`. In the query builder, select a date/time breakout and choose a grain such as day, week, month, quarter, or year. The driver translates that grouping to Pinot `DATETRUNC` SQL and uses the Metabase report timezone when one is configured.

Example Pinot schema field:

```json
{
  "dateTimeFieldSpecs": [
    {
      "name": "DaysSinceEpoch",
      "dataType": "INT",
      "format": "1:DAYS:EPOCH",
      "granularity": "1:DAYS"
    }
  ]
}
```

Example SQL shape generated for a daily breakout:

```sql
SELECT
  DATETRUNC('day', "DaysSinceEpoch", 'DAYS', 'UTC', 'MILLISECONDS') AS "DaysSinceEpoch__day",
  COUNT(*) AS "flights"
FROM airlineStats
GROUP BY DATETRUNC('day', "DaysSinceEpoch", 'DAYS', 'UTC', 'MILLISECONDS')
ORDER BY DATETRUNC('day', "DaysSinceEpoch", 'DAYS', 'UTC', 'MILLISECONDS') ASC
LIMIT 5
```

## SSH Tunnel Support

The driver supports SSH tunneling for secure connections to Pinot instances that are not directly accessible. Configure SSH tunnel settings in the Metabase database connection settings under the **SSH tunnel** section.

## SQL Parameters

The driver fully supports [Metabase SQL variable substitution](https://www.metabase.com/docs/latest/questions/native-editor/sql-parameters), including:

| Variable Type | Supported Widgets |
|:-------------|:-----------------|
| Number | Dropdown list, Search box, Input box |
| Text | Input box, Search box, Dropdown list |
| Date | Input box |
| Field Filter | `=`, `!=`, `IN`, `NOT IN`, `LIKE`, `NOT LIKE`, starts with, ends with, contains, does not contain |

### Field Filter Example

```sql
SELECT count(*)
FROM products
WHERE 1=1 [[AND {{category}}]]
```

{: .important }
For field filters, do **not** include the column name in the SQL. Instead, map the variable to a field using the side panel in Metabase's native query editor.
