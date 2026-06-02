#!/bin/bash

# Set the Pinot version
if [ -z "${PINOT_VERSION}" ]; then
  echo "PINOT_VERSION is not set. Using default version 1.3.0"
  PINOT_VERSION="1.3.0"
fi

# Set the download URL
DOWNLOAD_URL="https://archive.apache.org/dist/pinot/apache-pinot-${PINOT_VERSION}/apache-pinot-${PINOT_VERSION}-bin.tar.gz"

# Set the destination directory
if [ -z "${PINOT_HOME}" ]; then
  echo "PINOT_HOME is not set. Using default directory /tmp/pinot"
  PINOT_HOME="/tmp/pinot"
fi

# Set the broker port
if [ -z "${BROKER_PORT_FORWARD}" ]; then
  echo "BROKER_PORT_FORWARD is not set. Using default port 8000"
  BROKER_PORT_FORWARD="8000"
fi

# Create the destination directory
mkdir -p "${PINOT_HOME}"

# Check if the directory exists
if [ -d "${PINOT_HOME}/apache-pinot-${PINOT_VERSION}-bin" ]; then
    echo "Pinot package already exists in ${PINOT_HOME}/apache-pinot-${PINOT_VERSION}-bin"
else
    # Download the Pinot package
    curl  --parallel -L "${DOWNLOAD_URL}" -o "${PINOT_HOME}/apache-pinot-${PINOT_VERSION}-bin.tar.gz"

    # Extract the downloaded package
    tar -xzf "${PINOT_HOME}/apache-pinot-${PINOT_VERSION}-bin.tar.gz" -C "${PINOT_HOME}"

    # Remove the downloaded package
    rm "${PINOT_HOME}/apache-pinot-${PINOT_VERSION}-bin.tar.gz"
fi


# Start the Pinot cluster
${PINOT_HOME}/apache-pinot-${PINOT_VERSION}-bin/bin/pinot-admin.sh QuickStart -type MULTI_STAGE &
PID=$!

# Print the JVM settings
jps -lvm

run_sql() {
  local sql="$1"
  local payload

  payload=$(jq -n --arg sql "${sql}" '{sql: $sql}')
  curl -s -X POST \
    -H 'Accept: application/json' \
    -H 'Content-Type: application/json' \
    -d "${payload}" \
    "http://localhost:${BROKER_PORT_FORWARD}/query/sql"
}

### ---------------------------------------------------------------------------
### Ensure Pinot cluster started correctly.
### ---------------------------------------------------------------------------

echo "Ensure Pinot cluster started correctly"

# Wait at most 10 minutes to reach the desired state
for i in $(seq 1 150)
do
  SUCCEED_TABLE=0
  for table in "airlineStats" "baseballStats" "dimBaseballTeams" "githubComplexTypeEvents" "githubEvents" "starbucksStores";
  do
    QUERY="select count(*) from ${table} limit 1"
    echo "Running SQL: ${QUERY}"
    QUERY_RES=$(run_sql "${QUERY}")
    QUERY_STATUS=$?
    echo "${QUERY_RES}"

    if [ "${QUERY_STATUS}" -eq 0 ]; then
      COUNT_STAR_RES=$(echo "${QUERY_RES}" | jq -r '.resultTable.rows[0][0] // 0')
      if [[ "${COUNT_STAR_RES}" =~ ^[0-9]+$ ]] && [ "${COUNT_STAR_RES}" -gt 0 ]; then
        SUCCEED_TABLE=$((SUCCEED_TABLE+1))
      fi
    fi
    echo "QUERY: ${QUERY}, QUERY_RES: ${QUERY_RES}"
  done
  echo "SUCCEED_TABLE: ${SUCCEED_TABLE}"
  if [ "${SUCCEED_TABLE}" -eq 6 ]; then
    break
  fi
  sleep 4
done

if [ "${SUCCEED_TABLE}" -lt 6 ]; then
  echo 'Quickstart failed: Cannot confirmed count-star result from quickstart table in 10 minutes'
  exit 1
fi

### ---------------------------------------------------------------------------
### Validate Metabase temporal grouping query shape.
### ---------------------------------------------------------------------------

TEMPORAL_DAY_EXPRESSION="DATETRUNC('day', \"DaysSinceEpoch\", 'DAYS', 'UTC', 'MILLISECONDS')"
TEMPORAL_QUERY="SELECT ${TEMPORAL_DAY_EXPRESSION} AS \"DaysSinceEpoch__day\", COUNT(*) AS \"flights\" FROM airlineStats GROUP BY ${TEMPORAL_DAY_EXPRESSION} ORDER BY ${TEMPORAL_DAY_EXPRESSION} ASC LIMIT 5"

echo "Validating temporal bucket query used by Metabase time-grain grouping"
echo "Running SQL: ${TEMPORAL_QUERY}"
TEMPORAL_RES=$(run_sql "${TEMPORAL_QUERY}")
TEMPORAL_STATUS=$?
echo "${TEMPORAL_RES}"

if [ "${TEMPORAL_STATUS}" -ne 0 ]; then
  echo "Quickstart failed: temporal bucket query request failed"
  exit 1
fi

TEMPORAL_EXCEPTION_COUNT=$(echo "${TEMPORAL_RES}" | jq -r '(.exceptions // []) | length' 2>/dev/null || echo 1)
TEMPORAL_ROW_COUNT=$(echo "${TEMPORAL_RES}" | jq -r '(.resultTable.rows // []) | length' 2>/dev/null || echo 0)
TEMPORAL_FIRST_BUCKET=$(echo "${TEMPORAL_RES}" | jq -r '.resultTable.rows[0][0] // empty' 2>/dev/null || true)
TEMPORAL_FIRST_COUNT=$(echo "${TEMPORAL_RES}" | jq -r '.resultTable.rows[0][1] // 0' 2>/dev/null || echo 0)

if [ "${TEMPORAL_EXCEPTION_COUNT}" -gt 0 ]; then
  echo "Quickstart failed: temporal bucket query returned Pinot exceptions"
  exit 1
fi

if [ "${TEMPORAL_ROW_COUNT}" -lt 1 ] ||
   ! [[ "${TEMPORAL_FIRST_BUCKET}" =~ ^[0-9]+$ ]] ||
   ! [[ "${TEMPORAL_FIRST_COUNT}" =~ ^[0-9]+$ ]] ||
   [ "${TEMPORAL_FIRST_COUNT}" -lt 1 ]; then
  echo "Quickstart failed: temporal bucket query did not return valid bucketed rows"
  exit 1
fi

echo "Temporal bucket query validated correctly"
echo "Pinot cluster started correctly"
