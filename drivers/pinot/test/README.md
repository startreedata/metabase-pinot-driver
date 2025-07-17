# Pinot Driver Testing Guide

This document provides comprehensive information about testing the Metabase Pinot driver, including test coverage, performance testing, and integration testing.

## Test Structure

The test suite is organized into several modules:

```
test/
├── metabase/driver/
│   ├── pinot_test.clj           # Main driver integration tests
│   └── pinot/
│       ├── client_test.clj      # HTTP client tests
│       ├── execute_test.clj     # Query execution tests  
│       ├── query_processor_test.clj # MBQL to SQL translation tests
│       ├── sync_test.clj        # Database synchronization tests
│       └── fixtures.clj         # Test fixtures and mock data
├── tests.edn                    # Kaocha test configuration
└── README.md                    # This file
```

## Running Tests

### Prerequisites

1. **Java 21+** - Required for Clojure and Metabase
2. **Clojure CLI** - Version 1.12.1.1550+
3. **Apache Pinot** (optional) - For integration tests
4. **Make** - For using the provided Makefile

### Quick Start

```bash
# Run all tests
make test

# Run tests with coverage
cd drivers/pinot
clojure -X:test-coverage

# Run specific test suites using Kaocha
clojure -M:test-runner --config-file tests.edn
```

### Test Commands

#### Basic Test Execution

```bash
# Run all unit tests
clojure -M:test

# Run tests with better reporting
clojure -M:test-runner

# Run only unit tests (skip integration/slow tests)  
clojure -M:test-runner --focus :unit

# Run only integration tests
clojure -M:test-runner --focus :integration
```

#### Coverage Reporting

```bash
# Generate HTML coverage report
clojure -X:test-coverage

# Generate coverage with command-line interface
clojure -M:coverage

# View coverage report
open target/coverage/index.html
```

#### Performance Testing

```bash
# Run performance benchmarks
clojure -M:benchmark -e "(require '[metabase.driver.pinot.performance-test])"

# Profile test execution
clojure -M:test-runner --plugin kaocha.plugin/profiling
```

## Test Coverage

The test suite provides comprehensive coverage across all driver modules:

### Coverage Metrics

- **Target Coverage**: 80%+ overall
- **Minimum Coverage**: 70% (CI fails below this)
- **Low Watermark**: 50% (warnings)
- **High Watermark**: 80% (good coverage)

### Coverage Areas

| Module | Coverage Focus | Key Test Areas |
|--------|---------------|----------------|
| `client.clj` | HTTP communication | Request handling, authentication, error handling |
| `execute.clj` | Query execution | Result processing, type inference, metadata |
| `sync.clj` | Database sync | Table discovery, schema mapping, version detection |
| `query_processor.clj` | MBQL translation | Filter/aggregation translation, SQL generation |
| `pinot.clj` | Driver integration | Connection testing, feature support, timeouts |

### Coverage Reports

Coverage reports are generated in multiple formats:

- **HTML**: `target/coverage/index.html` - Interactive coverage explorer
- **JSON**: `target/coverage/codecov.json` - Machine-readable format
- **JUnit XML**: `target/coverage/junit.xml` - CI integration
- **Console**: Real-time coverage display during test runs

## Test Categories

### Unit Tests

Fast, isolated tests that mock external dependencies:

```bash
# Run only unit tests
clojure -M:test-runner --focus :unit
```

**Characteristics:**
- No external dependencies
- Fast execution (< 5 seconds total)
- Mock HTTP requests to Pinot
- Test individual functions and modules

### Integration Tests  

Tests that verify end-to-end functionality:

```bash
# Run integration tests (requires Pinot server)
clojure -M:test-runner --focus :integration
```

**Characteristics:**
- May require running Pinot server
- Test complete workflows
- Verify driver registration and configuration
- Test real query execution paths

### Performance Tests

Tests focused on performance characteristics:

```bash
# Run performance tests
clojure -M:test-runner --focus :performance
```

**Characteristics:**
- Test with large datasets
- Measure execution time
- Memory usage validation
- Concurrency testing

## Test Fixtures and Mock Data

The `fixtures.clj` file provides comprehensive test data:

### Database Configurations

```clojure
;; Basic connection
test-db-details

;; With authentication  
auth-db-details

;; Multi-tenant setup
multi-tenant-db-details

;; SSH tunnel configuration
ssh-tunnel-db-details
```

### Mock Responses

```clojure
;; Pinot server responses
mock-health-response
mock-version-response
mock-tables-response

;; Query results
simple-query-result
aggregation-query-result
large-query-result

;; Error scenarios
connection-error
unauthorized-error
timeout-error
```

### Test Helper Functions

```clojure
;; Create mock HTTP responses
(mock-http-success body)
(mock-http-error status message)

;; Set up test fixtures
(with-mock-pinot-responses {...})
(with-test-database db-details)

;; Generate test data
(generate-large-dataset 10000 50)
(generate-schema-with-fields 100 20 5)
```

## Writing Tests

### Test Organization

Follow the established patterns:

```clojure
(ns metabase.driver.pinot.module-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [metabase.driver.pinot.module :as module]
   [metabase.driver.pinot.fixtures :as fixtures]))

(deftest feature-test
  (testing "specific functionality"
    (testing "successful case"
      (is (= expected (module/function input))))
    
    (testing "error case" 
      (is (thrown? Exception (module/function invalid-input))))
    
    (testing "edge case"
      (is (= edge-expected (module/function edge-input))))))
```

### Mock External Dependencies

Use `with-redefs` to mock HTTP calls:

```clojure
(testing "API interaction"
  (with-redefs [http/get (fn [url options] 
                           {:status 200 :body "{}"})]
    (is (= expected (api-function)))))
```

### Performance Testing

Use `time` and `criterium` for performance tests:

```clojure
(deftest performance-test
  (testing "large dataset processing"
    (let [large-data (generate-large-dataset 50000 100)]
      (time
        (let [result (process-data large-data)]
          (is (= 50000 (count result))))))))
```

### Testing Async Operations

Handle async operations properly:

```clojure
(deftest async-test
  (testing "cancellation mechanism"
    (let [cancel-chan (async/chan)]
      (async/close! cancel-chan)
      (is (thrown? Exception 
                   (execute-with-cancellation cancel-chan query))))))
```

## Continuous Integration

### GitHub Actions

The CI pipeline runs comprehensive tests:

```yaml
# .github/workflows/ci.yml
- name: Run tests with coverage
  run: |
    cd drivers/pinot  
    clojure -X:test-coverage
    clojure -M:test-runner --config-file tests.edn
```

### Coverage Requirements

- **Minimum**: 70% overall coverage
- **Target**: 80%+ coverage  
- **CI Failure**: Below 70% coverage
- **Coverage Reports**: Uploaded to Codecov

### Test Artifacts

CI uploads test artifacts:
- Coverage reports (HTML, JSON, XML)
- Test results (JUnit XML)
- Performance benchmarks
- Log files

## Debugging Tests

### Test Failures

1. **Check logs**: Test output includes detailed logging
2. **Mock verification**: Ensure mocks match expected calls
3. **Data validation**: Verify test data matches expectations
4. **Async timing**: Check for race conditions

### Performance Issues

1. **Profile tests**: Use `:kaocha.plugin/profiling`
2. **Memory usage**: Monitor with JVM flags
3. **Mock efficiency**: Ensure mocks don't add overhead
4. **Test isolation**: Verify tests don't interfere

### Common Issues

| Issue | Solution |
|-------|----------|
| Test timeout | Increase timeout or optimize test |
| Mock not called | Check URL/parameter matching |
| Memory leak | Ensure proper cleanup in fixtures |
| Flaky test | Add proper synchronization |
| Coverage miss | Add edge case tests |

## Best Practices

### Test Design

1. **Isolation**: Tests should not depend on each other
2. **Deterministic**: Same input should always produce same output  
3. **Fast**: Unit tests should complete quickly
4. **Comprehensive**: Cover happy path, errors, and edge cases
5. **Readable**: Clear test names and structure

### Mock Strategy

1. **Mock external dependencies**: HTTP calls, file system, time
2. **Use realistic data**: Mock responses should match real Pinot responses
3. **Test mock setup**: Verify mocks are called as expected
4. **Fail fast**: Mock failures should be obvious

### Coverage Goals

1. **Line coverage**: 80%+ of code lines executed
2. **Branch coverage**: 80%+ of conditional branches tested
3. **Function coverage**: 95%+ of functions have at least one test
4. **Integration coverage**: All driver methods tested end-to-end

## Troubleshooting

### Test Environment Setup

If tests fail to run:

1. **Check Java version**: `java -version` (should be 21+)
2. **Check Clojure CLI**: `clojure -version` 
3. **Verify dependencies**: `clojure -Stree`
4. **Clean cache**: `rm -rf .cpcache`

### Coverage Issues

If coverage is low:

1. **Identify uncovered code**: Check HTML report
2. **Add missing tests**: Focus on uncovered branches
3. **Remove dead code**: Delete unused functions
4. **Update exclusions**: Add test files to exclusions

### Performance Problems

If tests are slow:

1. **Profile execution**: Use profiling plugin
2. **Optimize mocks**: Use faster mock implementations
3. **Parallel execution**: Run tests concurrently where safe
4. **Skip expensive tests**: Use focus/skip metadata

## Resources

- [Clojure Test Documentation](https://clojure.github.io/clojure/clojure.test-api.html)
- [Kaocha Test Runner](https://cljdoc.org/d/lambdaisland/kaocha)
- [Cloverage Coverage Tool](https://github.com/cloverage/cloverage)
- [Criterium Benchmarking](https://github.com/hugoduncan/criterium)
- [Metabase Driver Development](https://www.metabase.com/docs/latest/developers-guide) 