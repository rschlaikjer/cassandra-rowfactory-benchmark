import random
import sys
import time
import uuid

from cassandra.cluster import Cluster
from cassandra.query import (
    dict_factory,
    named_tuple_factory,
    ordered_dict_factory,
    tuple_factory,
)

# The keyspace and table name to use for testing
TEST_KEYSPACE = 'row_factory_test'
TEST_TABLE = 'sample_data'

# How much data to load into the test table
TEST_DATA_SIZE = 10000

# How many times to run a random query with a given row factory
BENCHMARK_ITERATIONS = 5

# How many IDs to query each benchmark iteration
BENCHMARK_SIZE = 1000

# Cassandra host list
CASSANDRA_HOSTS = frozenset(['127.0.0.1', '127.0.0.2', '127.0.0.3'])

# Keyspace configuration
KEYSPACE_SETTINGS = "{'class':'SimpleStrategy', 'replication_factor':1}"

# List of row factories to benchmark
ROW_FACTORIES_TO_TEST = [
    tuple_factory,
    named_tuple_factory,
    dict_factory,
    ordered_dict_factory
]

def create_cassandra_client():
    cluster = Cluster(CASSANDRA_HOSTS)
    return cluster.connect()


def prepare_db_environment(session):
    # Ensure we have a test keyspace
    session.execute(
        'CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = %s' % (
            TEST_KEYSPACE,
            KEYSPACE_SETTINGS
        )
    )

    # Set the keyspace as the default
    session.set_keyspace(TEST_KEYSPACE)

    # Ensure our test table exists
    session.execute(
        '''
        CREATE TABLE IF NOT EXISTS %s (
            id uuid,
            col1 text,
            col2 text,
            col3 text,
            col4 text,
            col5 text,
            col6 text,
            col7 text,
            col8 text,
            col9 text,
            PRIMARY KEY (id)
        )
        ''' % TEST_TABLE
    )


def insert_test_data(session):
    # Clear any existing data
    session.execute('TRUNCATE %s' % TEST_TABLE)

    # Insert a known quantity of test data
    row_uuids = []
    insert_cmd = '''
        INSERT INTO %s (
            id, col1, col2, col3, col4, col5, col6, col7, col8, col9
        ) VALUES (
            %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s
        )
    ''' % TEST_TABLE
    for i in range(0, TEST_DATA_SIZE):
        sys.stderr.write(
            "Load test data (%d/%d)...\r" % (i, TEST_DATA_SIZE)
        )
        row_uuid = uuid.uuid4()
        row_uuids.append(row_uuid)
        params = (
            row_uuid,
            uuid.uuid4().hex, uuid.uuid4().hex, uuid.uuid4().hex,
            uuid.uuid4().hex, uuid.uuid4().hex, uuid.uuid4().hex,
            uuid.uuid4().hex, uuid.uuid4().hex, uuid.uuid4().hex,
        )
        session.execute(
            insert_cmd, params,
        )
    sys.stderr.write("Loaded %d rows of test data.\n" % TEST_DATA_SIZE)

    return row_uuids


def run_benchmark(session, row_ids, row_factory):
    """
    For the given iteration count select a sample of row IDs and fetch
    them, one after another, from the database.
    Print out a summary of the times.
    """
    sys.stderr.write(''.join(['-' for _ in range(80)]))
    sys.stderr.write('\nBeginning test for row factory %s\n' % row_factory)
    session.row_factory = row_factory

    benchmark_runtimes = []
    for iteration in range(BENCHMARK_ITERATIONS):
        sys.stderr.write('Running benchmark iteration %d... ' % iteration)

        # Select BENCHMARK_SIZE random row IDs to query against
        test_ids = [
            row_ids[i] for i in random.sample(
                range(0, len(row_ids)), BENCHMARK_SIZE
            )
        ]

        # Run a select against the database for each of the IDs
        start_time = time.time()
        for row_uuid in test_ids:
            _rows = session.execute(
                'SELECT * FROM %s WHERE id = %%s' % TEST_TABLE,
                (row_uuid,)
            )
        end_time = time.time()

        # Append the test result
        delta = end_time - start_time
        sys.stderr.write('%f seconds\n' % delta)
        benchmark_runtimes.append(delta)

    qps = [
        BENCHMARK_SIZE / delta
        for delta in benchmark_runtimes
    ]
    average_runtime = sum(benchmark_runtimes) / BENCHMARK_ITERATIONS
    average_qps = sum(qps) / BENCHMARK_ITERATIONS
    sys.stderr.write('Benchmark complete.\n')
    sys.stderr.write('Runtime avg: %f seconds (stddev: %f)\n' % (
        average_runtime, stddev(benchmark_runtimes)
    ))
    sys.stderr.write('QPS avg: %f seconds (stddev: %f)\n' % (
        average_qps, stddev(qps)
    ))


def perform_warmup(session, row_uuids):
    sys.stderr.write("Warming cassandra up a bit first...")
    for _ in range(3):
        for row_uuid in row_uuids:
            _rows = session.execute(
                'SELECT * FROM %s WHERE id = %%s' % TEST_TABLE,
                (row_uuid,)
            )
    sys.stderr.write("done\n")


def stddev(series):
    def mean(data):
        return sum(data) / len(data)

    def sum_of_squares(data):
        m = mean(data)
        return sum((d - m)**2 for d in data)

    return (sum_of_squares(series) / len(series)) * 0.5


def main():
    # Connect to the cassandra cluster
    session = create_cassandra_client()

    # Ensure the DB exists
    prepare_db_environment(session)

    # Load in a test data set, and keep the keys handy
    row_uuids = insert_test_data(session)

    # Warm up by reading all the IDs a handful of times so
    # that the first benchmark isn't disadvantaged.
    perform_warmup(session, row_uuids)

    # Run a benchmark for each of the row factories under test
    for row_factory in ROW_FACTORIES_TO_TEST:
        run_benchmark(session, row_uuids, row_factory)


if __name__ == '__main__':
    main()
