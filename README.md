# Cassandra Row Factory Benchmark

This benchmark code is companion to my
[blog post](https://rhye.org/post/python-cassandra-namedtuple-performance/)
regarding the relative speed of cassandra's row factories.

## To Run

    # Create a virtualenv
    virtualenv venv
    . venv/bin/activate

    # Install the requirements
    pip install -r requirements.txt

    # You may wish to tweak CASSANDRA_HOSTS, BENCHMARK_SIZE or
    # BENCHMARK_ITERATIONS before continuing

    # Run the benchmark
    python main.py

    # Sample output
    Loaded 10000 rows of test data.
    Warming cassandra up a bit first...done
    --------------------------------------------------------------------------------
    Beginning test for row factory <cyfunction tuple_factory at 0x7ff9b6e44a10>
    Running benchmark iteration 0... 0.902121 seconds
    Running benchmark iteration 1... 0.860280 seconds
    Running benchmark iteration 2... 0.878435 seconds
    Running benchmark iteration 3... 0.917914 seconds
    Running benchmark iteration 4... 0.862856 seconds
    Benchmark complete.
    Runtime avg: 0.884321 seconds (stddev: 0.000252)
    QPS avg: 1131.533536 seconds (stddev: 405.529567)
    --------------------------------------------------------------------------------
    Beginning test for row factory <cyfunction named_tuple_factory at 0x7ff9b6e44ad0>
    Running benchmark iteration 0... 1.466348 seconds
    Running benchmark iteration 1... 1.480543 seconds
    Running benchmark iteration 2... 1.499516 seconds
    Running benchmark iteration 3... 1.472076 seconds
    Running benchmark iteration 4... 1.484504 seconds
    Benchmark complete.
    Runtime avg: 1.480597 seconds (stddev: 0.000065)
    QPS avg: 675.442898 seconds (stddev: 13.412463)
    --------------------------------------------------------------------------------
    Beginning test for row factory <cyfunction dict_factory at 0x7ff9b6e44b90>
    Running benchmark iteration 0... 0.863990 seconds
    Running benchmark iteration 1... 0.861385 seconds
    Running benchmark iteration 2... 0.879204 seconds
    Running benchmark iteration 3... 0.892426 seconds
    Running benchmark iteration 4... 0.883563 seconds
    Benchmark complete.
    Runtime avg: 0.876114 seconds (stddev: 0.000070)
    QPS avg: 1141.611256 seconds (stddev: 118.118469)
    --------------------------------------------------------------------------------
    Beginning test for row factory <cyfunction ordered_dict_factory at 0x7ff9b6e44c50>
    Running benchmark iteration 0... 0.948919 seconds
    Running benchmark iteration 1... 0.941756 seconds
    Running benchmark iteration 2... 0.933791 seconds
    Running benchmark iteration 3... 0.958218 seconds
    Running benchmark iteration 4... 0.944122 seconds
    Benchmark complete.
    Runtime avg: 0.945361 seconds (stddev: 0.000033)
    QPS avg: 1057.873886 seconds (stddev: 40.724691)
