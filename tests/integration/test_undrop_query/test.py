import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_undrop_MergeTree(started_cluster):
    node1.query("drop table if exists test_25338_undrop sync;")
    node1.query("create table test_25338_undrop (id Int32) Engine=MergeTree() order by id;")
    node1.query("insert into test_25338_undrop values (1),(2),(3);")
    node1.query("drop table test_25338_undrop;")
    assert(
        node1.query("select value from system.metrics where metric = 'TablesToDropQueueSize';") == "1\n"
    )
    node1.query("undrop table test_25338_undrop;")
    assert(
        node1.query("select value from system.metrics where metric = 'TablesToDropQueueSize';") == "0\n"
    )
    assert(
        node1.query("select * from test_25338_undrop order by id;") == "1\n2\n3\n"
    )
    node1.query("drop table test_25338_undrop sync;")

def test_undrop_MergeTree_with_uuid(started_cluster):
    node1.query("create table test_25338_undrop UUID '3719b97a-fc7c-4bb1-84c0-a9906006fb88' (id Int32) Engine=MergeTree() order by id;")
    node1.query("insert into test_25338_undrop values (1),(2),(3);")
    node1.query("drop table test_25338_undrop;")
    assert(
        node1.query("select value from system.metrics where metric = 'TablesToDropQueueSize';") == "1\n"
    )
    node1.query("undrop table test_25338_undrop;")
    assert(
        node1.query("select value from system.metrics where metric = 'TablesToDropQueueSize';") == "0\n"
    )
    assert(
        node1.query("select * from test_25338_undrop order by id;") == "1\n2\n3\n"
    )
    node1.query("drop table test_25338_undrop sync;")

def test_undrop_MergeTree_on_cluster(started_cluster):
    node1.query("create table test_25338_undrop on cluster test_cluster (id Int32) Engine=MergeTree() order by id;")
    node1.query("insert into test_25338_undrop values (1),(2),(3);")
    node1.query("drop table test_25338_undrop on cluster test_cluster;")
    assert(
        node1.query("select value from system.metrics where metric = 'TablesToDropQueueSize';") == "1\n"
    )
    node1.query("undrop table test_25338_undrop on cluster test_cluster;")
    assert(
        node1.query("select value from system.metrics where metric = 'TablesToDropQueueSize';") == "0\n"
    )
    assert(
        node1.query("select * from test_25338_undrop order by id;") == "1\n2\n3\n"
    )
    node1.query("drop table test_25338_undrop sync;")

def test_undrop_ReplicatedMergeTree(started_cluster):
    node1.query("create table test_25338_undrop (id Int32) Engine=ReplicatedMergeTree('/clickhouse/tables/1/test_25338_undrop', '1') order by id;")
    node1.query("insert into test_25338_undrop values (1),(2),(3);")
    node1.query("drop table test_25338_undrop;")
    assert(
        node1.query("select value from system.metrics where metric = 'TablesToDropQueueSize';") == "1\n"
    )
    node1.query("undrop table test_25338_undrop;")
    assert(
        node1.query("select value from system.metrics where metric = 'TablesToDropQueueSize';") == "0\n"
    )
    assert(
        node1.query("select * from test_25338_undrop order by id;") == "1\n2\n3\n"
    )
    node1.query("drop table test_25338_undrop sync;")

def test_undrop_Memory(started_cluster):
    node1.query("create table test_25338_undrop (id Int32) Engine=Memory();")
    node1.query("drop table test_25338_undrop;")
    error = node1.query_and_get_error(
        "undrop table test_25338_undrop;"
    )
    assert "UNKNOWN_TABLE" in error

def test_undrop_Log(started_cluster):
    node1.query("create table test_25338_undrop (id Int32) Engine=Log();")
    node1.query("insert into test_25338_undrop values (1),(2),(3);")
    node1.query("drop table test_25338_undrop;")
    assert(
        node1.query("select value from system.metrics where metric = 'TablesToDropQueueSize';") == "1\n"
    )
    node1.query("undrop table test_25338_undrop;")
    assert(
        node1.query("select value from system.metrics where metric = 'TablesToDropQueueSize';") == "0\n"
    )
    assert(
        node1.query("select * from test_25338_undrop order by id;") == "1\n2\n3\n"
    )
    node1.query("drop table test_25338_undrop sync;")

def test_undrop_Distributed(started_cluster):
    node1.query("create table test_25338_undrop_d (id Int32) Engine = Distributed(test_cluster, default, test_25338_undrop, rand());")
    node1.query("drop table test_25338_undrop_d;")
    assert(
        node1.query("select value from system.metrics where metric = 'TablesToDropQueueSize';") == "1\n"
    )
    node1.query("undrop table test_25338_undrop_d;")
    assert(
        node1.query("select value from system.metrics where metric = 'TablesToDropQueueSize';") == "0\n"
    )
    assert(
        node1.query("select table from system.tables where table='test_25338_undrop_d';") == "test_25338_undrop_d\n"
    )
    node1.query("drop table test_25338_undrop_d sync;")

def test_undrop_drop_and_undrop_multiple_times(started_cluster):
    node1.query("create table test_25338_undrop (id Int32) Engine=MergeTree() order by id;")
    node1.query("insert into test_25338_undrop values (10);")
    node1.query("drop table test_25338_undrop;")
    assert(
        node1.query("select value from system.metrics where metric = 'TablesToDropQueueSize';") == "1\n"
    )
    node1.query("create table test_25338_undrop (id Int32) Engine=MergeTree() order by id;")
    node1.query("insert into test_25338_undrop values (20);")
    node1.query("drop table test_25338_undrop;")
    assert(
        node1.query("select value from system.metrics where metric = 'TablesToDropQueueSize';") == "2\n"
    )
    node1.query("create table test_25338_undrop (id Int32) Engine=MergeTree() order by id;")
    node1.query("insert into test_25338_undrop values (30);")
    node1.query("drop table test_25338_undrop;")
    assert(
        node1.query("select value from system.metrics where metric = 'TablesToDropQueueSize';") == "3\n"
    )
    node1.query("undrop table test_25338_undrop;")
    assert(
        node1.query("select value from system.metrics where metric = 'TablesToDropQueueSize';") == "2\n"
    )
    assert(
        node1.query("select * from test_25338_undrop order by id;") == "30\n"
    )
    error = node1.query_and_get_error(
        "undrop table test_25338_undrop;"
    )
    assert "TABLE_ALREADY_EXISTS" in error
    node1.query("drop table test_25338_undrop sync;")
