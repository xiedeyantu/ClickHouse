import pytest
import uuid
import random
import logging
import time

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node3 = cluster.add_instance("node3", main_configs=["configs/with_delay_config.xml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_undrop_MergeTree(started_cluster):
    node1.query("drop table if exists test_25338_undrop sync;")
    node1.query(
        "create table test_25338_undrop (id Int32, num Int32) Engine=MergeTree() order by id;"
    )
    node1.query("insert into test_25338_undrop values (1, 1);")
    node1.query("drop table test_25338_undrop;")
    assert (
        node1.query(
            "select value from system.metrics where metric = 'TablesToDropQueueSize';"
        )
        == "1\n"
    )
    node1.query(
        "undrop table test_25338_undrop settings allow_experimental_undrop_table_query = 1;"
    )
    assert (
        node1.query(
            "select value from system.metrics where metric = 'TablesToDropQueueSize';"
        )
        == "0\n"
    )
    assert node1.query("select num from test_25338_undrop order by id;") == "1\n"
    # check mutation
    node1.query("alter table test_25338_undrop update num = 2 where id = 1;")
    assert (
        node1.query(
            "select command from system.mutations where table='test_25338_undrop'"
        )
        == "UPDATE num = 2 WHERE id = 1\n"
    )
    # check detach
    node1.query("detach table test_25338_undrop;")
    error = node1.query_and_get_error(
        "undrop table test_25338_undrop settings allow_experimental_undrop_table_query = 1;"
    )
    assert "TABLE_ALREADY_EXISTS" in error
    node1.query("attach table test_25338_undrop;")
    node1.query("drop table test_25338_undrop sync;")


def test_undrop_MergeTree_with_uuid(started_cluster):
    node1.query(
        "create table test_25338_undrop UUID '3719b97a-fc7c-4bb1-84c0-a9906006fb88' (id Int32) Engine=MergeTree() order by id;"
    )
    node1.query("insert into test_25338_undrop values (1),(2),(3);")
    node1.query("drop table test_25338_undrop;")
    assert (
        node1.query(
            "select value from system.metrics where metric = 'TablesToDropQueueSize';"
        )
        == "1\n"
    )
    node1.query(
        "undrop table test_25338_undrop UUID '3719b97a-fc7c-4bb1-84c0-a9906006fb88' settings allow_experimental_undrop_table_query = 1;"
    )
    assert (
        node1.query(
            "select value from system.metrics where metric = 'TablesToDropQueueSize';"
        )
        == "0\n"
    )
    assert node1.query("select * from test_25338_undrop order by id;") == "1\n2\n3\n"
    node1.query("drop table test_25338_undrop sync;")


def test_undrop_MergeTree_on_cluster(started_cluster):
    node1.query(
        "create table test_25338_undrop on cluster test_cluster (id Int32) Engine=MergeTree() order by id;"
    )
    node1.query("insert into test_25338_undrop values (1),(2),(3);")
    node1.query("drop table test_25338_undrop on cluster test_cluster;")
    assert (
        node1.query(
            "select value from system.metrics where metric = 'TablesToDropQueueSize';"
        )
        == "1\n"
    )
    node1.query(
        "undrop table test_25338_undrop on cluster test_cluster settings allow_experimental_undrop_table_query = 1;"
    )
    assert (
        node1.query(
            "select value from system.metrics where metric = 'TablesToDropQueueSize';"
        )
        == "0\n"
    )
    assert node1.query("select * from test_25338_undrop order by id;") == "1\n2\n3\n"
    node1.query("drop table test_25338_undrop sync;")


def test_undrop_ReplicatedMergeTree(started_cluster):
    node1.query(
        "create table test_25338_undrop (id Int32) Engine=ReplicatedMergeTree('/clickhouse/tables/1/test_25338_undrop', '1') order by id;"
    )
    node1.query("insert into test_25338_undrop values (1),(2),(3);")
    node1.query("drop table test_25338_undrop;")
    assert (
        node1.query(
            "select value from system.metrics where metric = 'TablesToDropQueueSize';"
        )
        == "1\n"
    )
    node1.query(
        "undrop table test_25338_undrop settings allow_experimental_undrop_table_query = 1;"
    )
    assert (
        node1.query(
            "select value from system.metrics where metric = 'TablesToDropQueueSize';"
        )
        == "0\n"
    )
    assert node1.query("select * from test_25338_undrop order by id;") == "1\n2\n3\n"
    node1.query("drop table test_25338_undrop sync;")


def test_undrop_Memory(started_cluster):
    node1.query("create table test_25338_undrop (id Int32) Engine=Memory();")
    node1.query("drop table test_25338_undrop;")
    error = node1.query_and_get_error(
        "undrop table test_25338_undrop settings allow_experimental_undrop_table_query = 1;"
    )
    assert "UNKNOWN_TABLE" in error


def test_undrop_Log(started_cluster):
    node1.query("create table test_25338_undrop (id Int32) Engine=Log();")
    node1.query("insert into test_25338_undrop values (1),(2),(3);")
    node1.query("drop table test_25338_undrop;")
    assert (
        node1.query(
            "select value from system.metrics where metric = 'TablesToDropQueueSize';"
        )
        == "1\n"
    )
    node1.query(
        "undrop table test_25338_undrop settings allow_experimental_undrop_table_query = 1;"
    )
    assert (
        node1.query(
            "select value from system.metrics where metric = 'TablesToDropQueueSize';"
        )
        == "0\n"
    )
    assert node1.query("select * from test_25338_undrop order by id;") == "1\n2\n3\n"
    node1.query("drop table test_25338_undrop sync;")


def test_undrop_Distributed(started_cluster):
    node1.query(
        "create table test_25338_undrop_d (id Int32) Engine = Distributed(test_cluster, default, test_25338_undrop, rand());"
    )
    node1.query("drop table test_25338_undrop_d;")
    assert (
        node1.query(
            "select value from system.metrics where metric = 'TablesToDropQueueSize';"
        )
        == "1\n"
    )
    node1.query(
        "undrop table test_25338_undrop_d settings allow_experimental_undrop_table_query = 1;"
    )
    assert (
        node1.query(
            "select value from system.metrics where metric = 'TablesToDropQueueSize';"
        )
        == "0\n"
    )
    assert (
        node1.query(
            "select table from system.tables where table='test_25338_undrop_d';"
        )
        == "test_25338_undrop_d\n"
    )
    node1.query("drop table test_25338_undrop_d sync;")


def test_undrop_drop_and_undrop_multiple_times(started_cluster):
    node1.query(
        "create table test_25338_undrop (id Int32) Engine=MergeTree() order by id;"
    )
    node1.query("insert into test_25338_undrop values (10);")
    node1.query("drop table test_25338_undrop;")
    assert (
        node1.query(
            "select value from system.metrics where metric = 'TablesToDropQueueSize';"
        )
        == "1\n"
    )
    node1.query(
        "create table test_25338_undrop (id Int32) Engine=MergeTree() order by id;"
    )
    node1.query("insert into test_25338_undrop values (20);")
    node1.query("drop table test_25338_undrop;")
    assert (
        node1.query(
            "select value from system.metrics where metric = 'TablesToDropQueueSize';"
        )
        == "2\n"
    )
    node1.query(
        "create table test_25338_undrop (id Int32) Engine=MergeTree() order by id;"
    )
    node1.query("insert into test_25338_undrop values (30);")
    node1.query("drop table test_25338_undrop;")
    assert (
        node1.query(
            "select value from system.metrics where metric = 'TablesToDropQueueSize';"
        )
        == "3\n"
    )
    node1.query(
        "undrop table test_25338_undrop settings allow_experimental_undrop_table_query = 1;"
    )
    assert (
        node1.query(
            "select value from system.metrics where metric = 'TablesToDropQueueSize';"
        )
        == "2\n"
    )
    assert node1.query("select * from test_25338_undrop order by id;") == "30\n"
    error = node1.query_and_get_error(
        "undrop table test_25338_undrop settings allow_experimental_undrop_table_query = 1;"
    )
    assert "TABLE_ALREADY_EXISTS" in error
    node1.query("drop table test_25338_undrop sync;")


def test_undrop_drop_and_undrop_loop(started_cluster):
    count = 0
    while count < 10:
        random_sec = random.randint(0, 10)
        table_uuid = uuid.uuid1().__str__()
        logging.info(
            "random_sec: " + random_sec.__str__() + ", table_uuid: " + table_uuid
        )
        node3.query(
            "create table test_25338_undrop_loop"
            + count.__str__()
            + " UUID '"
            + table_uuid
            + "' (id Int32) Engine=MergeTree() order by id;"
        )
        node3.query(
            "insert into test_25338_undrop_loop" + count.__str__() + " values (1);"
        )
        node3.query("drop table test_25338_undrop_loop" + count.__str__() + ";")
        time.sleep(random_sec)
        ret, error = node3.query_and_get_answer_with_error(
            "undrop table test_25338_undrop_loop"
            + count.__str__()
            + " uuid '"
            + table_uuid
            + "' settings allow_experimental_undrop_table_query = 1;"
        )
        if "UNKNOWN_TABLE" in error:
            logging.info("UNKNOWN_TABLE")
        else:
            assert (
                node3.query(
                    "select * from test_25338_undrop_loop" + count.__str__() + ";"
                )
                == "1\n"
            )
        count = count + 1
