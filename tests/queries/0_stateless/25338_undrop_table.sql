-- Tags: no-parallel, distributed, zookeeper

-- { echo }
select 'test MergeTree undrop';
drop table if exists test_25338_undrop sync;
create table test_25338_undrop (id Int32) Engine=MergeTree() order by id;
insert into test_25338_undrop values (1),(2),(3);
drop table test_25338_undrop;
select metric,value from system.metrics where metric = 'TablesToDropQueueSize';
undrop table test_25338_undrop;
select metric,value from system.metrics where metric = 'TablesToDropQueueSize';
select * from test_25338_undrop order by id;
drop table test_25338_undrop sync;

select 'test MergeTree with uuid';
create table test_25338_undrop UUID '3719b97a-fc7c-4bb1-84c0-a9906006fb88' (id Int32) Engine=MergeTree() order by id;
insert into test_25338_undrop values (1),(2),(3);
drop table test_25338_undrop;
select metric,value from system.metrics where metric = 'TablesToDropQueueSize';
undrop table test_25338_undrop UUID '3719b97a-fc7c-4bb1-84c0-a9906006fb88';
select metric,value from system.metrics where metric = 'TablesToDropQueueSize';
select * from test_25338_undrop order by id;
drop table test_25338_undrop sync;

select 'test MergeTree on cluster';
create table test_25338_undrop on cluster test_cluster_two_shards (id Int32) Engine=MergeTree() order by id format Null;
insert into test_25338_undrop values (1),(2),(3);
drop table test_25338_undrop on cluster test_cluster_two_shards format Null;
select metric,value from system.metrics where metric = 'TablesToDropQueueSize';
undrop table test_25338_undrop on cluster test_cluster_two_shards format Null;
select metric,value from system.metrics where metric = 'TablesToDropQueueSize';
select * from test_25338_undrop order by id;
drop table test_25338_undrop on cluster test_cluster_two_shards sync format Null;

select 'test ReplicatedMergeTree undrop';
create table test_25338_undrop (id Int32) Engine=ReplicatedMergeTree('/clickhouse/tables/1/test_25338_undrop', '1') order by id;
insert into test_25338_undrop values (1),(2),(3);
drop table test_25338_undrop;
select metric,value from system.metrics where metric = 'TablesToDropQueueSize';
undrop table test_25338_undrop;
select metric,value from system.metrics where metric = 'TablesToDropQueueSize';
select * from test_25338_undrop order by id;
drop table test_25338_undrop sync;

select 'test Memory undrop';
create table test_25338_undrop (id Int32) Engine=Memory();
insert into test_25338_undrop values (1),(2),(3);
drop table test_25338_undrop;
select metric,value from system.metrics where metric = 'TablesToDropQueueSize';
undrop table test_25338_undrop; -- { serverError 60 }
select metric,value from system.metrics where metric = 'TablesToDropQueueSize';

select 'test Log undrop';
create table test_25338_undrop (id Int32) Engine=Log();
insert into test_25338_undrop values (1),(2),(3);
drop table test_25338_undrop;
select metric,value from system.metrics where metric = 'TablesToDropQueueSize';
undrop table test_25338_undrop;
select metric,value from system.metrics where metric = 'TablesToDropQueueSize';
select * from test_25338_undrop order by id;
drop table test_25338_undrop sync;

select 'test Distributed undrop';
create table test_25338_undrop_d (id Int32) Engine = Distributed(test_cluster_two_shards, currentDatabase(), test_25338_undrop, rand());
drop table test_25338_undrop_d;
select metric,value from system.metrics where metric = 'TablesToDropQueueSize';
undrop table test_25338_undrop_d;
select metric,value from system.metrics where metric = 'TablesToDropQueueSize';
select table from system.tables where table='test_25338_undrop_d';

select 'test MergeTree drop and undrop multiple times';
create table test_25338_undrop (id Int32) Engine=MergeTree() order by id;
insert into test_25338_undrop values (1);
drop table test_25338_undrop;
select sleep(1);
select metric,value from system.metrics where metric = 'TablesToDropQueueSize';
create table test_25338_undrop (id Int32) Engine=MergeTree() order by id;
insert into test_25338_undrop values (2);
drop table test_25338_undrop;
select sleep(1);
select metric,value from system.metrics where metric = 'TablesToDropQueueSize';
create table test_25338_undrop (id Int32) Engine=MergeTree() order by id;
insert into test_25338_undrop values (3);
drop table test_25338_undrop;
select sleep(1);
select metric,value from system.metrics where metric = 'TablesToDropQueueSize';
undrop table test_25338_undrop;
select metric,value from system.metrics where metric = 'TablesToDropQueueSize';
select * from test_25338_undrop order by id;
undrop table test_25338_undrop; -- { serverError 57 }
drop table test_25338_undrop sync;
