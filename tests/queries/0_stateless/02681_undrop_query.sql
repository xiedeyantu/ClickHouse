-- Tags: no-ordinary-database, shard

set database_atomic_wait_for_drop_and_detach_synchronously = 0;
set allow_experimental_undrop_table_query = 1;


select 'test MergeTree undrop';
drop table if exists 02681_undrop_mergetree sync;
create table 02681_undrop_mergetree (id Int32) Engine=MergeTree() order by id;
insert into 02681_undrop_mergetree values (1),(2),(3);
drop table 02681_undrop_mergetree;
select table from system.marked_dropped_tables where table = '02681_undrop_mergetree';
undrop table 02681_undrop_mergetree;
select table from system.marked_dropped_tables;
select * from 02681_undrop_mergetree order by id;
drop table 02681_undrop_mergetree sync;

select 'test detach';
drop table if exists 02681_undrop_mergetree sync;
create table 02681_undrop_detach (id Int32, num Int32) Engine=MergeTree() order by id;
insert into 02681_undrop_detach values (1, 1);
detach table 02681_undrop_detach;
undrop table 02681_undrop_detach; -- { serverError 57 }
attach table 02681_undrop_detach;
alter table 02681_undrop_detach update num = 2 where id = 1;
select command, is_done from system.mutations where table='02681_undrop_detach';
drop table 02681_undrop_detach sync;

select 'test MergeTree with uuid';
drop table if exists 02681_undrop_uuid sync;
create table 02681_undrop_uuid UUID '3719b97a-fc7c-4bb1-84c0-a9906006fb88' (id Int32) Engine=MergeTree() order by id;
insert into 02681_undrop_uuid values (1),(2),(3);
drop table 02681_undrop_uuid;
select table from system.marked_dropped_tables where table = '02681_undrop_uuid';
undrop table 02681_undrop_uuid UUID '3719b97a-fc7c-4bb1-84c0-a9906006fb88';
select table from system.marked_dropped_tables where table = '02681_undrop_uuid';
select * from 02681_undrop_uuid order by id;
drop table 02681_undrop_uuid sync;

select 'test MergeTree with uuid and cluster';
drop table if exists 02681_undrop_uuid_on_cluster on cluster test_cluster_two_shards sync format Null;
create table 02681_undrop_uuid_on_cluster UUID '3719b97a-fc7c-4bb1-84c0-a9906006fb89' on cluster test_cluster_two_shards (id Int32) Engine=MergeTree() order by id format Null;
insert into 02681_undrop_uuid_on_cluster values (1),(2),(3);
drop table 02681_undrop_uuid_on_cluster on cluster test_cluster_two_shards format Null;
select table from system.marked_dropped_tables where table = '02681_undrop_uuid_on_cluster';
undrop table 02681_undrop_uuid_on_cluster UUID '3719b97a-fc7c-4bb1-84c0-a9906006fb89' on cluster test_cluster_two_shards format Null;
select table from system.marked_dropped_tables where table = '02681_undrop_uuid_on_cluster';
select * from 02681_undrop_uuid_on_cluster order by id;
drop table 02681_undrop_uuid_on_cluster sync;

select 'test MergeTree without uuid on cluster';
drop table if exists 02681_undrop_no_uuid_on_cluster on cluster test_cluster_two_shards sync format Null;
create table 02681_undrop_no_uuid_on_cluster on cluster test_cluster_two_shards (id Int32) Engine=MergeTree() order by id format Null;
insert into 02681_undrop_no_uuid_on_cluster values (1),(2),(3);
drop table 02681_undrop_no_uuid_on_cluster on cluster test_cluster_two_shards format Null;
select table from system.marked_dropped_tables where table = '02681_undrop_no_uuid_on_cluster';
undrop table 02681_undrop_no_uuid_on_cluster on cluster test_cluster_two_shards format Null;
select table from system.marked_dropped_tables where table = '02681_undrop_no_uuid_on_cluster';
select * from 02681_undrop_no_uuid_on_cluster order by id;
drop table 02681_undrop_no_uuid_on_cluster on cluster test_cluster_two_shards sync format Null;

select 'test ReplicatedMergeTree undrop';
drop table if exists 02681_undrop_replicatedmergetree sync;
create table 02681_undrop_replicatedmergetree (id Int32) Engine=ReplicatedMergeTree('/clickhouse/tables/1/02681_undrop_replicatedmergetree', '1') order by id;
insert into 02681_undrop_replicatedmergetree values (1),(2),(3);
drop table 02681_undrop_replicatedmergetree;
select table from system.marked_dropped_tables where table = '02681_undrop_replicatedmergetree';
undrop table 02681_undrop_replicatedmergetree;
select table from system.marked_dropped_tables where table = '02681_undrop_replicatedmergetree';
select * from 02681_undrop_replicatedmergetree order by id;
drop table 02681_undrop_replicatedmergetree sync;

select 'test Memory undrop';
drop table if exists 02681_undrop_memory sync;
create table 02681_undrop_memory (id Int32) Engine=Memory();
insert into 02681_undrop_memory values (1),(2),(3);
drop table 02681_undrop_memory;
select table from system.marked_dropped_tables where table = '02681_undrop_memory';
undrop table 02681_undrop_memory; -- { serverError 60 }
select table from system.marked_dropped_tables where table = '02681_undrop_memory';

select 'test Log undrop';
drop table if exists 02681_undrop_log sync;
create table 02681_undrop_log (id Int32) Engine=Log();
insert into 02681_undrop_log values (1),(2),(3);
drop table 02681_undrop_log;
select table from system.marked_dropped_tables where table = '02681_undrop_log';
undrop table 02681_undrop_log;
select table from system.marked_dropped_tables where table = '02681_undrop_log';
select * from 02681_undrop_log order by id;
drop table 02681_undrop_log sync;

select 'test Distributed undrop';
drop table if exists 02681_undrop_distributed sync;
create table 02681_undrop_distributed (id Int32) Engine = Distributed(test_cluster_two_shards, default, 02681_undrop, rand());
drop table 02681_undrop_distributed;
select table from system.marked_dropped_tables where table = '02681_undrop_distributed';
undrop table 02681_undrop_distributed;
select table from system.marked_dropped_tables where table = '02681_undrop_distributed';
drop table 02681_undrop_distributed sync;

select 'test MergeTree drop and undrop multiple times';
drop table if exists 02681_undrop_multiple sync;
create table 02681_undrop_multiple (id Int32) Engine=MergeTree() order by id;
insert into 02681_undrop_multiple values (1);
drop table 02681_undrop_multiple;
select table from system.marked_dropped_tables where table = '02681_undrop_multiple';
create table 02681_undrop_multiple (id Int32) Engine=MergeTree() order by id;
insert into 02681_undrop_multiple values (2);
drop table 02681_undrop_multiple;
select table from system.marked_dropped_tables where table = '02681_undrop_multiple';
create table 02681_undrop_multiple (id Int32) Engine=MergeTree() order by id;
insert into 02681_undrop_multiple values (3);
drop table 02681_undrop_multiple;

select table from system.marked_dropped_tables where table = '02681_undrop_multiple';
undrop table 02681_undrop_multiple;
select table from system.marked_dropped_tables where table = '02681_undrop_multiple';
select * from 02681_undrop_multiple order by id;
undrop table 02681_undrop_multiple; -- { serverError 57 }
drop table 02681_undrop_multiple sync;
