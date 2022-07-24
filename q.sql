\echo 1

begin;

delete from balances;

copy balances from stdin csv header;

commit;
