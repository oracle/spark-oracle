create user "schemaa" identified by schemaa;
grant CONNECT, RESOURCE, unlimited tablespace to "schemaa";

create user "schemaA" identified by schemaA;
grant CONNECT, RESOURCE, unlimited tablespace to "schemaA";


-- schema tables
connect "schemaa"/schemaa
create table "a"(
    "a"  varchar2(30),
    "A"  varchar2(30),
    "--a"  varchar2(30),
    "or 1=1" varchar2(30)
);

grant SELECT on "a" to public;

insert into "a"
values('A_a_a', 'B_a_a', 'C_a_a', 'D_a_a');
insert into "a"
values('''A_a_a'' or 1==1', null, null, null);
commit;

create table "A"(
                    "a"  varchar2(30),
                    "A"  varchar2(30),
                    "--a"  varchar2(30),
                    "or 1=1" varchar2(30)
);
grant SELECT on "A" to public;

insert into "A"
values('A_a_A', 'B_a_A', 'C_a_A', 'D_a_A');
commit;


connect "schemaA"/schemaA
create table "a"(
                    "a"  varchar2(30),
                    "A"  varchar2(30),
                    "--a"  varchar2(30),
                    "or 1=1" varchar2(30)
);

grant SELECT on "a" to public;

insert into "a"
values('A_A_a', 'B_A_a', 'C_A_a', 'D_A_a');
insert into "a"
values('''A_A_a'' or 1==1', null, null, null);
commit;

create table "A"(
                    "a"  varchar2(30),
                    "A"  varchar2(30),
                    "--a"  varchar2(30),
                    "or 1=1" varchar2(30)
);
grant SELECT on "A" to public;

insert into "A"
values('A_A_A', 'B_A_A', 'C_A_A', 'D_A_A');
