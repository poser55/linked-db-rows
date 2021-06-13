create table special_datatypes (
    id integer not null,

    ${oracle_exclude_start}
    ${sqlserver_exclude_start}
    ${mysql_exclude_start}
     uuid_example uuid,
    ${mysql_exclude_end}
    ${sqlserver_exclude_end}
    ${oracle_exclude_end}

    ${oracle_include_start}
     uuid_example varchar(128),
    ${oracle_include_end}

    ${sqlserver_include_start}
     uuid_example varchar(128),
    ${sqlserver_include_end}

    ${mysql_include_start}
     uuid_example varchar(128),
    ${mysql_include_end}

    -- clob is text in postgres
    ${sqlserver_exclude_start}
    ${mysql_exclude_start}
     additional_text clob,
    ${mysql_exclude_end}
    ${sqlserver_exclude_end}

    ${sqlserver_include_start}
     additional_text varchar(200),
    ${sqlserver_include_end}

    ${mysql_include_start}
     additional_text varchar(200),
    ${mysql_include_end}

    ${sqlserver_exclude_start}
    ${postgres_exclude_start}
     a_blob blob,
    ${postgres_exclude_end}
    ${sqlserver_exclude_end}

    ${postgres_include_start}
     a_blob bytea,
    ${postgres_include_end}

    primary key (id)
);


insert into special_datatypes (id, uuid_example, additional_text) values (  1, 'ea0e2ebc-ff0b-4ce4-863f-be70222a7084' , 'bla');
insert into special_datatypes (id, uuid_example, additional_text) values (  2, null, 'bla bla');
insert into special_datatypes (id, uuid_example, additional_text) values (  3, 'a-a-a', 'bla bla');

${postgres_include_start}
CREATE TABLE postgres_test (
    id int8 NOT NULL,
    file bytea NULL,
    delta varchar(500) NULL,
    CONSTRAINT postgres_pkey PRIMARY KEY (id)
);

-- sets file to null
insert into postgres_test values (  1, 'xxx', 'ea0e2ebc-ff0b-4ce4-863f-be70222a7084' );

INSERT INTO postgres_test (id, file) VALUES (10, decode('013d7d16d7ad4fefb61bd95b765c8ceb', 'hex'))

${postgres_include_end}


${h2_include_start}
update special_datatypes set a_blob = RAWTOHEX('Test') where id = 1;
${h2_include_end}

${oracle_include_start}
update special_datatypes set a_blob = utl_raw.cast_to_raw ('value') where id = 1;
${oracle_include_end}

-- todo sqlserver init is missing
-- ${sqlserver_include_start}
-- update special_datatypes set a_blob = 'blob value' where id = 1;
-- ${sqlserver_include_end}

${mysql_include_start}
update special_datatypes set a_blob = 'blob value' where id = 1;
${mysql_include_end}