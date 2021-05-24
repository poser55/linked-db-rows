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

    primary key (id)
);


insert into special_datatypes values (  1, 'ea0e2ebc-ff0b-4ce4-863f-be70222a7084' , 'bla');
insert into special_datatypes values (  2, null, 'bla bla');


