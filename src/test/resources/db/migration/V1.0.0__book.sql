-- CREATE schema public;

CREATE TABLE author (
    id integer NOT NULL,
    last_name varchar(50) NOT NULL,
    CONSTRAINT author_pkey PRIMARY KEY (id)
);

CREATE TABLE book (
    id integer NOT NULL,
    author_id integer NOT NULL,
    title varchar(50) NOT NULL,
    number_pages integer,
    dms_id integer,
    CONSTRAINT book_pkey PRIMARY KEY (id),
    foreign key (author_id) references author(id)
);

insert into author (id, last_name) values (1, 'Hemingway');
insert into author (id, last_name) values (2, 'Huxley');
insert into author (id, last_name) values (3, 'Vonnegut');
insert into book (id, author_id, title, dms_id) values (1, 2, 'Brave new world', 1);
--insert into book (id, author_id, title) values (2, 2, 'Island');
insert into book (id, author_id, title, dms_id) values (3, 3, 'Slaughter House Five' , 2);


CREATE TABLE datatypes (
    id           integer     NOT NULL,
    varchar_type varchar(50),

    ${oracle_exclude_start}
    ${hsqldb_exclude_start}
        text_type    text,
    ${oracle_exclude_end}
    ${hsqldb_exclude_end}
    ${oracle_include_start}
        text_type    varchar(50),
    ${oracle_include_end}
    ${hsqldb_include_start}
        text_type    varchar(50),
    ${hsqldb_include_end}



${oracle_exclude_start}
${sqlserver_exclude_start}
    boolean_type boolean,
${sqlserver_exclude_end}
${oracle_exclude_end}

${oracle_include_start}
    boolean_type char(1),
${oracle_include_end}
${sqlserver_include_start}
    boolean_type char(1),
${sqlserver_include_end}

${sqlserver_exclude_start}
    timestamp_type timestamp,
${sqlserver_exclude_end}
${sqlserver_include_start}
    timestamp_type datetime,
${sqlserver_include_end}
    date_type date,

    CONSTRAINT datatypes_pkey PRIMARY KEY (id)
);

${oracle_include_start}
ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD';
ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD"T"HH:MI:SS';

${oracle_include_end}

insert into datatypes (id, varchar_type, text_type, boolean_type, timestamp_type, date_type) values (1, 'varchar', 'my text',
                                                                                                     ${oracle_exclude_start}
                                                                                                     ${sqlserver_exclude_start}
                                                                                                        true,
                                                                                                     ${sqlserver_exclude_end}
                                                                                                     ${oracle_exclude_end}
                                                                                                     ${oracle_include_start}
                                                                                                         'Y',
                                                                                                     ${oracle_include_end}
                                                                                                     ${sqlserver_include_start}
                                                                                                         'Y',
                                                                                                     ${sqlserver_include_end}
                                                                                                     ${hsqldb_exclude_start}
                                                                                                     '2019-01-01T12:19:11',
                                                                                                     ${hsqldb_exclude_end}
                                                                                                     ${hsqldb_include_start}
                                                                                                         '2019-01-01 12:19:11',
                                                                                                     ${hsqldb_include_end}
                                                                                                     '2020-02-03');

insert into datatypes (id, varchar_type, text_type, boolean_type, timestamp_type, date_type) values (100, null, null,
                                                                                                     ${oracle_exclude_start}
                                                                                                     ${sqlserver_exclude_start}
                                                                                                        true,
                                                                                                     ${sqlserver_exclude_end}
                                                                                                     ${oracle_exclude_end}
                                                                                                     ${oracle_include_start}
                                                                                                         'Y',
                                                                                                     ${oracle_include_end}
                                                                                                     ${sqlserver_include_start}
                                                                                                         'Y',
                                                                                                     ${sqlserver_include_end}
                                                                                                         ${hsqldb_exclude_start}
                                                                                                         '2019-01-01T12:19:11',
                                                                                                     ${hsqldb_exclude_end}
                                                                                                         ${hsqldb_include_start}
                                                                                                         '2019-01-01 12:19:11',
                                                                                                     ${hsqldb_include_end}
                                                                                                     '2020-02-03');
-- just for tests
${mysql_exclude_start}
create sequence datatypes_id_seq start with 1050 increment by 50;
create sequence strange_id_seq start with 1050 increment by 50;
${mysql_exclude_end}

${mysql_include_start}
-- is unfortunately not globally active (so needs to be set on each connection again)
SET SQL_MODE=ANSI_QUOTES;
${mysql_include_end}