--CREATE schema public;

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
    CONSTRAINT book_pkey PRIMARY KEY (id),
    foreign key (author_id) references author(id)
);

insert into author (id, last_name) values (1, 'Hemingway');
insert into author (id, last_name) values (2, 'Huxley');
insert into book (id, author_id, title) values (1, 2, 'Brave new world');


CREATE TABLE datatypes (
    id           integer     NOT NULL,
    varchar_type varchar(50),
    text_type    text,
    boolean_type boolean,
    timestamp_type timestamp,
    date_type date,

    CONSTRAINT datatypes_pkey PRIMARY KEY (id)
);

insert into datatypes (id, varchar_type, text_type, boolean_type, timestamp_type, date_type) values (1, 'varchar', 'my text', true, '2019-01-01T12:19:11',
                                                                                                     '2020-02-03');

insert into datatypes (id, varchar_type, text_type, boolean_type, timestamp_type, date_type) values (100, null, null, true, '2019-01-01T12:19:11',
                                                                                                     '2020-02-03');
-- just for tests
create sequence datatypes_id_seq start with 1050 increment by 50;
create sequence strange_id_seq start with 1050 increment by 50;
