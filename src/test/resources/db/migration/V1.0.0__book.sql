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
    CONSTRAINT book_pkey PRIMARY KEY (id),
    CONSTRAINT fk_book_author FOREIGN KEY (author_id)
        REFERENCES author (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

insert into author (id, last_name) values (1, 'Hemingway');
insert into author (id, last_name) values (2, 'Huxley');
insert into book (id, author_id, title) values (1, 2, 'Brave new world');



