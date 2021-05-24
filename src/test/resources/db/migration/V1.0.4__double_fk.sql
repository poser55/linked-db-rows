CREATE TABLE combined(
    begin_id INTEGER NOT NULL,
    end_id INTEGER NOT NULL,
    CONSTRAINT combined_pkey PRIMARY KEY (begin_id, end_id)
);

CREATE TABLE link(
    id INTEGER NOT NULL,
    begin_id INTEGER NOT NULL,
    end_id INTEGER NOT NULL,
    foreign key (begin_id, end_id) references combined(begin_id, end_id),
    PRIMARY KEY (id)
);

insert into combined values (1, 2);
insert into combined values (1, 3);

insert into link  values (1, 1, 2);