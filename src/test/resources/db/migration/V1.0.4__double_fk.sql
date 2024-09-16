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

CREATE TABLE link2self(
            id INTEGER NOT NULL PRIMARY KEY,
            peer integer,
            link_id integer,
            constraint link2self_self foreign key (peer) references link2self(id),
            constraint link2self_peer foreign key (link_id) references link(id)
);

insert into link2self  values (1, null, null);
insert into link2self  values (2, 1, 1);
insert into link2self  values (3, 2, 1);

-- testing special remappings for inserts

CREATE TABLE triplet(
    a INTEGER NOT NULL,
    b INTEGER NOT NULL,
    c INTEGER NOT NULL,
    CONSTRAINT triplet_pkey PRIMARY KEY (a, b, c)
);

CREATE TABLE link_triplet(
    id INTEGER NOT NULL,
    a INTEGER NOT NULL,
    b INTEGER NOT NULL,
    c INTEGER NOT NULL,
    foreign key (a, b, c) references triplet(a, b, c),
    PRIMARY KEY (id)
);

insert into triplet values (1, 2, 3);
insert into triplet values (1, 3, 5);

insert into triplet values (2, 2, 3);
insert into triplet values (3, 2, 3);

insert into link_triplet values (1, 1, 2, 3);
insert into link_triplet values (2, 1, 3, 5);
