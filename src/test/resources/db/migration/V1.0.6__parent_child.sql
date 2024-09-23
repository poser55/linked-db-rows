CREATE TABLE parent
(
    id        integer     NOT NULL,
    last_name varchar(50) NOT NULL,
    CONSTRAINT parent_pkey PRIMARY KEY (id)
);

CREATE TABLE child
(
    id        integer     NOT NULL,
    parent_id integer     NOT NULL,
    name      varchar(50) NOT NULL,
    CONSTRAINT child_pkey PRIMARY KEY (id),
    foreign key (parent_id) references parent (id)
);


insert into parent (id, last_name)
values (1, 'Mom');
insert into child (id, parent_id, name)
values (1, 1, 'Kid 1');
insert into child (id, parent_id, name)
values (2, 1, 'Kid 2');
insert into child (id, parent_id, name)
values (3, 1, 'Kid 3');
