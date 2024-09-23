-- Source: /joe-celkos-sql

CREATE TABLE Nodes (
    node_id INTEGER NOT NULL,
    Name varchar(100),
    CONSTRAINT Nodes_pkey PRIMARY KEY (node_id)
);

CREATE TABLE Edge(
    begin_id INTEGER NOT NULL,
    end_id INTEGER NOT NULL,
    CONSTRAINT Edge_pkey PRIMARY KEY (begin_id, end_id),
    foreign key (begin_id) references Nodes(node_id),
    foreign key (end_id) references Nodes(node_id)
);



insert into Nodes values (1, 'Zuerich');
insert into Nodes values (2, 'Bern');
insert into Nodes values (3, 'Basel');
insert into Nodes values (4, 'Riehen');
insert into Nodes values (5, 'Oerlikon');

insert into Edge values (1,2);
insert into Edge values (2,3);
insert into Edge values (1,3);
insert into Edge values (3,4);
insert into Edge values (1,5);


insert into Nodes values (20, 'NY');
insert into Nodes values (21, 'NJ');

insert into Edge values (20,21);

${mysql_include_start}
ALTER TABLE Nodes ENGINE=InnoDB;
ALTER TABLE Edge ENGINE=InnoDB;
SET FOREIGN_KEY_CHECKS=1
${mysql_include_end}


