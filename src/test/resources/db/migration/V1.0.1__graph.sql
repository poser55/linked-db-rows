-- Source: /joe-celkos-sql

CREATE TABLE Nodes (node_id INTEGER NOT NULL PRIMARY KEY,
    Name varchar(100)
);

CREATE TABLE Edge(
    begin_id INTEGER NOT NULL
     REFERENCES Nodes (node_id)
         ON DELETE CASCADE,
    end_id INTEGER NOT NULL
     REFERENCES Nodes (node_id)
         ON DELETE CASCADE,
    PRIMARY KEY (begin_id, end_id));

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




