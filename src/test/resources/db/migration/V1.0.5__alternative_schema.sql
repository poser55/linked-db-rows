-- we lack permissions to create schemas in our oracle and mysql test container; sqlserver somehow does not like our schema commands
${oracle_exclude_start}
${sqlserver_exclude_start}
${mysql_exclude_start}
create schema doc;
SET SCHEMA doc;



-- the following does not work in the test container (missing permission)
-- ${oracle_include_start}
--     CREATE USER doc
--         IDENTIFIED BY docdoc;
-- ${oracle_include_end}


CREATE TABLE doc.document(
    id integer NOT NULL,
    name varchar(50) NOT NULL,
    content varchar(1024),
    CONSTRAINT document_pkey PRIMARY KEY (id)
);

insert into doc.document (id, name) values (1, 'doc1');
insert into doc.document (id, name) values (2, 'Slaughter House Five');


set schema public;

-- ${sqlserver_include_start}
-- set schema dbo;
-- ${sqlserver_include_end}

${mysql_exclude_end}
${sqlserver_exclude_end}
${oracle_exclude_end}