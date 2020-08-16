create table user_table (
                        id serial4 not null,
                        username varchar(128) not null,
                        email varchar(128) not null,
                        profile text null,
                        primary key (id)
);
create table blogpost (
                          id serial4 not null,
                          title varchar(128) not null,
                          content text not null,
                          tags text null,
                          status int not null,
                          create_time int null,
                          update_time int null,
                          user_id int not null,
                          primary key (id),
                          constraint FK_post_user
                              foreign key (user_id)
                                  references user_table (id) on delete cascade on update restrict
);
create table comment (
                         id serial4 not null,
                         content text not null,
                         status int not null,
                         create_time int null,
                         author varchar(128) not null,
                         email varchar(128) not null,
                         url varchar(128) null,
                         post_id int not null,
                         primary key (id),
                         constraint FK_comment_post
                             foreign key (post_id)
                                 references blogpost (id) on delete cascade on update restrict
);
create table tag_table (
                           id serial4 not null,
                           name varchar(128) not null,
                           frequency int null default 1,
                           primary key (id)
);
insert into user_table (
    id,
    username,
    email
)
values (    1,
            'demo',
            'webmaster@example.com'
       );
insert into user_table (
    id,
    username,
    email
)
values (
           2,
           'poser',
           'xxx@example.com'
       );

insert into blogpost (
    id,
    title,
    content,
    status,
    create_time,
    update_time,
    user_id,
    tags
)
values (
           1,
           'Welcome!',
           'My blog entry',
           2,
           1330952187,
           1330952187,
           1,
           'entry 1'
       );
insert into blogpost (
    id,
    title,
    content,
    status,
    create_time,
    update_time,
    user_id,
    tags
)
values (
           2,
           'A Test Post',
           'Lorem ipsum dolor sit amet, adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.',
           2,
           1250952187,
           1250952187,
           2,
           'test'
       );
insert into comment (
    content,
    status,
    create_time,
    author,
    email,
    post_id
)
values (
           'This is a test comment.',
           2,
           1250952187,
           'Tester',
           'tester@example.com',
           2
       );
insert into tag_table (name)
values ('tag');
insert into tag_table (name)
values ('blog');
insert into tag_table (name)
values ('test');