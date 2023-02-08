drop schema if exists inventory;
create schema inventory;
set search_path=inventory;

CREATE OR REPLACE FUNCTION update_modify_time_column()
RETURNS TRIGGER AS $$
BEGIN
   NEW.modify_time = now();
   RETURN NEW;
END;
$$ language 'plpgsql';

-- create  user table
drop table if exists inventory.user;
create table if not exists inventory.user (
    id           serial not null,
    name         varchar(155)                        null,
    device_model varchar(155)                        null,
    email        varchar(50)                         null,
    phone        varchar(50)                         null,
    create_time  timestamp default CURRENT_TIMESTAMP not null,
    modify_time  timestamp default CURRENT_TIMESTAMP not null,
    primary key (id)
);

CREATE TRIGGER update_user_modify_time BEFORE UPDATE
    ON inventory.user FOR EACH ROW EXECUTE PROCEDURE
    update_modify_time_column();

-- insert data
insert into inventory.user(name,device_model,email,phone) values
('customer-01','dm-01','abc01@email.com','188776xxxxx'),
('customer-02','dm-02','abc02@email.com','166776xxxxx');

-- create product table
create table if not exists inventory.product
(
    pid          serial not null,
    pname        varchar(155)                        null,
    pprice       decimal(10,2)                           ,
    create_time  timestamp default CURRENT_TIMESTAMP not null,
    modify_time  timestamp default CURRENT_TIMESTAMP not null,
    primary key (pid)
);

CREATE TRIGGER update_user_modify_time BEFORE UPDATE
    ON inventory.product FOR EACH ROW EXECUTE PROCEDURE
    update_modify_time_column();

-- insert data
insert into inventory.product(pid,pname,pprice) values
('1','prodcut-001',125.12),
('2','prodcut-002',225.31);

-- create order table
drop table if exists inventory.user_order;
create table if not exists inventory.user_order
(
    id           serial,
    oid          varchar(155)                        not null,
    uid          int                                         ,
    pid          int                                         ,
    onum         int                                         ,
    create_time  timestamp default CURRENT_TIMESTAMP not null,
    modify_time  timestamp default CURRENT_TIMESTAMP not null,
    primary key (id)
);

CREATE TRIGGER update_user_modify_time BEFORE UPDATE
    ON inventory.user_order FOR EACH ROW EXECUTE PROCEDURE
    update_modify_time_column();

-- insert data
insert into user_order(oid,uid,pid,onum) values
('o10001',1,1,100),
('o10002',1,2,30),
('o10001',2,1,22),
('o10002',2,2,16);

-- select data
select * from user;
select * from product;
select * from user_order;
