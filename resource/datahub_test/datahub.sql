
case 003 "12" 0
before:
drop table ora.i2_char003;
drop table mill.i2_char004;
drop table ora.i2_binary002;
drop table ora.i2_time0021 ;
drop table ora.i2_no002 ;
drop TABLE ora.i2_geome002;
drop TABLE ora.i2_xml002;
drop TABLE ora.i2_unique002;
drop TABLE ora.i2_clr002;
drop table ora.i2_variant002;
drop table ora.i2_text002;
drop table ora.i2_sysname002;
drop table mill.i2_binary002;
drop table mill.i2_time0021 ;
drop table mill.i2_no002 ;
drop TABLE mill.i2_geome002;
drop TABLE mill.i2_xml002;
drop TABLE mill.i2_unique002;
drop TABLE mill.i2_clr002;
drop table mill.i2_variant002;
drop table mill.i2_text002;
drop table mill.i2_sysname002;

go

src_sql:
create table ora.i2_char003 (
    no           int IDENTITY(1,1),
    c_char       char(5) not null,
    c_nchar      nchar(10) default 'def',
    c_varchar    varchar(20) not null,
    c_nvarchar   nvarchar(3000),
	constraint pk_nochar003 primary key(no),
	constraint qu_vcchar003 unique(c_char)
);
insert into ora.i2_char003 values ('A','56789','AA101','1111ABC');
insert into ora.i2_char003 values ('B','45678','AA102','1111DEF');
insert into ora.i2_char003 values ('C','45678','AB103','1Ϻ1Ӣ111ɷ1');
insert into ora.i2_char003(c_char,c_varchar) values ('D','AB104');
update ora.i2_char003 set c_char='66666' where no = 2
delete from ora.i2_char003 where  c_char='d';
go

create table ora.i2_binary002 (
    no                  int primary key,
    c_binary            binary(30),
    c_varbinary         varbinary(1000),
	c_varbinarym	    varbinary(max)
);

insert into ora.i2_binary002 values (1,0x1f,0x111111111,0x44444444aaaaaaaaa);
insert into ora.i2_binary002 values (2,0x2f,0x222222222,0x55555555bbbbbbbbb);
insert into ora.i2_binary002 values (3,0x3f,0x333333333,0x66666666ccccccccc);
insert into ora.i2_binary002(no,c_binary) values(4,0x4f);
update ora.i2_binary002 set c_binary=0x5e,c_varbinary=0x34343535365,c_varbinarym=0x123123123123 where no=2;
delete from ora.i2_binary002 where  no=3;
go

create table ora.i2_time0021 (
    no                   int primary key IDENTITY(1,1),
    c_date               date,
	c_time				 time(5),
	c_datetime			 datetime,
	c_datetime2			 datetime2(6),
    c_timestamp          timestamp,
	c_smalldatetime		 smalldatetime,
	c_datetimeoffset	 datetimeoffset(0)
);
insert into ora.i2_time0021 values
(
convert(date,SYSDATETIME()),
convert(time,'10:01:16.001'),
convert(datetime,'1999-12-12 14:31:56.003',20),
convert(datetime2,'2048-06-01 20:51:24.00099',21),
default,
convert(smalldatetime,'2000-12-05 12:01:25',20),
CONVERT (datetimeoffset,'2021-01-13 16:50:33.0191327 +08:00')
);
insert into ora.i2_time0021 values
(
convert(date,'2020-08-31'),
convert(time,SYSDATETIME()),
convert(datetime,'1998-12-12 14:31:56.013',20),
convert(datetime2,'2047-06-01 20:51:24.00199',21),
default,
convert(smalldatetime,'2001-12-05 12:02:20',20),
CONVERT (datetimeoffset,SYSDATETIMEOFFSET())
);
insert into ora.i2_time0021 values
(
convert(date,SYSDATETIME()),
convert(time,SYSDATETIME()),
convert(datetime,'1997-12-12 14:31:56.333',20),
convert(datetime2,'2046-06-01 20:51:24.99999',21),
default,
convert(smalldatetime,'2002-12-05 12:00:00',20),
CONVERT (datetimeoffset,SYSDATETIMEOFFSET())
);
insert into ora.i2_time0021(c_datetimeoffset) values (CONVERT (datetimeoffset,SYSDATETIMEOFFSET()));
update ora.i2_time0021 set c_date=CONVERT (date,'2020-12-31') where no=2;
update ora.i2_time0021 set c_datetime=CONVERT (datetime,'2021-06-11 10:11:14.013') where no=1;
delete from ora.i2_time0021 where no =2
go

create table ora.i2_no002 (
    no             int IDENTITY(1,1),
    c_int          int default 0 not null,
	c_tinyint	   tinyint,
	c_smallint	   smallint not null,
	c_bigint	   bigint default 50000 not null,
	c_decimal	   decimal(15,10),
	c_numeric	   numeric(10,4),
    c_float        float,
    c_real         real,
    c_money        money,
    c_smallmoney   smallmoney,
	c_bit		   bit,
	constraint pk_idno002 primary key(no)
);

insert into ora.i2_no002 values (-2147483648,0,-32768,-9223372036854775808,-0.000000000000008,-176543.23000000098000008,-300.5670012,-109.1116789,-922337203685477.5808,-214748.3648,1);
insert into ora.i2_no002 values (2147483647,255,-2990,9223372036854775807,0.0000000054,276543.2398,300,208.6789,922337203685477.5807,214748.3647,0);
insert into ora.i2_no002 values (300,55,30000,50003,654.54,876543.2398,300.45,500.6789,100.23,300.45,1);
insert into ora.i2_no002(c_int,c_smallint,c_bigint) values (400,10004,50004);
update ora.i2_no002 set c_numeric=33333.8899,c_decimal=0.0000000002,c_float=0.000012,c_real=-0.0098,c_money=0,c_smallmoney=0 where no=2;
update ora.i2_no002 set c_real=100.89,c_bit=1 where no=4;
update ora.i2_no002 set c_int=-1000,c_tinyint=9,c_smallint=12345,c_bigint=-27999,c_bit=0 where no=1;
delete from ora.i2_no002 where no=3;
go

CREATE TABLE ora.i2_geome002 (
    no                  int primary key,
    c_geometry          geometry,
    c_geography         geography,
    c_smallm             smallmoney
);
insert into ora.i2_geome002 values (1,geometry::STGeomFromText('LINESTRING (100 100, 20 180, 180 180)',0),geography :: STGeomFromText('POINT(55.9271035250276 -3.29431266523898)',4326),11.0000);
insert into ora.i2_geome002 values (2,geometry::STGeomFromText('POLYGON ((0 0, 150 0, 150 150, 0 150, 0 0))',0),geography::STGeomFromText('POLYGON((47 -22, 77 72, -47 22, 47 -22))', 4326),null);
insert into ora.i2_geome002 values (3,geometry :: STGeomFromText('POLYGON((-122.358 47.653, -122.348 47.649, -122.348 47.658, -122.358 47.658, -122.358 47.653))',4326),geography::STGeomFromText('LINESTRING( -122.360 47.656, -122.343 47.656)', 4326),123.0099);
update ora.i2_geome002 set c_geography=geography::STGeomFromText('COMPOUNDCURVE (CIRCULARSTRING (-122.358 47.653, -122.348 47.649, -122.348 47.658), CIRCULARSTRING(-122.348 47.658, -122.358 47.658, -122.358 47.653))',4326) where no=2;
delete from ora.i2_geome002 where  no=3;
go

CREATE TABLE ora.i2_xml002 (no int primary key, c_xml xml);
INSERT INTO ora.i2_xml002 VALUES(1,'<Root>
<ProductDescription ProductID="1" ProductName="Road Bike">
<Features>
<Warranty>1 year parts and labor</Warranty>
<Maintenance>3 year parts and labor extended maintenance is available</Maintenance>
</Features>
</ProductDescription>
</Root>');
INSERT INTO ora.i2_xml002 VALUES(2,'<Root>
<ProductDescription ProductID="1" ProductName="yingfang active">
<Features>
<Warranty>1 year parts and labor</Warranty>
<Maintenance>3 year parts and labor extended maintenance is available</Maintenance>
</Features>
</ProductDescription>
</Root>');
INSERT INTO ora.i2_xml002 VALUES(3,'<Root>
<ProductDescription ProductID="1" ProductName="yf stream">
<Features>
<Warranty>1 year parts and labor</Warranty>
<Maintenance>3 year parts and labor extended maintenance is available</Maintenance>
</Features>
</ProductDescription>
</Root>');
insert into ora.i2_xml002(no,c_xml) values(4,'<Root>
<ProductDescription ProductID="1" ProductName="yf coopy">
<Features>
<Warranty>1 year parts and labor</Warranty>
<Maintenance>3 year parts and labor extended maintenance is available</Maintenance>
</Features>
</ProductDescription>
</Root>');
update ora.i2_xml002 set c_xml='<Root>
<Location LocationID="10"
LaborHours="1.1"
MachineHours=".2" >Manufacturing steps are described here.
<step>Manufacturing step 1 at this work center</step>
<step>Manufacturing step 2 at this work center</step>
</Location>
</Root>' where no=1;
delete from ora.i2_xml002 where no =1;
go

CREATE TABLE ora.i2_unique002
(no int primary key,
c_unique UNIQUEIDENTIFIER DEFAULT newID(),
c_varchar VARCHAR(20) );
INSERT INTO ora.i2_unique002 VALUES (1,newID(), 'def');
INSERT INTO ora.i2_unique002 VALUES (2,newID(), 'ying');
INSERT INTO ora.i2_unique002 VALUES (3,newID(), 'fang');
INSERT INTO ora.i2_unique002(no,c_varchar) VALUES (4,'abc');
update ora.i2_unique002 set c_unique='6F9619FF-8B86-D011-B42D-00C04FC964FF' where no=1;
delete from ora.i2_unique002 where  no=3;
go

CREATE TABLE ora.i2_clr002
(
    no                  int primary key,
	c_hierarchyid       hierarchyid,
	c_h1                hierarchyid
);
insert into ora.i2_clr002 values (1,'/1/2/3/','/1/2/6/');
insert into ora.i2_clr002 values (2,'/1/2/4/','/1/2/2/');
insert into ora.i2_clr002 values (3,'/1/1/2/','/1/2/5/');
insert into ora.i2_clr002(no,c_hierarchyid) values(4,'/1/1/1/');
update ora.i2_clr002 set c_h1='/-1/-2.3/-3.-999/' where no=2;
delete from ora.i2_clr002 where  no=3;
go

create table ora.i2_variant002 (
	no int primary key,
	c_unique     sql_variant DEFAULT newID(),
    c_char20     sql_variant default 'def',
    c_varchar    sql_variant not null,
	c_nchar50    sql_variant,
    c_nvarchar   sql_variant,
    c_int        sql_variant default 0 not null,
	c_smallint   sql_variant,
	c_tinyint    sql_variant check (c_tinyint>=30 and c_tinyint<=180),
	c_bigint     sql_variant,
	c_decimal    sql_variant,
	c_numeric	 sql_variant,
	c_float      sql_variant,
	c_real       sql_variant,
	c_money      sql_variant,
	c_smallmoney sql_variant,
	c_bit        sql_variant,
	c_date       sql_variant,
	c_time       sql_variant,
	c_datetime	 sql_variant,
	c_datetime2  sql_variant,
	c_smalldatetime  sql_variant,
	c_datetimeoffset sql_variant,
	c_binary     sql_variant,
	c_varbinary  sql_variant,
	c_hierarchyid  sql_variant
	);
insert into ora.i2_variant002 values (1,newid(),'fang56789','AA101','GHjjjjjjjjj','1Ϻ1Ӣ111ɷ1111޹1˾111111111',10,9999,81,67891,2020.88,500.6789,300.45,500.6789,100.23,300.45,1,
convert(date,SYSDATETIME()),
convert(time,SYSDATETIME()),
convert(datetime,'1999-12-12 14:31:56.333',20),
convert(datetime2,'2048-06-01 20:51:24.99999',21),
convert(smalldatetime,'2000-12-05 12:00:00',20),
CONVERT (datetimeoffset,SYSDATETIMEOFFSET()),
0x1f,0x111111111,'/1/2/3/');
insert into ora.i2_variant002 values (2,newid(),'yuan56789','AA102','GHjjjjjjjjj','1Ϻ1Ӣ111ɷ1111޹1˾111111112',10,9999,91,67891,2020.88,500.6789,300.45,500.6789,100.23,300.45,1,
convert(date,SYSDATETIME()),
convert(time,SYSDATETIME()),
convert(datetime,'1999-12-12 14:31:56.333',20),
convert(datetime2,'2048-06-01 20:51:24.99999',21),
convert(smalldatetime,'2000-12-05 12:00:00',20),
CONVERT (datetimeoffset,SYSDATETIMEOFFSET()),
0x2f,0x111111111,'/1/2/3/');
insert into ora.i2_variant002 values (3,newid(),'chang56789','AA103','GHjjjjjjjjj','1Ϻ1Ӣ111ɷ1111޹1˾111111114',10,9999,101,67891,2020.88,500.6789,300.45,500.6789,100.23,300.45,1,
convert(date,SYSDATETIME()),
convert(time,SYSDATETIME()),
convert(datetime,'1999-12-12 14:31:56.333',20),
convert(datetime2,'2048-06-01 20:51:24.99999',21),
convert(smalldatetime,'2000-12-05 12:00:00',20),
CONVERT (datetimeoffset,SYSDATETIMEOFFSET()),
0x3f,0x111111111,'/1/2/3/');
insert into ora.i2_variant002(no,c_varchar,c_tinyint) values ( 4,'AA104',111);
update ora.i2_variant002 set c_float=201.34,c_datetime='2019-01-28 10:10:10.333',c_hierarchyid='/1/4/6/',c_binary=0x7f,c_bigint=56789 where no=2;
delete from ora.i2_variant002 where  no=3;
go

create table ora.i2_text002 (
    no           int primary key,
	c_text		 text,
	c_ntext		 ntext,
	c_image      image
);
insert into ora.i2_text002 values (1,'012345679809909089','1111һ1111111111INFO',0x18F6D0D51D96A9664FC1491174A9DAFD67C8B0D2571EB1ABF7EDEC2CCB508EEAA4D7163B4);
insert into ora.i2_text002 values (2,'lkjdkgljasjhjjklgs','1111һ1111111111INFO',0x28F6D0D51D96A9664FC1491174A9DAFD67C8B0D2571EB1ABF7EDEC2CCB508EEAA4D7163B4);
insert into ora.i2_text002 values (3,'kljlkjlkjkloiuiyio','1111һ1111111111INFO',0x38F6D0D51D96A9664FC1491174A9DAFD67C8B0D2571EB1ABF7EDEC2CCB508EEAA4D7163B4);
insert into ora.i2_text002(no,c_ntext) values(4,'zzzzzzzzzzzz');
insert into ora.i2_text002 values (5,'','',0x38F6D0D51D96A9664FC1491174A9DAFD67C8B0D2571EB1ABF7EDEC2CCB508EEAA4D7163B4);
update ora.i2_text002 set c_text='yingfang test case' where no=1;
delete from ora.i2_text002 where  no=3;
go

create table ora.i2_sysname002 (no int primary key,c_s1 sysname,c_n1 nvarchar(128) not null);
insert into ora.i2_sysname002 values (1,N'test sysname1111A','1');
insert into ora.i2_sysname002 values (2,N'test sysname1111AB','1');
insert into ora.i2_sysname002 values (3,N'test sysname1111ABC','1');
insert into ora.i2_sysname002(no,c_s1,c_n1) values (4,N'test sysname1111ABCD','1');
update ora.i2_sysname002 set c_s1=N'test sysname1111FF' where no=2;
delete from ora.i2_sysname002 where  no=3;
go

create table mill.i2_char004 (
    no           int IDENTITY(1,1),
    c_char       char(5) not null,
    c_nchar      nchar(10) default 'def',
    c_varchar    varchar(20) not null,
    c_nvarchar   nvarchar(3000),
	constraint pk_nochar005 primary key(no),
	constraint qu_vcchar005 unique(c_char)
);
insert into mill.i2_char004 values ('A','56789','AA101','1111ABC');
insert into mill.i2_char004 values ('B','45678','AA102','1111DEF');
insert into mill.i2_char004 values ('C','45678','AB103','1Ϻ1Ӣ111ɷ1');
insert into mill.i2_char004(c_char,c_varchar) values ('D','AB104');
update mill.i2_char004 set c_char='66666' where no = 2
delete from mill.i2_char004 where  c_char='d';
go

create table mill.i2_time0021 (
    no                   int primary key IDENTITY(1,1),
    c_date               date,
	c_time				 time(5),
	c_datetime			 datetime,
	c_datetime2			 datetime2(6),
    c_timestamp          timestamp,
	c_smalldatetime		 smalldatetime,
	c_datetimeoffset	 datetimeoffset(0)
);
insert into mill.i2_time0021 values
(
convert(date,SYSDATETIME()),
convert(time,'10:01:16.001'),
convert(datetime,'1999-12-12 14:31:56.003',20),
convert(datetime2,'2048-06-01 20:51:24.00099',21),
default,
convert(smalldatetime,'2000-12-05 12:01:25',20),
CONVERT (datetimeoffset,'2021-01-13 16:50:33.0191327 +08:00')
);
insert into mill.i2_time0021 values
(
convert(date,'2020-08-31'),
convert(time,SYSDATETIME()),
convert(datetime,'1998-12-12 14:31:56.013',20),
convert(datetime2,'2047-06-01 20:51:24.00199',21),
default,
convert(smalldatetime,'2001-12-05 12:02:20',20),
CONVERT (datetimeoffset,SYSDATETIMEOFFSET())
);
insert into mill.i2_time0021 values
(
convert(date,SYSDATETIME()),
convert(time,SYSDATETIME()),
convert(datetime,'1997-12-12 14:31:56.333',20),
convert(datetime2,'2046-06-01 20:51:24.99999',21),
default,
convert(smalldatetime,'2002-12-05 12:00:00',20),
CONVERT (datetimeoffset,SYSDATETIMEOFFSET())
);
insert into mill.i2_time0021(c_datetimeoffset) values (CONVERT (datetimeoffset,SYSDATETIMEOFFSET()));
update mill.i2_time0021 set c_date=CONVERT (date,'2020-12-31') where no=2;
update mill.i2_time0021 set c_datetime=CONVERT (datetime,'2021-06-11 10:11:14.013') where no=1;
delete from mill.i2_time0021 where no =2
go

create table mill.i2_no002 (
    no             int IDENTITY(1,1),
    c_int          int default 0 not null,
	c_tinyint	   tinyint,
	c_smallint	   smallint not null,
	c_bigint	   bigint default 50000 not null,
	c_decimal	   decimal(15,10),
	c_numeric	   numeric(10,4),
    c_float        float,
    c_real         real,
    c_money        money,
    c_smallmoney   smallmoney,
	c_bit		   bit,
	constraint pk_idno002 primary key(no)
);

insert into mill.i2_no002 values (-2147483648,0,-32768,-9223372036854775808,-0.000000000000008,-176543.23000000098000008,-300.5670012,-109.1116789,-922337203685477.5808,-214748.3648,1);
insert into mill.i2_no002 values (2147483647,255,-2990,9223372036854775807,0.0000000054,276543.2398,300,208.6789,922337203685477.5807,214748.3647,0);
insert into mill.i2_no002 values (300,55,30000,50003,654.54,876543.2398,300.45,500.6789,100.23,300.45,1);
insert into mill.i2_no002(c_int,c_smallint,c_bigint) values (400,10004,50004);
update mill.i2_no002 set c_numeric=33333.8899,c_decimal=0.0000000002,c_float=0.000012,c_real=-0.0098,c_money=0,c_smallmoney=0 where no=2;
update mill.i2_no002 set c_real=100.89,c_bit=1 where no=4;
update mill.i2_no002 set c_int=-1000,c_tinyint=9,c_smallint=12345,c_bigint=-27999,c_bit=0 where no=1;
delete from mill.i2_no002 where no=3;
go

CREATE TABLE mill.i2_geome002 (
    no                  int primary key,
    c_geometry          geometry,
    c_geography         geography,
    c_smallm             smallmoney
);
insert into mill.i2_geome002 values (1,geometry::STGeomFromText('LINESTRING (100 100, 20 180, 180 180)',0),geography :: STGeomFromText('POINT(55.9271035250276 -3.29431266523898)',4326),11.0000);
insert into mill.i2_geome002 values (2,geometry::STGeomFromText('POLYGON ((0 0, 150 0, 150 150, 0 150, 0 0))',0),geography::STGeomFromText('POLYGON((47 -22, 77 72, -47 22, 47 -22))', 4326),null);
insert into mill.i2_geome002 values (3,geometry :: STGeomFromText('POLYGON((-122.358 47.653, -122.348 47.649, -122.348 47.658, -122.358 47.658, -122.358 47.653))',4326),geography::STGeomFromText('LINESTRING( -122.360 47.656, -122.343 47.656)', 4326),123.0099);
update mill.i2_geome002 set c_geography=geography::STGeomFromText('COMPOUNDCURVE (CIRCULARSTRING (-122.358 47.653, -122.348 47.649, -122.348 47.658), CIRCULARSTRING(-122.348 47.658, -122.358 47.658, -122.358 47.653))',4326) where no=2;
delete from mill.i2_geome002 where  no=3;
go

CREATE TABLE mill.i2_xml002 (no int primary key, c_xml xml);
INSERT INTO mill.i2_xml002 VALUES(1,'<Root>
<ProductDescription ProductID="1" ProductName="Road Bike">
<Features>
<Warranty>1 year parts and labor</Warranty>
<Maintenance>3 year parts and labor extended maintenance is available</Maintenance>
</Features>
</ProductDescription>
</Root>');
INSERT INTO mill.i2_xml002 VALUES(2,'<Root>
<ProductDescription ProductID="1" ProductName="yingfang active">
<Features>
<Warranty>1 year parts and labor</Warranty>
<Maintenance>3 year parts and labor extended maintenance is available</Maintenance>
</Features>
</ProductDescription>
</Root>');
INSERT INTO mill.i2_xml002 VALUES(3,'<Root>
<ProductDescription ProductID="1" ProductName="yf stream">
<Features>
<Warranty>1 year parts and labor</Warranty>
<Maintenance>3 year parts and labor extended maintenance is available</Maintenance>
</Features>
</ProductDescription>
</Root>');
insert into mill.i2_xml002(no,c_xml) values(4,'<Root>
<ProductDescription ProductID="1" ProductName="yf coopy">
<Features>
<Warranty>1 year parts and labor</Warranty>
<Maintenance>3 year parts and labor extended maintenance is available</Maintenance>
</Features>
</ProductDescription>
</Root>');
update mill.i2_xml002 set c_xml='<Root>
<Location LocationID="10"
LaborHours="1.1"
MachineHours=".2" >Manufacturing steps are described here.
<step>Manufacturing step 1 at this work center</step>
<step>Manufacturing step 2 at this work center</step>
</Location>
</Root>' where no=1;
delete from mill.i2_xml002 where no =1;
go

CREATE TABLE mill.i2_unique002
(no int primary key,
c_unique UNIQUEIDENTIFIER DEFAULT newID(),
c_varchar VARCHAR(20) );
INSERT INTO mill.i2_unique002 VALUES (1,newID(), 'def');
INSERT INTO mill.i2_unique002 VALUES (2,newID(), 'ying');
INSERT INTO mill.i2_unique002 VALUES (3,newID(), 'fang');
INSERT INTO mill.i2_unique002(no,c_varchar) VALUES (4,'abc');
update mill.i2_unique002 set c_unique='6F9619FF-8B86-D011-B42D-00C04FC964FF' where no=1;
delete from mill.i2_unique002 where  no=3;
go

CREATE TABLE mill.i2_clr002
(
    no                  int primary key,
	c_hierarchyid       hierarchyid,
	c_h1                hierarchyid
);
insert into mill.i2_clr002 values (1,'/1/2/3/','/1/2/6/');
insert into mill.i2_clr002 values (2,'/1/2/4/','/1/2/2/');
insert into mill.i2_clr002 values (3,'/1/1/2/','/1/2/5/');
insert into mill.i2_clr002(no,c_hierarchyid) values(4,'/1/1/1/');
update mill.i2_clr002 set c_h1='/-1/-2.3/-3.-999/' where no=2;
delete from mill.i2_clr002 where  no=3;
go

create table mill.i2_variant002 (
	no int primary key,
	c_unique     sql_variant DEFAULT newID(),
    c_char20     sql_variant default 'def',
    c_varchar    sql_variant not null,
	c_nchar50    sql_variant,
    c_nvarchar   sql_variant,
    c_int        sql_variant default 0 not null,
	c_smallint   sql_variant,
	c_tinyint    sql_variant check (c_tinyint>=30 and c_tinyint<=180),
	c_bigint     sql_variant,
	c_decimal    sql_variant,
	c_numeric	 sql_variant,
	c_float      sql_variant,
	c_real       sql_variant,
	c_money      sql_variant,
	c_smallmoney sql_variant,
	c_bit        sql_variant,
	c_date       sql_variant,
	c_time       sql_variant,
	c_datetime	 sql_variant,
	c_datetime2  sql_variant,
	c_smalldatetime  sql_variant,
	c_datetimeoffset sql_variant,
	c_binary     sql_variant,
	c_varbinary  sql_variant,
	c_hierarchyid  sql_variant
	);
insert into mill.i2_variant002 values (1,newid(),'fang56789','AA101','GHjjjjjjjjj','1Ϻ1Ӣ111ɷ1111޹1˾111111111',10,9999,81,67891,2020.88,500.6789,300.45,500.6789,100.23,300.45,1,
convert(date,SYSDATETIME()),
convert(time,SYSDATETIME()),
convert(datetime,'1999-12-12 14:31:56.333',20),
convert(datetime2,'2048-06-01 20:51:24.99999',21),
convert(smalldatetime,'2000-12-05 12:00:00',20),
CONVERT (datetimeoffset,SYSDATETIMEOFFSET()),
0x1f,0x111111111,'/1/2/3/');
insert into mill.i2_variant002 values (2,newid(),'yuan56789','AA102','GHjjjjjjjjj','1Ϻ1Ӣ111ɷ1111޹1˾111111112',10,9999,91,67891,2020.88,500.6789,300.45,500.6789,100.23,300.45,1,
convert(date,SYSDATETIME()),
convert(time,SYSDATETIME()),
convert(datetime,'1999-12-12 14:31:56.333',20),
convert(datetime2,'2048-06-01 20:51:24.99999',21),
convert(smalldatetime,'2000-12-05 12:00:00',20),
CONVERT (datetimeoffset,SYSDATETIMEOFFSET()),
0x2f,0x111111111,'/1/2/3/');
insert into mill.i2_variant002 values (3,newid(),'chang56789','AA103','GHjjjjjjjjj','1Ϻ1Ӣ111ɷ1111޹1˾111111114',10,9999,101,67891,2020.88,500.6789,300.45,500.6789,100.23,300.45,1,
convert(date,SYSDATETIME()),
convert(time,SYSDATETIME()),
convert(datetime,'1999-12-12 14:31:56.333',20),
convert(datetime2,'2048-06-01 20:51:24.99999',21),
convert(smalldatetime,'2000-12-05 12:00:00',20),
CONVERT (datetimeoffset,SYSDATETIMEOFFSET()),
0x3f,0x111111111,'/1/2/3/');
insert into mill.i2_variant002(no,c_varchar,c_tinyint) values ( 4,'AA104',111);
update mill.i2_variant002 set c_float=201.34,c_datetime='2019-01-28 10:10:10.333',c_hierarchyid='/1/4/6/',c_binary=0x7f,c_bigint=56789 where no=2;
delete from mill.i2_variant002 where  no=3;
go

create table mill.i2_text002 (
    no           int primary key,
	c_text		 text,
	c_ntext		 ntext,
	c_image      image
);
insert into mill.i2_text002 values (1,'012345679809909089','1111һ1111111111INFO',0x18F6D0D51D96A9664FC1491174A9DAFD67C8B0D2571EB1ABF7EDEC2CCB508EEAA4D7163B4);
insert into mill.i2_text002 values (2,'lkjdkgljasjhjjklgs','1111һ1111111111INFO',0x28F6D0D51D96A9664FC1491174A9DAFD67C8B0D2571EB1ABF7EDEC2CCB508EEAA4D7163B4);
insert into mill.i2_text002 values (3,'kljlkjlkjkloiuiyio','1111һ1111111111INFO',0x38F6D0D51D96A9664FC1491174A9DAFD67C8B0D2571EB1ABF7EDEC2CCB508EEAA4D7163B4);
insert into mill.i2_text002(no,c_ntext) values(4,'zzzzzzzzzzzz');
insert into mill.i2_text002 values (5,'','',0x38F6D0D51D96A9664FC1491174A9DAFD67C8B0D2571EB1ABF7EDEC2CCB508EEAA4D7163B4);
update mill.i2_text002 set c_text='yingfang test case' where no=1;
delete from mill.i2_text002 where  no=3;
go

create table mill.i2_sysname002 (no int primary key,c_s1 sysname,c_n1 nvarchar(128) not null);
insert into mill.i2_sysname002 values (1,N'test sysname1111A','1');
insert into mill.i2_sysname002 values (2,N'test sysname1111AB','1');
insert into mill.i2_sysname002 values (3,N'test sysname1111ABC','1');
insert into mill.i2_sysname002(no,c_s1,c_n1) values (4,N'test sysname1111ABCD','1');
update mill.i2_sysname002 set c_s1=N'test sysname1111FF' where no=2;
delete from mill.i2_sysname002 where  no=3;
go

tgt_sql: (1)
    go