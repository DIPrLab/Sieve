# Sieve 
## A Middleware Approach to Scalable Access Control for Database Management Systems

![Sieve Logo](images/logo.png)

### Intro
SIEVE is a general purpose middleware to support access control in DBMS that enables them to scale query processing with very large number of access control policies. Full version of the paper can be seen at [arXiv](https://arxiv.org/abs/2004.07498). 


### Setup 

#### MySQL

1. Download [MySQL](https://dev.mysql.com/downloads/installer/).
2. Optionally, download the GUI extension for MySQL - [MySQL Workbench](https://dev.mysql.com/downloads/workbench/)
3. In MySQL Workbench, navigate to your server and note your user and localhost connection. (E.g. user = root, server = localhost:3306).
4. In the 'Schemas' section, right click > Create Schema...
5. Name the schema as 'sieve' > Select Apply.
6. When prompted on option to load a script, skip, and select apply. You will now see sieve in your Schemas.
7. On your machine, unzip the file data/wifi_dataset.tar.xz twice (once for .xz and once for .tar).
8. On the top left corner, navigate to File > Open SQL Script... > Browse for `data/wifi_dataset/wifi_defs.sql.` > Click on the lightning icon to execute script.
9. Repeat step 8 for the file `data/wifi_dataset/wifi_data`.
10. Execute the following script to update the definitions for the wifi dataset: 
```angular2html
alter table sieve.user_policy change `policy_id` `policy_id` int(10) auto_increment;
alter table sieve.user_policy_object_condition change `id` `id` int(10) auto_increment;
alter table sieve.user_guard_expression modify column purpose varchar(50);
ALTER TABLE sieve.queries MODIFY selectivity float NULL;
ALTER TABLE sieve.queries MODIFY selectivity_type varchar(64) NULL;
```

#### PostgreSQL

1. Download [PostgreSQL](https://www.enterprisedb.com/downloads/postgres-postgresql-downloads).
2. Optionally, download the GUI extension for PostgreSQL - [PgAdmin](https://www.pgadmin.org/download/).
3. In pgAdmin, navigate and right click on the ‘Login/Group Role’ in the Object Explorer on the left. Select Create > Login/Group Role and name the role ‘sieve’.
   1. Navigate to 'Definition' and set a password for the sieve user.
   2. Navigate to 'Privileges' and grant user access to be a superuser.
   3. More information about privileges can be found [here](https://www.sqlshack.com/postgresql-tutorial-to-create-a-user/) (SQLShack).
   4. Select Save.
4. Right click on PostgreSQL <version #> > Create.. > Database. 
   1. Name the database ‘sieve’. 
   2. Navigate to Security > Navigate to Privileges > Add Grantor > Add user ‘sieve’.
   3. Select Save.
5. Right click on the sieve database > Create.. > Schema… Name the schema ‘sieve’.
6. On your machine, unzip the file data/mall_dataset.tar.xz twice (once for .xz and once for .tar).
7. In Object Explorer > Databases, right click on 'sieve', navigate to the Query Tool, and import the file `data/mall_dataset/mall_defs.sql.`
8. Press F5 to execute the script or navigate to the execute button on the top middle of the screen.
9. Repeat Step 7 and 8 for the file `data/mall_dataset/mall_data`.
10. Execute the following script to update the definitions for the mall dataset:
```angular2html
alter table sieve.user_policy change `policy_id` `policy_id` int(10) auto_increment;
alter table sieve.user_policy_object_condition change `id` `id` int(10) auto_increment;
alter table sieve.user_guard_expression modify column purpose varchar(50);
ALTER TABLE sieve.queries MODIFY selectivity float NULL;
ALTER TABLE sieve.queries MODIFY selectivity_type varchar(64) NULL;
```

### Usage

1. Install any java code editor (maven extension required) or [IntelliJ](https://www.jetbrains.com/idea/download/?section=windows) (comes with inbuilt maven requirements)
2. Open the sieve project.
3. Set the dbms and table_name options in `src/resources/config/general.properties`
   1. If dbms = mysql, table_name = PRESENCE
   2. If dbms = postgres, table_name = MALL_OBSERVATION
4. Set true for the experiments that you wish to run (Options: Query Performance, Policy Scale up)
5. Compile the code
```
mvn clean install

```
6. Execute it with
```
mvn exec:java 
```
7. Before running the code, execute this script to delete the data and reload it with the applicable policies:
```angular2html
truncate table sieve.USER_POLICY;
truncate table sieve.USER_POLICY_OBJECT_CONDITION;
truncate table sieve.USER_GUARD_EXPRESSION;
truncate table sieve.USER_GUARD_PARTS;
truncate table sieve.USER_GUARD_TO_POLICY;
```

## License
[Apache 2.0](https://choosealicense.com/licenses/apache-2.0/)

