# Sieve 
## A Middleware Approach to Scalable Access Control for Database Management Systems

![Sieve Logo](images/logo.png)

### Intro
SIEVE is a general purpose middleware to support access control in DBMS that enables them to scale query processing with very large number of access control policies. Full version of the paper can be seen at [arXiv](https://arxiv.org/abs/2004.07498). 


### Setup 

#### MySQL

1. Download [MySQL](https://dev.mysql.com/downloads/installer/).
2. Optionally, download the GUI extension for MySQL - [MySQL Workbench](https://dev.mysql.com/downloads/workbench/)
4. Create a new Schema and name the Schema 'sieve'.
5. Create a new user called 'sieve' and grant all administrative privileges over that database to the user.
6. When prompted on option to load a script, skip, and select apply. You will now see sieve in your Schemas.
7. On your machine, unzip the file data/wifi_dataset.tar.xz twice (once for .xz and once for .tar).
8. Open the SQL Script `data/wifi_dataset/wifi_defs.sql.` > Execute the script to load the schema definitions.
9. Repeat step 8 for the file `data/wifi_dataset/wifi_data`.
10. Execute the following script to update the definitions for the wifi dataset if the code is to be run with caching or workload: 
```angular2html
alter table sieve.user_policy change `policy_id` `policy_id` int(10) auto_increment;
alter table sieve.user_policy_object_condition change `id` `id` int(10) auto_increment;
alter table sieve.user_guard_expression modify column purpose varchar(50);
alter table sieve.queries MODIFY selectivity float NULL;
alter table sieve.queries MODIFY selectivity_type varchar(64) NULL;
```

#### PostgreSQL 

1. Download [PostgreSQL](https://www.enterprisedb.com/downloads/postgres-postgresql-downloads).
2. Optionally, download the GUI extension for PostgreSQL - [PgAdmin](https://www.pgadmin.org/download/).
3. Create a Login/Group Role and name the role ‘sieve’.
   1. Navigate to 'Definition' and set a password for the sieve user.
   2. Navigate to 'Privileges' and grant user access to be a superuser.
   3. More information about privileges can be found [here](https://www.sqlshack.com/postgresql-tutorial-to-create-a-user/) (SQLShack).
   4. Select Save.
4. For psql users, create a new user called 'sieve' and grant all DBA role privileges. 
5. Create a new database and name the database 'sieve'. Add the 'sieve' user as the owner of this database.
6. For psql users, create a new database called 'sieve', and add the user 'sieve' to this new database.
7. Create a new schema and name the schema 'sieve'.
8. On your machine, unzip the file data/mall_dataset.tar.xz twice (once for .xz and once for .tar).
9. Import the file `data/mall_dataset/mall_defs.sql.` Execute this script.
10. Repeat Step 9 for the file `data/mall_dataset/mall_data`.


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

## License
[Apache 2.0](https://choosealicense.com/licenses/apache-2.0/)

