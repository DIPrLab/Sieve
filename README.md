# Sieve 
## A Middleware Approach to Scalable Access Control for Database Management Systems

![Sieve Logo](images/logo.png)

### Intro
SIEVE is a general purpose middleware to support access control in DBMS that enables them to scale query processing with very large number of access control policies. Full version of the paper can be seen at [arXiv](https://arxiv.org/abs/2004.07498). 


### Setup 

#### MySQL

1. Download [MySQL](https://dev.mysql.com/downloads/installer/).
2. Optionally, download the GUI extension for MySQL - [MySQL Workbench](https://dev.mysql.com/downloads/workbench/) -- (Inefficient for Linux Users)
3. Note your user and localhost connection. (E.g. user = root, server = localhost:3306).
4. Create a schema and name the schema 'sieve'.
5. Create a new user, name the user 'sieve' and grant that user administrative privileges over the database.
6. When prompted on option to load a script, skip, and select apply. You will now see sieve in your Schemas.
7. On your machine, unzip the file data/wifi_dataset.tar.xz twice (once for .xz and once for .tar).
8. Open the SQL Script `data/wifi_dataset/wifi_defs.sql.` > execute the script.
9. Repeat step 8 for the file `data/wifi_dataset/wifi_data`.
10. If the code needs to be run with caching or workload: Execute the following script to update the definitions for the wifi dataset: 
```angular2html
ALTER TABLE sieve.user_policy change `policy_id` `policy_id` int(10) auto_increment;
ALTER TABLE sieve.user_policy_object_condition change `id` `id` int(10) auto_increment;
ALTER TABLE sieve.user_guard_expression modify column purpose varchar(50);
ALTER TABLE sieve.queries MODIFY selectivity float NULL;
ALTER TABLE sieve.queries MODIFY selectivity_type varchar(64) NULL;
```

#### PostgreSQL

1. Download [PostgreSQL](https://www.enterprisedb.com/downloads/postgres-postgresql-downloads).
2. Create a user called 'sieve' and grant all administrative privileges of a DBA role.
3. Create a new Schema called 'sieve'.
4. On your machine, unzip the file data/mall_dataset.tar.xz twice (once for .xz and once for .tar).
5. After obtaining 'mall_defs' and 'mall_data' as a SQL dump file, execute the script for ```data/mall_dataset/mall_defs``` followed by the script for ```data/mall_dataset/mall_data```.
6. Optionally, download the GUI extension for PostgreSQL - [PgAdmin](https://www.pgadmin.org/download/). -- (Inefficient for Linux Users)
7. In pgAdmin, navigate and right click on the ‘Login/Group Role’ in the Object Explorer on the left. Select Create > Login/Group Role and name the role ‘sieve’.
   1. Navigate to 'Definition' and set a password for the sieve user.
   2. Navigate to 'Privileges' and grant user access to be a superuser.
   3. More information about privileges can be found [here](https://www.sqlshack.com/postgresql-tutorial-to-create-a-user/) (SQLShack).
   4. Select Save.
8. Right click on PostgreSQL <version #> > Create.. > Database. 
   1. Name the database ‘sieve’. 
   2. Navigate to Security > Navigate to Privileges > Add Grantor > Add user ‘sieve’.
   3. Select Save.
9. Right click on the sieve database > Create.. > Schema… Name the schema ‘sieve’.
10. On your machine, unzip the file data/mall_dataset.tar.xz twice (once for .xz and once for .tar).
11. In Object Explorer > Databases, right click on 'sieve', navigate to the Query Tool, and import the file `data/mall_dataset/mall_defs.sql.`
12. Press F5 to execute the script or navigate to the execute button on the top middle of the screen.
13. Repeat Step 7 and 8 for the file `data/mall_dataset/mall_data`.


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

