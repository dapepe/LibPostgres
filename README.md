LibPostgres
===========

Tiny wrapper over php postgresql functions (pg_connect, pg_query, etc).  
It is just wrapper, database access layer, not ORM.

##Installation

    composer require denismilovanov/libpostgres

##Sample


####Lets start with connection:

    $oDB = new \LibPostgres\LibPostgresDriver(array(
        'host' => 'localhost',
        'port' => 5432,
        'user_name' => 'test',
        'user_password' => 'test',
        'db_name' => 'test',
    ));

####Start transaction, queries, commit

    $oDB->startTransaction();
    $oDB->query("
        CREATE TABLE IF NOT EXISTS users (
            id bigserial NOT NULL,
            type_id integer NOT NULL,
            kind_id integer NOT NULL,
            name varchar(255) NOT NULL,
            birth_date date NOT NULL,
            CONSTRAINT users_pkey PRIMARY KEY (id)
        );
    ");
    $oDB->query("
        DELETE FROM users;
        ALTER SEQUENCE users_id_seq RESTART WITH 1;
    ");
    $oDB->commit();

PostgreSQL has a perfect powerful transactional engine.  
All single DML and DDL queries (if other is not specified) are performed in autocommit mode.  
But we decided to forbid autocommit in this wrapper even for single DML/DDL queries.  
So you have to explicitly start transaction and say commit (or rollback).  
(You know, "with great power, comes great responsibility".)  
Wrapper throws an exception when you try to perform query without transaction being started.  
Manual control of the transaction forces you to think a bit more about things that happen :)  

####Single query in transaction, placeholders

    $oDB->t()->query("
        INSERT INTO users
            SELECT  nextval('users_id_seq'::regclass),
                    (random() * ?d)::integer + 1,
                    (random() * ?d)::integer + 1,
                    name || ' ' || surname,
                    ('1960-01-01'::date + interval '1 day' * (random() * 1000)::integer)::date
                FROM    unnest(array[?w]) AS name,
                        unnest(array[?w]) AS surname;
    ",
        2,
        3,
        array('Ann', 'Bob', 'Christin', 'Dave', 'Eve', 'George'),
        array('Adams', 'Black', 'Cole')
    );

You may perform a single transaction - just call method named t().  
It simplifies life, but says to everyone "it is a transaction! :)"

Also you may use placeholders in your queries.
Placeholders are:
* `?d` - integer (intval used), or flat array of integers,
* `?f` - float (floatval used), or flat array of floats,
* `?w` - string, text, date, timestamp, etc, of flat array of these types (pg_escape_string used),
* `?h` - hstore (flat key-value array, it will be tranformed to `'key1 => value1, ...'::hstore`),
* `?j`, `?jb` - json or jsonb,
* `?` - simple replacement without preparations.

The values of placeholders should be passed in methods after text of query.

####Selecting single field

    $sFullName = $oDB->selectField("
        SELECT name
            FROM users
            ORDER BY birth_date DESC
            LIMIT 1;
    ");
    echo $sFullName . "\n";

My output is:

    Eve Adams

####Selecting single record

    $aUser = $oDB->selectRecord("
        SELECT id, name
            FROM users
            ORDER BY birth_date ASC
            LIMIT 1;
    ");
    echo json_encode($aUser) . "\n";

My output (note that I use json_encode) - a hash:

    {"id":"18","name":"George Cole"}

####Selecting column

    $aUsersNames = $oDB->selectColumn("
        SELECT name
            FROM users
            ORDER BY birth_date ASC
            LIMIT 3;
    ");
    echo json_encode($aUsersNames) . "\n";

My output (still json_encoded) - a flat array of requested names:

    ["George Cole","Bob Black","Dave Black"]

####Selecting indexed column

    $aUsersNamesDates = $oDB->selectIndexedColumn("
        SELECT name, birth_date
            FROM users
            ORDER BY birth_date ASC
            LIMIT 3;
    ");
    echo json_encode($aUsersNamesDates) . "\n";

First key in select will be an index, second - value, let's see at the encoded result:

    {"George Cole":"1960-01-27","Bob Black":"1960-04-08","Dave Black":"1960-04-10"}

####Selecting table

    $aUsers = $oDB->selectTable("
        SELECT id, name, birth_date
            FROM users
            ORDER BY id DESC
            LIMIT 3;
    ");
    echo json_encode($aUsers) . "\n";

It returns list of hashes:

    [{"id":"18","name":"George Cole","birth_date":"1961-05-27"},{"id":"17","name":"George Black","birth_date":"1961-04-15"},{"id":"16","name":"George Adams","birth_date":"1961-12-27"}]

####Selecting indexed table

    $aUsers = $oDB->selectIndexedTable("
        SELECT id, name, birth_date
            FROM users
            WHERE name ~ ?w
            ORDER BY birth_date ASC
    ",
        'Dave'
    );
    echo json_encode($aUsers) . "\n";

First key will be an index:

    {"12":{"id":"12","name":"Dave Cole","birth_date":"1960-08-25"},"10":{"id":"10","name":"Dave Adams","birth_date":"1960-10-29"},"11":{"id":"11","name":"Dave Black","birth_date":"1961-06-01"}}

####Selecting 2 indexed column

    $aUsers = $oDB->select2IndexedColumn("
        SELECT type_id, kind_id, name
            FROM users
            ORDER BY type_id, kind_id, name
    ");
    echo json_encode($aUsers) . "\n";

First and seconds key become indexes, third key becomes value. Query produces:

    {"1":{"1":"Bob Black","2":"Dave Black","3":"George Adams","4":"Ann Black"},"2":{"1":"George Black","2":"Christin Cole","3":"Eve Black","4":"George Cole"},"3":{"2":"Bob Adams","3":"Eve Adams"}}

Also supported `select2IndexedTable`, `select3IndexedColumn`, `select3IndexedTable`.

### Exceptions

Wrong formed query will cause an exception:

    try {
        $oDB->startTransaction();
        $oDB->query("
            INSERT ;
        ");
        $oDB->commit();
    } catch (Exception $oException) {
        echo "An exception caught: " . $oException->getMessage() . "\n";
        die;
    }

Output:

    An exception caught: ERROR:  syntax error
    LINE 2:             INSERT ;

Also exceptions are thrown in case of constraint violation, deadlocks, server problems, etc.

### Notes

* select-smth methods do not require to start transaction explicitly and they do not limit you to use DML in autocommit mode, such as `INSERT INTO ... RETURNING`,
* to use pg_pconnect instead of pg_connect pass `persistence => 1` or `persistence => PGSQL_CONNECT_FORCE_NEW` into constructor.
