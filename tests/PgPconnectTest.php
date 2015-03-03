<?php

require_once 'TestInit.php';

class PgPconnectTest extends PHPUnit_Framework_TestCase
{

    public function testPgPconnectGeneral() {

        // pg_pconnect

        $oDB1 = new \LibPostgres\LibPostgresDriver(array(
            'host' => getenv('TEST_HOST') ? : 'localhost',
            'port' => getenv('TEST_PORT') ? : 5432,
            'user_name' => getenv('TEST_USER_NAME'),
            'user_password' => getenv('TEST_PASSWORD'),
            'db_name' => getenv('TEST_DB_NAME'),
            'persistance' => 1,
        ));

        $iMin = 105; $iMax = 201;
        $iResult = $oDB1->selectField("
            SELECT sum(t)
                FROM generate_series(?d, ?d) AS t;
        ",
            $iMin,
            $iMax
        );

        $this->assertEquals($iResult, ($iMax + $iMin) * (($iMax - $iMin + 1) / 2));

        // pg_pconnect(..., PGSQL_CONNECT_FORCE_NEW)

        $oDB2 = new \LibPostgres\LibPostgresDriver(array(
            'host' => getenv('TEST_HOST') ? : 'localhost',
            'port' => getenv('TEST_PORT') ? : 5432,
            'user_name' => getenv('TEST_USER_NAME'),
            'user_password' => getenv('TEST_PASSWORD'),
            'db_name' => getenv('TEST_DB_NAME'),
            'persistance' => PGSQL_CONNECT_FORCE_NEW,
        ));

        $iMin = 405; $iMax = 1201;
        $iResult = $oDB2->selectField("
            SELECT sum(t)
                FROM generate_series(?d, ?d) AS t;
        ",
            $iMin,
            $iMax
        );

        $this->assertEquals($iResult, ($iMax + $iMin) * (($iMax - $iMin + 1) / 2));

    }

}
