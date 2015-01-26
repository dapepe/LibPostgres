<?php

require_once 'TestInit.php';

class SelectTest extends PHPUnit_Framework_TestCase
{

    public function testSelectGeneral() {

        $oDB = new \LibPostgres\LibPostgresDriver(array(
            'host' => getenv('TEST_HOST') ? : 'localhost',
            'port' => getenv('TEST_PORT') ? : 5432,
            'user_name' => getenv('TEST_USER_NAME'),
            'user_password' => getenv('TEST_PASSWORD'),
            'db_name' => getenv('TEST_DB_NAME'),
        ));

        // selectField and ?d

        $iMin = 100; $iMax = 200;
        $iResult = $oDB->selectField("
            SELECT max(t)
                FROM generate_series(?d, ?d) AS t;
        ",
            $iMin,
            $iMax
        );

        $this->assertEquals($iResult, $iMax);

        // selectRecord and ?w

        $aResult = $oDB->selectRecord("
            SELECT ?w || t::varchar AS field, md5(?w || t::varchar) AS md5
                FROM generate_series(1, 10) AS t
                ORDER BY t DESC
                LIMIT 1
        ",
            "STR'ING",
            "STR'ING"
        );

        $this->assertEquals($aResult['field'], "STR'ING10");
        $this->assertEquals($aResult['md5'], md5("STR'ING10"));

        // selectColumn and ?h

        $aColumn = $oDB->selectColumn("
            WITH hs AS (
                SELECT ?h || ('count => ' || t)::hstore AS h
                    FROM generate_series(0, 10) AS t
            )
            SELECT (h->'field')::varchar || (h->'\"foo\"')::varchar || (h->'baz')::varchar || (h->'count')::varchar
                FROM hs
                ORDER BY (h->'count')::integer;
        ",
            array(
                'field' => '"FIELD"',
                '"foo"' => '\"bar\"',
                'baz' => "\n\t",
                'normal' => str_repeat('1', 100),
            )
        );

        $this->assertEquals($aColumn[3], '"FIELD"\"bar\"' . "\n\t" . '3');


    }

}
