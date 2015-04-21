<?php

require_once 'TestInit.php';

class NullTest extends PHPUnit_Framework_TestCase
{

    public function testNull() {

        $oDB = new \LibPostgres\LibPostgresDriver(array(
            'host' => getenv('TEST_HOST') ? : 'localhost',
            'port' => getenv('TEST_PORT') ? : 5432,
            'user_name' => getenv('TEST_USER_NAME'),
            'user_password' => getenv('TEST_PASSWORD'),
            'db_name' => getenv('TEST_DB_NAME'),
        ));

        // ?d

        $iFirst = 1;
        $iSecond = 2;

        $aResult = $oDB->selectRecord("
            SELECT  (?d IS NULL)::integer AS is_null,
                    (?d IS NOT NULL)::integer AS is_not_null,
                    (SELECT sum(COALESCE(x, 0)) FROM unnest(array[?d]::integer[]) AS x) AS sum_coalesce
        ",
            null,
            $iFirst,
            array($iFirst, null, $iSecond)
        );

        $this->assertEquals(1, $aResult['is_null']);
        $this->assertEquals(1, $aResult['is_not_null']);
        $this->assertEquals($iFirst + $iSecond, $aResult['sum_coalesce']);

        // ?f

        $fFirst = 1.1;
        $fSecond = 2.2;

        $aResult = $oDB->selectRecord("
            SELECT  (?f IS NULL)::integer AS is_null,
                    (?f IS NOT NULL)::integer AS is_not_null,
                    (SELECT sum(COALESCE(x, 0)) FROM unnest(array[?f]::numeric[]) AS x) AS sum_coalesce
        ",
            null,
            $fFirst,
            array($fFirst, null, $fSecond)
        );

        $this->assertEquals(1, $aResult['is_null']);
        $this->assertEquals(1, $aResult['is_not_null']);
        $this->assertTrue(bccomp($fFirst + $fSecond, $aResult['sum_coalesce'], 2) === 0);

        // ?j / ?jb / ?h

        $aResult = $oDB->selectRecord("
            SELECT  (?j IS NULL)::integer AS is_j_null,
                    (?jb IS NULL)::integer AS is_jb_null,
                    (?h IS NULL)::integer AS is_h_null,
                    ((?h->'i_am_null') IS NULL)::integer AS is_null_inside_hstore,
                    ((?h->'i_am_not_null') IS NOT NULL)::integer AS is_not_null_inside_hstore
        ",
            null,
            null,
            null,
            array('i_am_null' => null, 'i_am_not_null' => 1),
            array('i_am_null' => null, 'i_am_not_null' => 1)
        );

        $this->assertEquals(1, $aResult['is_j_null']);
        $this->assertEquals(1, $aResult['is_jb_null']);
        $this->assertEquals(1, $aResult['is_jb_null']);
        $this->assertEquals(1, $aResult['is_null_inside_hstore']);
        $this->assertEquals(1, $aResult['is_not_null_inside_hstore']);

        // ?w

        $sString1 = null;
        $sString2 = "2'2\r";
        $sStringDefault = "3'3\n";

        $aResult = $oDB->selectRecord("
            SELECT  (?w IS NULL)::integer AS is_null,
                    (?w IS NOT NULL)::integer AS is_not_null,
                    COALESCE(?w, ?w) || ?w AS concat,
                    (SELECT string_agg(COALESCE(s, ?w), '') FROM unnest(array[?w]::varchar[]) AS s) AS concat_coalesce

        ",
            $sString1,
            $sString2,
            $sString1, $sStringDefault, $sString2,
            $sStringDefault, array($sString1, $sString2, $sString1)
        );

        $this->assertEquals(1, $aResult['is_null']);
        $this->assertEquals(1, $aResult['is_not_null']);
        $this->assertEquals($sStringDefault . $sString2, $aResult['concat']);
        $this->assertEquals($sStringDefault . $sString2 . $sStringDefault, $aResult['concat_coalesce']);

    }

}
