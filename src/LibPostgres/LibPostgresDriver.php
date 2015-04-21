<?php

/*
 * This is a PostgreSQL PHP wrapper.
 *
 * (c) 2014 Denis Milovanov
 *
 * See http://github.com/denismilovanov/LibPostgres
 */

namespace LibPostgres;

class LibPostgresDriver
{
    private $rConnection;
    private $bConnected;
    private $sConnString;
    private $aConfig;
    private $sHost;
    private $sUser;
    private $sPwd;
    private $sDB;
    private $iPersistence;
    private $iPort;
    private $iRows;
    private $sLastQuery;
    private $bActiveTransaction = false;
    private $bSingleTransaction = false;
    private $iAffectedRowsCount = 0;
    private $iLastResult = null;

    public function __construct($aConfig)
    {
        $this->aConfig = $aConfig;

        $this->sHost   = $aConfig['host'];
        $this->iPort   = $aConfig['port'];
        $this->sUser   = $aConfig['user_name'];
        $this->sPwd    = $aConfig['user_password'];
        $this->sDB     = $aConfig['db_name'];

        // 0 - pg_connect
        // 1 - pg_pconnect
        // 2 - pg_pconnect(..., PGSQL_CONNECT_FORCE_NEW)
        $this->iPersistence = isset($aConfig['persistence']) ? $aConfig['persistence'] : 0;

        $this->sConnString = 'host='     . $this->sHost . ' '
                           . 'port='     . $this->iPort . ' '
                           . 'dbname='   . $this->sDB   . ' '
                           . 'user='     . $this->sUser . ' '
                           . 'password=' . $this->sPwd  . ' '
                           . "options='--client_encoding=UTF8'";
    }

    public function getConfig()
    {
        return $this->aConfig;
    }

    public function connect()
    {
        if ($this->bConnected) {
            return $this->rConnection;
        }

        $iLevel = error_reporting();

        // to suppress E_WARNING that can happen
        error_reporting(0);

        if ($this->iPersistence == 0) {
            // plain connect
            $this->rConnection = pg_connect($this->sConnString, PGSQL_CONNECT_FORCE_NEW);
        } else if ($this->iPersistence == 1) {
            // persistent connect
            $this->rConnection = pg_pconnect($this->sConnString);
        } else if ($this->iPersistence == PGSQL_CONNECT_FORCE_NEW) {
            // persistent connect forced new
            $this->rConnection = pg_connect($this->sConnString, PGSQL_CONNECT_FORCE_NEW);
        }

        // lets restore previous level
        error_reporting($iLevel);

        $iConnStatus = pg_connection_status($this->rConnection);

        if ($iConnStatus !== PGSQL_CONNECTION_OK) {
            if (is_resource($this->rConnection)) {
                pg_close($this->rConnection);
            }

            throw new \Exception('Unable to connect.');
        }

        $this->bConnected = true;

        return $this->rConnection;
    }

    public function isConnected()
    {
        return $this->bConnected;
    }

    public function close()
    {
        if ($this->bConnected) {
            $this->bConnected = false;

            if (is_resource($this->rConnection)) {
                return pg_close($this->rConnection);
            } else {
                return true;
            }
        }

        return false;
    }

    public function __destruct()
    {
        return $this->close();
    }

    private function isResultOK($aOKStatuses = array(PGSQL_TUPLES_OK))
    {
        return in_array($this->iLastResult, $aOKStatuses);
    }

    public function startTransaction()
    {
        if (! $this->connect()) {
            return false;
        }

        if ($this->bActiveTransaction) {
            $this->throwException(
                "Cannot start another transaction, commit or rollback the previous one."
            );

            return false;
        }

        $rQueryResult = $this->_pg_query("BEGIN;", array(PGSQL_COMMAND_OK, PGSQL_TUPLES_OK));

        return $this->bActiveTransaction = true;
    }

    public function t()
    {
        $this->bSingleTransaction = true;
        return $this;
    }

    public function commit()
    {
        if (! $this->connect()) {
            return false;
        }

        if (! $this->bActiveTransaction) {
            $this->throwException(
                "Trying to commit without having an explicitly opened transaction."
            );

            return false;
        }

        $this->_pg_query("COMMIT;", array(PGSQL_COMMAND_OK, PGSQL_TUPLES_OK));
        $this->bActiveTransaction = false;

        return true;
    }

    public function rollback()
    {
        if (! $this->connect()) {
            return false;
        }

        if (! $this->bActiveTransaction) {
            $this->throwException(
                "Trying to roll back without having an explicitly opened transaction."
            );

            return false;
        }

        $this->_pg_query("ROLLBACK;", array(PGSQL_COMMAND_OK, PGSQL_TUPLES_OK));
        $this->bActiveTransaction = false;

        return true;
    }

    public function escape($sArg)
    {
        if (! $this->connect()) {
            return false;
        }

        return $sArg !== null ? pg_escape_string($this->rConnection, $sArg) : 'NULL';
    }

    public function prepareHstore($aArg) {
        if (! is_array($aArg)) {
            return '';
        }

        $aResult = array();

        foreach ($aArg as $sKey => $sValue) {
            if (empty($sKey)) {
                continue;
            }
            $sKey = str_replace('\\', '\\\\', $sKey);
            $sKey = str_replace('"', '\"', $sKey);
            if ($sValue !== null) {
                $sValue = str_replace('\\', '\\\\', $sValue);
                $sValue = str_replace('"', '\"', $sValue);
            }
            $aResult []= '"' . $sKey . '" => ' . ($sValue !== null ? ('"' . $sValue . '"') : 'NULL');
        }

        return $this->escape(implode(', ', $aResult));
    }

    public function prepareJson($mArg) {
        return $this->escape(json_encode($mArg));
    }

    public function process($aArgs)
    {
        if (empty($aArgs)) {
            return '';
        }

        if (! $this->connect()) {
            return false;
        }

        $this->sLastQuery = ' ' . array_shift($aArgs);

        if (empty($aArgs)) {
            return $this->sLastQuery;
        }

        foreach ($aArgs as $mArg) {
            if (! preg_match('/([^\\\\])\?(w|i|d|f|h|(jb)|j|)/', $this->sLastQuery, $aMatch)) {
                return $this->sLastQuery;
            }

            switch ($aMatch[2]) {
                case 'w':
                    if (is_array($mArg)) {
                        foreach ($mArg as $mKey => $sArg) {
                            $mArg[$mKey] = $sArg !== null ? ("'" . $this->escape($sArg) . "'") : 'NULL';
                        }

                        $mArg = implode(',', $mArg);

                    } else {
                        $mArg = $mArg !== null ? ("'" . $this->escape($mArg) . "'") : 'NULL';
                    }

                    break;

                case 'h':
                    $mArg = $mArg !== null ? ("'" . $this->prepareHstore($mArg) . "'::hstore") : 'NULL::hstore';

                    break;

                case 'j':
                    $mArg = $mArg !== null ? ("'" . $this->prepareJson($mArg) . "'::json") : 'NULL::jsonb';

                    break;

                case 'jb':
                    $mArg = $mArg !== null ? ("'" . $this->prepareJson($mArg) . "'::jsonb") : 'NULL::jsonb';

                    break;

                case 'i':
                    if (is_array($mArg)) {

                        foreach ($mArg as $mKey => $sArg) {
                            $mArg[$mKey] = "quote_ident('" . $sArg . "')";
                        }

                        $mArg = implode(',', $mArg);
                    } else {
                        $mArg = "quote_ident('" . $mArg . "')";
                    }

                    break;

                case 'd':
                    if (is_array($mArg)) {

                        foreach ($mArg as $mKey => $sArg) {
                            $mArg[$mKey] = $sArg !== null ? intval($sArg) : 'NULL';
                        }

                        $mArg = implode(',', $mArg);
                    } else {
                        $mArg = $mArg !== null ? intval($mArg) : 'NULL';
                    }

                    break;

                case 'f':
                    if (is_array($mArg)) {

                        foreach ($mArg as $mKey => $sArg) {
                            $mArg[$mKey] = $sArg !== null ? floatval($sArg) : 'NULL';
                        }

                        $mArg = implode(',', $mArg);
                    } else {
                        $mArg = $mArg !== null ? floatval($mArg) : 'NULL';
                    }

                    break;

                case '':
                    if (is_array($mArg)) {
                        $mArg = implode(',', $mArg);
                    }

                    break;
            }

            $sNeedle = $aMatch[0];
            $sReplacement = $aMatch[1] . str_replace('?', '\\?', $mArg);

            $iPos = strpos($this->sLastQuery, $sNeedle);

            if ($iPos !== false) {
                $this->sLastQuery = substr_replace(
                    $this->sLastQuery,
                    $sReplacement,
                    $iPos,
                    strlen($sNeedle)
                );
            }
        }

        $this->sLastQuery = str_replace('\\?', '?', $this->sLastQuery);

        return $this->sLastQuery;
    }

    private function _pg_query($aArguments = array(), $aOKStatuses = array(PGSQL_TUPLES_OK))
    {
        if (is_scalar($aArguments)) {
            $aArguments = array($aArguments);
        }

        $iLevel = error_reporting();

        // to suppress E_WARNING that can happen
        error_reporting(0);

        $rQueryResult = pg_query(
            $this->rConnection,
            $this->process($aArguments)
        );

        $this->iLastResult = pg_result_status($rQueryResult);

        error_reporting($iLevel);

        if (! $this->isResultOK($aOKStatuses)) {
            if ($aArguments == array('ROLLBACK;')) {
                // first false forces throwException to use pg_last_error
                // second false prevents recursion:
                //      throwException -> say rollback -> unable to say rollback (e.g. server has gone) ->
                //      --> throwException -> say rollback -> unable to say rollback --> ...
                $this->throwException(false, false);
            } else {
                $this->throwException();
            }
        }

        $this->iAffectedRowsCount = (int)pg_affected_rows($rQueryResult);
        $this->iRows = (int)pg_num_rows($rQueryResult);

        return $rQueryResult;
    }

    public function query()
    {
        if (! $this->connect()) {
            return false;
        }

        if ($this->bSingleTransaction) {
            $this->startTransaction();
        }

        if (! $this->bActiveTransaction) {
            // will throw an exception
            $this->throwException(
                "Trying to peform a DDL/DML operation without having an explicitly opened transaction."
            );
        }

        $rQueryResult = $this->_pg_query(func_get_args(), array(PGSQL_COMMAND_OK, PGSQL_TUPLES_OK));

        if ($this->bSingleTransaction) {
            $this->bSingleTransaction = false;
            $this->commit();
        }

        return true;
    }

    public function selectTable()
    {
        $aOut = array();

        if (! $this->connect()) {
            return $aOut;
        }

        $rQueryResult = $this->_pg_query(func_get_args());

        while ($aResult = pg_fetch_assoc($rQueryResult)) {
            $aOut[] = $aResult;
        }

        return $aOut;
    }

    public function selectRecord()
    {
        if (! $this->connect()) {
            return array();
        }

        $rQueryResult = $this->_pg_query(func_get_args());

        if ($this->iRows == 0) {
            return array();
        }

        $aRecord = pg_fetch_array($rQueryResult, 0, PGSQL_ASSOC);

        return $aRecord ? : array();
    }

    public function selectField()
    {
        if (! $this->connect()) {
            return false;
        }

        $rQueryResult = $this->_pg_query(func_get_args());

        $aRecord = pg_fetch_row($rQueryResult);

        // it is strange situation $aResult[0] is not set
        // selectField("SELECT ;") will throw an error in _pg_query
        return $aRecord ? $aRecord[0] : null;
    }

    public function selectColumn()
    {
        $aOut = array();

        if (! $this->connect()) {
            return $aOut;
        }

        $rQueryResult = $this->_pg_query(func_get_args());

        while ($aResult = pg_fetch_row($rQueryResult)) {
            // it is strange situation $aResult[0] is not set
            // selectColumn("SELECT ;") will throw an error in _pg_query
            $aOut []= $aResult[0];
        }

        return $aOut;
    }

    public function selectIndexedColumn()
    {
        $aOut = array();

        if (! $this->connect()) {
            return $aOut;
        }

        $rQueryResult = $this->_pg_query(func_get_args());

        while ($aResult = pg_fetch_row($rQueryResult)) {
            // keeping in mind the performance we do not check if $aResult[0], $aResult[1] are set
            // you should provide at least 2 column in the result set
            // also we do not suppress warnings by operator @, is is VERY BAD practice
            $aOut[$aResult[0]] = $aResult[1];
        }

        return $aOut;
    }

    public function selectIndexedColumnArrays()
    {
        $aOut = array();

        if (! $this->connect()) {
            return $aOut;
        }

        $rQueryResult = $this->_pg_query(func_get_args());

        while ($aResult = pg_fetch_row($rQueryResult)) {
            // you should provide at least 2 column in the result set
            $aOut[$aResult[0]] []= $aResult[1];
        }

        return $aOut;
    }

    public function selectIndexedTable()
    {
        $aOut = array();

        if (! $this->connect()) {
            return $aOut;
        }

        $rQueryResult = $this->_pg_query(func_get_args());

        while ($aResult = pg_fetch_assoc($rQueryResult)) {
            $aOut[reset($aResult)] = $aResult;
        }

        return $aOut;
    }

    public function select2IndexedColumn()
    {
        $aOut = array();

        if (! $this->connect()) {
            return $aOut;
        }

        $rQueryResult = $this->_pg_query(func_get_args());

        while ($aResult = pg_fetch_row($rQueryResult)) {
            // you should provide at least 3 column in the result set
            $aOut[$aResult[0]][$aResult[1]] = $aResult[2];
        }

        return $aOut;
    }

    public function select2IndexedTable()
    {
        $aOut = array();

        if (! $this->connect()) {
            return $aOut;
        }

        $rQueryResult = $this->_pg_query(func_get_args());

        while ($aResult = pg_fetch_assoc($rQueryResult)) {
            // you should provide at least 2 column in the result set
            $sIndex1 = reset($aResult);
            $sIndex2 = next($aResult);
            $aOut[$sIndex1][$sIndex2] = $aResult;
        }

        return $aOut;
    }

    public function select3IndexedColumn()
    {
        $aOut = array();

        if (! $this->connect()) {
            return $aOut;
        }

        $rQueryResult = $this->_pg_query(func_get_args());

        while ($aResult = pg_fetch_row($rQueryResult)) {
            // you should provide at least 4 column in the result set
            $aOut[$aResult[0]][$aResult[1]][$aResult[2]] = $aResult[3];
        }

        return $aOut;
    }

    public function select3IndexedTable()
    {
        $aOut = array();

        if (! $this->connect()) {
            return $aOut;
        }

        $rQueryResult = $this->_pg_query(func_get_args());

        while ($aResult = pg_fetch_row($rQueryResult)) {
            // you should provide at least 4 column in the result set
            $aOut[$aResult[0]][$aResult[1]][$aResult[2]] = $aResult;
        }

        return $aOut;
    }

    public function getLastQuery()
    {
        return trim($this->sLastQuery);
    }

    public function getRowsQuantity()
    {
        return $this->iRows;
    }

    public function getAffectedRowsQuantity()
    {
        return $this->iAffectedRowsCount;
    }

    protected function throwException($sMessage = false, $bSayRollback = true)
    {
        // use given message or internal?
        $sError = $sMessage ? : pg_last_error($this->rConnection);

        if (! $sError) {
            $sError = 'Undefined error (check if your select query really returns any data).';
        }

        // an exception will be thrown, so mark the end of transaction
        $this->bActiveTransaction = false;

        // see _pg_query for details
        if ($bSayRollback) {
            $this->_pg_query("ROLLBACK;", array(PGSQL_COMMAND_OK, PGSQL_TUPLES_OK));
        }

        throw new \Exception($sError);
    }

}
