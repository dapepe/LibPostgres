<?php

class LibPostgres {

    private $rConnection;
    private $bConnected;
    private $sConnString;
    private $aConfig;
    private $sHost;
    private $sUser;
    private $sPwd;
    private $sDB;
    private $iPort;
    private $iRows;
    private $sLastQuery;
    private $bActiveTransaction = false;
    private $bSingleTransaction = false;

    public function __construct($aConfig)
    {
        $this->parseConfig($aConfig);
    }

    public function t() {
        $this->bSingleTransaction = true;
        return $this;
    }

    public function parseConfig($aConfig)
    {
        $this->aConfig = $aConfig;

        $this->sHost   = $aConfig['host'];
        $this->iPort   = $aConfig['port'];
        $this->sUser   = $aConfig['user_name'];
        $this->sPwd    = $aConfig['user_password'];
        $this->sDB     = $aConfig['db_name'];

        $this->sConnString = 'host='     . $this->sHost . ' '
                           . 'port='     . $this->iPort . ' '
                           . 'dbname='   . $this->sDB   . ' '
                           . 'user='     . $this->sUser . ' '
                           . 'password=' . $this->sPwd  . ' '
                           . "options='--client_encoding=UTF8'";
    }

    public function getConfig() {
        return $this->aConfig;
    }

    public function connect()
    {
        if ($this->bConnected) {
            return $this->rConnection;
        }

        $iLevel = error_reporting();

        error_reporting(0);
        $this->rConnection = pg_connect($this->sConnString);
        error_reporting($iLevel);

        $iConnStatus = pg_connection_status($this->rConnection);

        if ($iConnStatus !== PGSQL_CONNECTION_OK) {
            if (is_resource($this->rConnection)) {
                pg_close($this->rConnection);
            }

            throw new Exception('Невозможно подключиться к базе данных.');
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

    public function startTransaction() {
        if (!$this->connect()) {
            return false;
        }

        if ($this->bActiveTransaction) {
            $this->handleQueryError(
                "Cannot start another transaction, commit or rollback the previous one."
            );

            return false;
        }

        $rQueryResult = pg_query(
            $this->rConnection,
            $this->process(array("BEGIN;"))
        );

        $iResult = pg_result_status($rQueryResult);
        $bResult = false;

        if ($iResult == PGSQL_COMMAND_OK || $iResult == PGSQL_TUPLES_OK) {
            $bResult = true;
        } else {
            $this->handleQueryError();

            $bResult = false;
        }

        $this->bActiveTransaction = $bResult;

        return $this->bActiveTransaction;
    }

    public function commit($bDoErrorCheck = true) {
        if (!$this->connect()) {
            return false;
        }

        if (!$this->bActiveTransaction) {
            $this->handleQueryError(
                "Trying to commit without having an explicitly opened transaction."
            );

            return false;
        }

        $bCanCommit = true;

        if ($bDoErrorCheck) {
            $bCanCommit = (!empty($this->aErrors)) ? false : true;
        }

        if ($bCanCommit) {
            $this->query("COMMIT;");
        } else {
            $this->query("ROLLBACK;");
            $this->handleQueryError(
                "Rolling back transaction due to a failed compulsory error check."
            );
        }

        $this->bActiveTransaction = false;

        return true;
    }

    public function rollback() {
        if (!$this->connect()) {
            return false;
        }

        if (!$this->bActiveTransaction) {
            $this->handleQueryError(
                "Trying to roll back without having an explicitly opened transaction."
            );

            return false;
        }

        $this->query("ROLLBACK;");
        $this->bActiveTransaction = false;

        return true;
    }

    public function escape($sArg) {
        if (! $this->connect()) {
            return false;
        }

        return pg_escape_string($this->rConnection, $sArg);
    }

    public function process($aArgs)
    {
        if (empty($aArgs)) {
            return '';
        }

        if (!$this->connect()) {
            return false;
        }

        $this->sLastQuery = ' ' . array_shift($aArgs);

        if (empty($aArgs)) {
            return $this->sLastQuery;
        }

        foreach ($aArgs as $mArg) {
            if (!preg_match('/([^\\\\])\?(w|i|d|f|)/', $this->sLastQuery, $aMatch)) {
                return $this->sLastQuery;
            }

            switch ($aMatch[2]) {
                case 'w':
                    if (is_array($mArg)) {
                        foreach ($mArg as $mKey => $sArg) {
                            $mArg[$mKey] = "'" . pg_escape_string($this->rConnection, $sArg) . "'";
                        }

                        $mArg = implode(',', $mArg);

                    } else {
                        $mArg = "'" . pg_escape_string($this->rConnection, $mArg) . "'";
                    }

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
                            $mArg[$mKey] = intval($sArg);
                        }

                        $mArg = implode(',', $mArg);
                    } else {
                        $mArg = intval($mArg);
                    }

                    break;

                case 'f':
                    if (is_array($mArg)) {

                        foreach ($mArg as $mKey => $sArg) {
                            $mArg[$mKey] = floatval($sArg);
                        }

                        $mArg = implode(',', $mArg);
                    } else {
                        $mArg = floatval($mArg);
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

    public function query()
    {
        if (!$this->connect()) {
            return false;
        }

        if (!$this->bActiveTransaction) {
            $this->handleQueryError(
                "Trying to peform a DDL/DML operation without having an explicitly opened transaction."
            );

            return false;
        }

        $rQueryResult = pg_query(
            $this->rConnection,
            $this->process(func_get_args())
        );

        if ($rQueryResult === false) {
           $this->handleQueryError();
           return false;
        }

        $iResult = pg_result_status($rQueryResult);

        if ($iResult == PGSQL_COMMAND_OK || $iResult == PGSQL_TUPLES_OK) {
            return true;
        }

        $this->handleQueryError();

        return false;
    }

    public function queryAff()
    {
        if (!$this->connect()) {
            return false;
        }

        if (!$this->bActiveTransaction) {
            $this->handleQueryError(
                "Trying to peform a DDL/DML operation without having an explicitly opened transaction."
            );

            return false;
        }

        $rQueryResult = pg_query(
            $this->rConnection,
            $this->process(func_get_args())
        );

        if ($rQueryResult === false) {
           $this->handleQueryError();
           return false;
        }

        $iResult = pg_result_status($rQueryResult);

        if ($iResult != PGSQL_COMMAND_OK) {
            $this->handleQueryError();
            return false;
        }

        return pg_affected_rows($rQueryResult);
    }

    public function selectTable()
    {
        $aOut = array();

        if (!$this->connect()) {
            return $aOut;
        }

        $rQueryResult = pg_query(
            $this->rConnection,
            $this->process(func_get_args())
        );

        $iResult = pg_result_status($rQueryResult);

        if ($iResult != PGSQL_TUPLES_OK) {
            $this->handleQueryError();
            return $aOut;
        }

        while ($aResult = pg_fetch_assoc($rQueryResult)) {
            $aOut[] = $aResult;
        }

        $this->iRows = pg_num_rows($rQueryResult);

        return $aOut;
    }

    public function selectRecord()
    {
        if (!$this->connect()) {
            return array();
        }

        $rQueryResult = pg_query(
            $this->rConnection,
            $this->process(func_get_args())
        );

        $iResult = pg_result_status($rQueryResult);

        if ($iResult != PGSQL_TUPLES_OK) {
            $this->handleQueryError();
            return array();
        }

        if (pg_num_rows($rQueryResult) == 0) {
            return array();
        }

        $aRecord = pg_fetch_array($rQueryResult, 0, PGSQL_ASSOC);

        return $aRecord ? $aRecord : array();
    }

    public function selectField()
    {
        if (!$this->connect()) {
            return false;
        }

        if ($this->bSingleTransaction) $this->startTransaction();

        $rQueryResult = pg_query(
            $this->rConnection,
            $this->process(func_get_args())
        );

        $iResult = pg_result_status($rQueryResult);

        if ($iResult != PGSQL_TUPLES_OK) {
            $this->handleQueryError();
            return false;
        }

        if ($this->bSingleTransaction) $this->commit();

        $this->bSingleTransaction = false;

        $aRecord = pg_fetch_row($rQueryResult);



        return $aRecord ? $aRecord[0] : null;
    }

    public function selectColumn()
    {
        $aOut = array();

        if (!$this->connect()) {
            return $aOut;
        }

        $rQueryResult = pg_query(
            $this->rConnection,
            $this->process(func_get_args())
        );

        $iResult = pg_result_status($rQueryResult);

        if ($iResult != PGSQL_TUPLES_OK) {
            $this->handleQueryError();
            return $aOut;
        }

        while ($aResult = pg_fetch_row($rQueryResult)) {
            $aOut[] = $aResult[0];
        }

        $this->iRows = pg_num_rows($rQueryResult);

        return $aOut;
    }

    public function selectIndexedColumn()
    {
        $aOut = array();

        if (!$this->connect()) {
            return $aOut;
        }

        $rQueryResult = pg_query(
            $this->rConnection,
            $this->process(func_get_args())
        );

        $iResult = pg_result_status($rQueryResult);

        if ($iResult != PGSQL_TUPLES_OK) {
            $this->handleQueryError();
            return $aOut;
        }

        while ($aResult = pg_fetch_row($rQueryResult)) {
            $aOut[$aResult[0]] = $aResult[1];
        }

        $this->iRows = pg_num_rows($rQueryResult);

        return $aOut;
    }

    public function selectIndexedColumnArrays()
    {
        $aOut = array();

        if (!$this->connect()) {
            return $aOut;
        }

        $rQueryResult = pg_query(
            $this->rConnection,
            $this->process(func_get_args())
        );

        $iResult = pg_result_status($rQueryResult);

        if ($iResult != PGSQL_TUPLES_OK) {
            $this->handleQueryError();
            return $aOut;
        }

        while ($aResult = pg_fetch_row($rQueryResult)) {
            $aOut[$aResult[0]][] = $aResult[1];
        }

        $this->iRows = pg_num_rows($rQueryResult);

        return $aOut;
    }

    public function selectIndexedTable()
    {
        $aOut = array();

        if (!$this->connect()) {
            return $aOut;
        }

        $rQueryResult = pg_query(
            $this->rConnection,
            $this->process(func_get_args())
        );

        $iResult = pg_result_status($rQueryResult);

        if ($iResult != PGSQL_TUPLES_OK) {
            $this->handleQueryError();
            return $aOut;
        }

        while ($aResult = pg_fetch_assoc($rQueryResult)) {
            $aOut[reset($aResult)] = $aResult;
        }

        $this->iRows = pg_num_rows($rQueryResult);

        return $aOut;
    }

    public function select2IndexedColumn()
    {
        $aOut = array();

        if (!$this->connect()) {
            return $aOut;
        }

        $rQueryResult = pg_query(
            $this->rConnection,
            $this->process(func_get_args())
        );

        $iResult = pg_result_status($rQueryResult);

        if ($iResult != PGSQL_TUPLES_OK) {
            $this->handleQueryError();
            return $aOut;
        }

        while ($aResult = pg_fetch_row($rQueryResult)) {
            $aOut[$aResult[0]][$aResult[1]] = $aResult[2];
        }

        $this->iRows = pg_num_rows($rQueryResult);

        return $aOut;
    }

    public function select2IndexedTable()
    {
        $aOut = array();

        if (!$this->connect()) {
            return $aOut;
        }

        $rQueryResult = pg_query(
            $this->rConnection,
            $this->process(func_get_args())
        );

        $iResult = pg_result_status($rQueryResult);

        if ($iResult != PGSQL_TUPLES_OK) {
            $this->handleQueryError();
            return $aOut;
        }

        while ($aResult = pg_fetch_assoc($rQueryResult)) {
            $sIndex1 = reset($aResult);
            $sIndex2 = next($aResult);
            $aOut[$sIndex1][$sIndex2] = $aResult;
        }

        $this->iRows = pg_num_rows($rQueryResult);

        return $aOut;
    }

    public function select3IndexedColumn()
    {
        $aOut = array();

        if (!$this->connect()) {
            return $aOut;
        }

        $rQueryResult = pg_query(
            $this->rConnection,
            $this->process(func_get_args())
        );

        $iResult = pg_result_status($rQueryResult);

        if ($iResult != PGSQL_TUPLES_OK) {
            $this->handleQueryError();
            return $aOut;
        }

        while ($aResult = pg_fetch_row($rQueryResult)) {
            $aOut[$aResult[0]][$aResult[1]][$aResult[2]] = $aResult[3];
        }

        $this->iRows = pg_num_rows($rQueryResult);

        return $aOut;
    }

    public function getLastQuery() {
        return $this->sLastQuery;
    }

    public function getRowsQuantity()
    {
        return intval($this->iRows);
    }

    protected function handleQueryError($sMessage = false)
    {
        $sError = ($sMessage) ? $sMessage : pg_last_error($this->rConnection);

        if (! $sError) {
            $sError = 'Undefined error.';
        }

        throw new \Exception($sError);
    }

}
