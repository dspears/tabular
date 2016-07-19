<?php

namespace Tabular\Data;

/**
 * Class TabularTable  - A simple, lightweight query API encapsulating a SQL table, with some useful features built in,
 * such as:
 *
 * - Ability to log all changes (updates/inserts/deletes) to an associated history table.
 * - Logging of the username / time of latest change to the table.
 * - Supports method chaining.
 * - Run in a mode where no updates/inserts are permitted (some client code only needs read access, and can enforce this
 *   by passing false to the $execute parameter of the constrctur).
 * - Logging of all SQL to a passed in logger object.
 *
 *
 * @author David Spears
 *
 */
class TabularTable {
  protected $tabularDb;
  protected $dbh;
  protected $tableName;
  protected $key;
  protected $histTableName;
  protected $historyKey;
  protected $rec;
  protected $userName;
  protected $selected;  // fields to be selected on next SELECT query.
  protected $whereClause;
  protected $bindings;
  protected $limit;
  protected $order;
  protected $execute;
  protected $history;
  protected $locked;
  protected $lockType;
  protected $sql;
  protected $lastQuery;
  protected $lastQueryString;
  protected $lastError;
  protected $logUpdatedByOn;

  /**
   * Constructor for a TabularTable object.
   *
   * @param $tabularDb - the database the table is in, represented by a TabularDb object.
   * @param $tableName - name of the table.
   * @param string $user - user accessing the table.
   * @param null $logger - a "debug logger" to receive trace of everything that's done (optional).
   * @param bool $execute - controls whether or not to actually write to the db (false=do NOT write changes).
   * @param bool $history - controls whether or not a history is logged (i.e. true = write changes to the "change log").
   * @param string $key - list of key columns (can be a comma separated list or an array).  This is optional here and can instead be specified in other API calls.
   */
  public function __construct($tabularDb, $tableName, $user='TABULARadmin', $logger=null, $execute=true, $history=true, $key='', $logUpdatedByOn=true) {
    $this->tabularDb = $tabularDb;
    $this->dbh = $tabularDb->dbh();
    $this->tableName = $tableName;
    $this->histTableName = $tableName."_change_log";
    $this->historyKey = '';
    $this->userName = $user;
    $this->selected = "*";
    $this->logger = $logger;
    $this->whereClause = '';
    $this->bindings = array();
    $this->limit = 0;
    $this->order = '';
    $this->execute = $execute;
    $this->history = $history;
    $this->locked = false;
    $this->sql = '';
    $this->key = $this->setKey($key);
    $this->lastQuery = '';
    $this->lastError = '';
    $this->logUpdatedByOn = $logUpdatedByOn;
  }

  /**
   * Set the name of the history table to use, overriding the default.
   *
   * @param $name
   */
  public function setHistoryTableName($name) {
    $this->histTableName = $name;
    return $this;
  }

  /**
   * Set the key column names for the history table.
   *
   * @param $key
   * @return $this
   */
  public function setHistoryKey($key) {
    $this->historyKey = $key;
    return $this;
  }

  /**
   * Get the names of the database tables represented by this TabularTable object, including any change log table name.
   *
   * @return string - comma separated list of table names.
   */
  public function getTableNames() {
    $tables = $this->tableName;
    if ($this->history) {
      $tables .= ','.$this->histTableName;
    }
    return $tables;
  }

  /**
   * Get the primary table name.
   */
  public function getTableName() {
    return $this->tableName;
  }

  public function select($s) {
    if (is_array($s)) {
      $s = implode(',',$s);
    }
    $this->selected = $s;
    return $this;
  }

  protected function log($msg) {
    if (isset($this->logger)) {
      $this->logger->log($msg);
    }
  }

  public function setLogger($logger) {
    $this->logger = $logger;
    return $this;
  }

  public function lock($lockType='WRITE',$moreTables='') {
    $tables = empty($moreTables) ? $this->tableName : $this->tableName.','.$moreTables;
    if ($this->history) {
      $tables .= ','.$this->histTableName;
    }
    $tableList = explode(',',$tables);
    $tableLocks = __::reduce($tableList,function($s,$tbl) use($lockType) {return $s.$tbl." $lockType,";},'');
    $tableLocks = substr($tableLocks,0,-1);
    $sql = "LOCK TABLES $tableLocks";
    $sth = $this->dbh->prepare($sql);
    $this->beforeQuery($sth);
    $ret = $sth->execute();
    $this->log($sql);
    if ($ret === false) {
      throw new Exception("Lock failed: $sql. ".print_r($sth->errorInfo(),true));
    }
    $this->locked = true;
    $this->lockType = $lockType;
    return $this;
  }

  public function unlock() {
    if ($this->locked) {
      $sql = "UNLOCK TABLES";
      $sth = $this->dbh->prepare($sql);
      $this->beforeQuery($sth);
      $ret = $sth->execute();
      $this->log($sql);
      if ($ret === false) {
        throw new Exception("Unlock failed: $sql. ".print_r($sth->errorInfo(),true));
      }
      $this->locked = false;
    }
    return $this;
  }

  public function loadFromArray($rows) {
    $sql = $this->constructInsert($rows[0]);
    $sth = $this->dbh->prepare($sql);
    $c = 1;
    $this->beforeQuery($sth);
    foreach ($rows as $row) {
      $ret = $sth->execute($row);
      if ($ret === false) {
        print_r($row);
        throw new Exception("Insert failed on row $c. ".print_r($sth->errorInfo(),true));
        $id = -1;
      } else {
        // Success
        $id = $this->dbh->lastInsertId();
      }
      $c++;
    }
    $this->reset();
    return $this;
  }

  protected function addUpdateInfo(&$rec=array(),&$updateCols=array()) {
    if ($this->logUpdatedByOn) {
      $this->rec['Updated_By'] = $rec['Updated_By'] = $this->userName;
      $this->rec['Updated_On'] = $rec['Updated_On'] = date("Y-m-d H:i:s");
      $updateCols[] = 'Updated_By';
      $updateCols[] = 'Updated_On';
    }
  }

  /**
   * Build the SQL string for an insert.
   *
   * @param array $row
   * @param string $tableName - optional table name.  Defaults to $this->tableName.
   * @return string
   */
  protected function constructInsert($row=array(),$tableName='') {
    if (empty($row)) {
      $row = $this->rec;
    }
    $tableName = empty($tableName) ? $this->tableName : $tableName;
    $sql = "INSERT INTO $tableName (";
    foreach ($row as $column=>$value) {
      $sql .= '`'.$column.'`,';
    }
    $sql = substr($sql,0,-1).")";
    $sql .= " VALUES (";
    foreach ($row as $column=>$value) {
      $sql .= ":".$column.",";
    }
    $sql = substr($sql,0,-1).")";
    return $sql;
  }

  public function insert($rec,$tableName='') {
    $this->rec = $rec;
    $this->addUpdateInfo();
    $sql = $this->constructInsert($this->rec,$tableName);
    $sth = $this->dbh->prepare($sql);
    $this->log($sql);
    $this->log($this->rec);
    $this->beforeQuery($sth);
    if ($this->execute) {
      $ret = $sth->execute($this->rec);
      if ($ret === false) {
        $err = implode(' : ',array_reverse($sth->errorInfo()));
        $this->setLastError($err);
        throw new Exception('Insert failed. Sql:'.$sql.' Msg: '.print_r($sth->errorInfo(),true));
        $id = -1;
      } else {
        // Success
        $this->log("Insert succeeded");
        $id = $this->dbh->lastInsertId();
      }
    } else {
      $this->log("Table writes are turned off.");
      $id = 0;
    }
    $this->reset();
    $this->log("INSERT ID is $id");
    return $id;
  }

  public function update($rec, $keyCols, $updateCols=array(), $limit=true, $keyVals=array()) {
    $update_rec = array();
    $sql = "UPDATE $this->tableName SET ";
    // If no updateCols provided, then assume all of them:
    if (empty($updateCols)) {
      $updateCols = array_keys($rec);
    }
    $this->addUpdateInfo($rec,$updateCols);
    foreach ($updateCols as $col) {
      $sql .= "`$col`=:$col,";
      $update_rec[$col] = $rec[$col];
      // Useful for debugging character encoding issues:
      // TABULAR::hex_dump($rec[$col]);
    }
    $sql = substr($sql,0,-1); // remove trailing comma
    $sql.= " WHERE ";
    foreach ($keyCols as $col) {
      if (isset($keyVals[$col])) {
        $sql .= " `$col`=:key_$col AND";
        $update_rec['key_'.$col] = $keyVals[$col];
      } else if (isset($rec[$col])) {
        $sql .= " `$col`=:$col AND";
        $update_rec[$col] = $rec[$col];
      } else {
        throw new Exception("Key column not found in record: $col. ".print_r($rec,true));
      }
    }
    $sql = substr($sql,0,-4);  // remove trailing " AND"
    if ($limit)
      $sql .= " LIMIT 1";
    $sth = $this->dbh->prepare($sql);
    $this->log($sql);
    $this->log($update_rec);
    $this->beforeQuery($sth);
    if ($this->execute) {
      $ret = $sth->execute($update_rec);
    } else {
      $ret = true;
    }
    if ($ret == false) {
      throw new Exception("Update failed. ".print_r($sth->errorInfo(),true));
    }

    // Return number of rows affected:
    $c = $sth->rowCount();
    $this->log("UPDATE affected $c rows");
    $this->reset();
    return $c;
  }

  public function hasDataChanged($rows) {
    $changed = false;
    if (!empty($rows)) {
      $updateCols = array();
      foreach ($rows as $ix=>$row) {
        foreach ($row['N'] as $col=>$val) {
          $old_val = trim($row['O'][$col]);
          $val = trim($val);
          if ($old_val != $val) {
            $updateCols[$col] = $col;
            FB::log('Changed detected in col '.$col." old: '$old_val', new: '$val'");
          }
        }
        if (!empty($updateCols)) {
          unset($updateCols);
          return true;
        }
      }
    }
    return $changed;
  }

  /**
   * Saves rows to the table, but does NOT record history.
   * If history is needed, calculate diffs using TABULAR::getDiffs(), then call $table->updateRows();
   *
   * @param Array $rows
   * @param string or Array $key
   * @return number
   */
  public function saveRows($rows, $key='') {
    $key = $this->getKey($key);
    $c = 0;
    if (!is_array($key)) {
      $key = str_replace(' ','',$key);
      $key=explode(',',$key);
    }
    foreach ($rows as $row) {
      $this->saveRow($row, $key);
      $c++;
    }
    return $c;
  }

  /**
   * saveRow - saves a row to the table, but does NOT record history.
   * If history is needed, calculate diffs using TABULAR::getDiffs(), then call $table->updateRows();
   *
   * @param Array $rows
   * @param string or Array $key
   */
  public function saveRow($row, $key='') {
    $key = $this->getKey($key);
    // See if this is an 'id' autoincrement situation:
    if (($key[0] == 'id') && (!isset($row['id']))) {
      // Assume its an autoincrement 'id' key.
      $db_row = '';
    } else {
      // Attempt to read the record (it may or may not exist):
      $db_row = $this->getby($row,$key);
    }
    // If found, this is an update:
    if (!empty($db_row)) {
      $c = $this->update($row,$key);
    } else {
      // Else it's an insert:
      $id = $this->insert($row);
      $c = ($id >= 0) ? 1 : 0;
    }
    // Return # rows affected (if no failure occurred it should be 1)
    return $c;
  }

  /**
   * Set the key to be used in SQL queries, updates, inserts, and deletes for this table.
   *
   * @param $key - Array or comma separated string of column names that indicate the keys to this table.
   */
  public function setKey($key) {
    if (empty($key)) {
      $this->key = '';
    } else {
      if (!is_array($key)) {
        $key = str_replace(' ','',$key);
        $key=explode(',',$key);
      }
      $this->key = $key;
    }
  }

  protected function getKey($key) {
    if (empty($key)) {
      if (empty($this->key)) {
        // Default to a key of 'id':
        $key = 'id';
      } else {
        $key = $this->key;
      }
    } else {
      if (!is_array($key)) {
        $key=explode(',',$key);
      }
    }
    return $key;
  }


  protected function writeHistory($updateType,$cri,$key,$row_new,$row_old=null,$updateCols=null) {
    if (!is_array($key)) {
      $key = array($key);
    }
    if ($this->history) {
      $updateType = ucfirst(strtolower($updateType));
      $rec = array(
          'CRI' => $cri,
          'Change_Type' => $updateType,
      );
      // Add keys:
      foreach ($key as $k) {
        $rec[$k] = $row_new[$k];
      }
      if ($this->locked) {
        $this->lock($this->lockType,$this->histTableName);
      }
      switch ($updateType) {
        case 'Insert':
        case 'Delete':
          // For inserts and deletes We don't record every field value.  Just a single record for the insert.
          $this->insert($rec, $this->histTableName);
          break;
        case 'Update':
          // Add additional fields to be written to change log, one record at a time.
          // NOTE: We are adding a history record for each changed field!
          foreach ($updateCols as $col) {
            $updateRec = $rec;
            $updateRec['Field_Name'] = $col;
            $updateRec['Old_Value'] = $row_old[$col];
            $updateRec['New_Value'] = $row_new[$col];
            $this->insert($updateRec, $this->histTableName);
          }
          break;
        default:
          throw new Exception("Invalid update type: $updateType");
      }
    }
  }

  /**
   * Updates a table given a set of 'diff' records.
   *
   * @param array $rows - The diff records.
   * @param string $key - list of key field names (can be comma separated string or an array).
   * @param unknown $colMask - Apply updates to only the fiels in this array (optional).
   *
   * @return number - Number of updates performed.
   */
  public function updateRows($rows,$key='id',$colMask=array()) {
    $cri = ''; // default value when no change control is used.
    $c = 0;
    if (!is_array($key)) {
      $key=explode(',',$key);
    }
    $useColMask = !empty($colMask);
    if (!empty($rows)) {
      $updateCols = array();
      foreach ($rows as $ix=>$row) {
        if ($useColMask) {
          $row['N'] = TABULAR::justKeys($row['N'],$colMask);
        }
        foreach ($row['N'] as $col=>$val) {
          $old_val = trim($row['O'][$col]);
          $val = trim($val);
          if ($old_val != $val) {
            $updateCols[$col] = $col;
            $old_vals[$col] = $val;
          }
        }
        if (!empty($updateCols)) {
          $c++;
          unset($keyVals);
          $keyVals = array();
          foreach ($key as $k) {
            $keyVals[$k] = $row['O'][$k];
          }
          $rowsAffected = $this->update($row['N'],$key,$updateCols,true,$keyVals);
          if ($rowsAffected == 0) {
            // When update indicates no rows affected, it could be one of two things:
            //
            // a. The record doesn't exist and needs to be inserted.
            // b. The data in the db record is identical to the user's update, so no action needed.
            //
            // So here we will determine which it is.
            // Attempt to read the record with the given key:
            $db_row = $this->getby($row['O'],$key);
            // If not found, this must be an insert:
            if (empty($db_row)) {
              $insertId = $this->insert($row['N']);
              if (!empty($this->historyKey)) {
                $historyKey = $this->historyKey;
                $row['N'][$historyKey] = $insertId;
                $this->log("Insert using historyKey {$this->historyKey}, value: {$row['N'][$historyKey]}");
              } else {
                $historyKey = $key;
              }
              $this->writeHistory('insert',$cri,$historyKey,$row['N']);
            }
          } else {
            // Record changes in change history log.
            if (!empty($this->historyKey)) {
              $historyKey = $this->historyKey;
              // Make sure the history key value is set in the new row data:
              $db_row = $this->getby($row['N'],$key);
              $row['N'][$historyKey] = $db_row[$historyKey];
              $this->log("Update using historyKey {$this->historyKey}, value: {$row['N'][$historyKey]}");
            } else {
              $historyKey = $key;
            }
            $this->writeHistory('update',$cri,$historyKey,$row['N'],$row['O'],$updateCols);
          }
          unset($updateCols);
          $updateCols = array();
        } else {
          $this->log("no diff");
        }
      }
    }
    return $c;
  }


  public function find($rec,$keyColumns) {
    $rows = array(); // default return value
    $sql = "SELECT * FROM $this->tableName WHERE ";
    foreach ($keyColumns as $col) {
      $sql .= " `$col`=:$col AND";
    }
    $sql = substr($sql,0,-4); // remove the last " AND"
    $this->log("FIND: $sql\n");
    $sth = $this->dbh->prepare($sql);
    $key = array();
    foreach ($keyColumns as $k) {
      $key[$k] = $rec[$k];
    }
    $this->beforeQuery($sth);
    $ret = $sth->execute($key);
    if ($ret === false) {
      // Unexpected failure from execute.  Throw an exception
      throw new Exception("Select failed. ".print_r($sth->errorInfo(),true));
    } else {
      $rows = $sth->fetchAll(PDO::FETCH_ASSOC);
    }
    $this->reset();
    return $rows;
  }

  public function clearUpdateCode() {
    // Set the changecode field for all rows to 'nochange'.
    // This must be done prior to starting inserts and updates for the latest upload.
    $sql = "UPDATE $this->tableName SET updatecode='Nochange'";
    $sth = $this->dbh->prepare($sql);
    $this->beforeQuery($sth);
    $ret = $sth->execute();
    if ($ret === false) {
      // Unexpected failure from execute.  Throw an exception
      throw new Exception("Clear updatecode failed. ".print_r($sth->errorInfo(),true));
    }
    $this->reset();
  }

  public function generateSqlCreate($columnSet,$key='id',$type="InnoDB") {
    $sql = "CREATE TABLE IF NOT EXISTS `{$this->tableName}` (";
    if ($key=='id') {
      $sql .= "`id` int(10) unsigned NOT NULL auto_increment,";
    }
    // Remove Updated_On and Updated_By if present, since we add them in further down:
    unset($columnSet['Updated_By']);
    unset($columnSet['Updated_On']);
    foreach ($columnSet as $col) { // cols() removed
      $sql .= "`".$col->name()."` ".$col->getSqlType().",";
    }
    $sql .= "`Updated_By` char(32) default NULL,";
    $sql .= "`Updated_On` date default NULL,";
    if ($key=='id') {
      $sql .= "PRIMARY KEY  (`id`),";
    } else if (is_array($key) && !empty($key)) {
      $sql .= "PRIMARY KEY (";
      foreach ($key as $k) $sql .= "`$k`,";
      $sql = substr($sql,0,-1);
      $sql .= "),";
    }
    $sql = substr($sql,0,-1);
    $charSet = $this->tabularDb->getCharSet();
    if ($charSet == 'utf8') {
      $sql .= ") CHARACTER SET utf8 COLLATE utf8_general_ci ENGINE=$type;"; // TYPE=$type";
    } else {
      $sql .= ") CHARACTER SET $charSet ENGINE=$type;"; // TYPE=$type";
    }
    return $sql;
  }

  public function getTableDescription() {
    $sql = "SELECT * FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA`='{$this->tabularDb->dbName()}' AND TABLE_NAME='{$this->tableName}';";
    $sth = $this->dbh->prepare($sql);
    $this->beforeQuery($sth);
    $ret = $sth->execute();
    $rows = $sth->fetchAll(PDO::FETCH_ASSOC);
    $cols = array();
    foreach ($rows as $row) {
      $colname = $row['COLUMN_NAME'];
      $cols[$colname] = $row;
    }
    $this->reset();
    return $cols;
  }

  public function getPHPcolumnDefs() {
    $cols = $this->getTableDescription();
    $p = "\$columnDefs=array(\n";
    foreach ($cols as $col=>$info) {
      $p .= "  '$col'=>array(\n";
      $len = empty($info['CHARACTER_MAXIMUM_LENGTH']) ? 5 : $info['CHARACTER_MAXIMUM_LENGTH'];
      $displayLen = ($len < 25) ? $len : 25;
      $p .= "    'maxlen' => '$len',\n";
      $p .= "    'type' => 'autocomplete',\n";
      $p .= "    'width' => '{$displayLen}em'\n";
      $p .= "  ),\n\n";
    }
    $p .= ");\n";
    return $p;
  }

  public function checkColumns($columnSet,$autoCreate=false,$autoPopulate=false) {
    $colsInDb = $this->getTableDescription();
    if (empty($colsInDb)) {
      $this->noTableInDb($columnSet,$autoCreate,$autoPopulate);
    } else {
      // Compare the columns in the DB vs. the column Set:
      //
      // When we find one that is in the column Set, but not in DB, we can generate an ALTER command to add it.
    }
  }

  protected function noTableInDb($columnSet,$autoCreate,$autoPopulate) {
    if ($autoCreate) {
      // Create the table
      $key = TabularColumnSet::keysAttrEqual($columnSet,'key',true);
      $sqlCreate = $this->generateSqlCreate($columnSet,$key);
      $this->sql($sqlCreate)->get();
      if ($autoPopulate) {
        // Generate some dummy records in the new Table:
        $this->insertDummyRecords($columnSet);
      }
    } else {
      // Throw an Exception - we dno't have a table to work with.
      throw new Exception("Table does not existing in database: ".$this->tableName);
    }
  }

  protected function insertDummyRecords($columnSet, $n=10) {
    $colsInDb = $this->getTableDescription();
    for ($i=0; $i<$n; $i++) {
      foreach ($colsInDb as $dbCol) {
        echo "Init col data: <br>\n";
        print_r($dbCol);
      }
      exit;
    }
  }

  /**
   * Kind of data we get back from MySQL:
   *  [TABLE_CATALOG] =>
   *  [TABLE_SCHEMA] => testsyseng
   *  [TABLE_NAME] => alarms
   *  [COLUMN_NAME] => Edition
   *  [ORDINAL_POSITION] => 1
   *  [COLUMN_DEFAULT] =>
   *  [IS_NULLABLE] => NO
   *  [DATA_TYPE] => varchar
   *  [CHARACTER_MAXIMUM_LENGTH] => 33
   *  [CHARACTER_OCTET_LENGTH] => 99
   */
  public function getMigration($columnDefs) {
    // Get description of the table from MySQL:
    $dbCols = $this->getTableDescription();
    if (empty($dbCols)) {
      // Table doesn't exist, create it:
      $this->createTable($columnDefs);
    } else {
      // Loop over columns:
      print_r($dbCols);
      //
      foreach ($columnDefs as $col=>$attrs) {
        $col_lower = strtolower($col);
        if (isset($dbCols["$col_lower"])) {
          // Column is present in DB.  Check to see if we need to alter it.
          $colType = TABULAR::getDbColType($columnDefs['type']);
          // ALTER TABLE `alarms_del` CHANGE `Owner` `Owner` VARCHAR( 138 ) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL
          // ALTER TABLE `alarms_del` CHANGE `Owner` `Owner` TEXT CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL
          // ALTER TABLE `alarms_testing` DROP `AD_Dallas_number`
          //
        } else {
          // Column wasn't in DB, add it.
        }
        // Future TBD:
        // Is this col present in the MySQL def?
        //if (present) {
        // Check to see if the MySQL column def needs to be modified...
        // Check for allowable modifications and throw exception if not allowed...
        //} else {
        // Add the new column to the MySQL table...
        //}
      }
      // Future TBD:
      // Loop over columns in table, looking for columns to delete (i.e. those not in $columnDefs):
      //foreach ($dbCols as $col) {
      //}
    }
  }


  public function where($whereClause,$bindings=array()) {
    if (!empty($whereClause)) {
      $this->whereClause = "WHERE $whereClause";
      $this->bindings = $bindings;
    }
    return $this;
  }

  public function whereArray($whereArray,$bindings=array()) {
    if (!empty($whereArray)) {
      $where = 'WHERE ';
      foreach ($whereArray as $k=>$v) {
        $where .= "$k='$v' AND ";
      }
      $where = substr($where,0,-4);
      $this->whereClause = $where;
      $this->bindings = $bindings;
    }
    return $this;
  }

  public function limit($offset=0,$limit=10) {
    $this->limit = $limit;
    $this->offset = $offset;
    return $this;
  }

  public function sql($sql) {
    $this->sql = $sql;
    return $this;
  }

  protected function limitSql() {
    $lim = '';
    if (!empty($this->limit)) {
      $lim = "LIMIT {$this->offset},{$this->limit}";
    }
    $this->limit = 0;
    return $lim;
  }

  public function orderby($order) {
    $this->order = $order;
    return $this;
  }

  protected function orderBySql() {
    $orderBy = '';
    if (!empty($this->order)) {
      $orderBy = "ORDER BY $this->order";
    }
    $this->order = '';
    return $orderBy;
  }

  public function beforeQuery($sth) {
    $this->lastError = '';
    $this->lastQuery = $sth->queryString;
  }

  protected function afterQuery($sth) {
    $this->lastQueryString = $sth->queryString;
  }

  protected function setLastError($error) {
    $this->lastError = $error;
  }

  public function getLastQuery() {
    return $this->lastQuery;
  }

  public function getLastQueryString() {
    return $this->lastQueryString;
  }

  public function getLastError() {
    return $this->lastError;
  }

  public function reset() {
    $this->selected = '*';
    $this->whereClause = '';
    $this->limit = 0;
    $this->order = '';
    $this->bindings = array();
    $this->sql = '';
  }

  public function oneToMany(TabularTable $manyTable, $key) {
    $this->manyTable = $manyTable;
    $this->manyKey = $key;

  }


  /**
   * Deletes all recods from the table.
   */
  public function truncate() {
    $this->sql = "TRUNCATE TABLE $this->tableName";
    $this->get();
  }

  /**
   * Execute a query and return result.
   *
   * @return mixed
   * @throws Exception
   */
  public function get() {
    if (!empty($this->sql)) {
      $sql = $this->sql;
    } else {
      $sql = "SELECT ".$this->selected." FROM {$this->tableName} {$this->whereClause} {$this->limitSql()}{$this->orderBySql()}";
    }
    $this->log($sql);
    $sth = $this->dbh->prepare($sql);
    $this->log($this->bindings);
    $this->beforeQuery($sth);
    $ret = $sth->execute($this->bindings);
    $this->afterQuery($sth);
    if ($ret === false) {
      throw new Exception("TabularTable get() query failed.  ret=$ret. errorInfo():".print_r($sth->errorInfo(),true));
    }
    $rows = $sth->fetchAll(PDO::FETCH_ASSOC);
    $this->log("Returned ".count($rows)." rows.");
    $this->lastQuery = $sql;
    $this->reset();
    //
    if (isset($this->manyTable)) {
      //$tableName = $this->manyTable->getTableName();
      $tableName = 'subrows';
      foreach ($rows as &$row) {
        $where = "";
        foreach ($this->manyKey as $k) {
          $where .= "`$k`='{$row[$k]}' AND ";
        }
        $where = substr($where,0,-5);
        $row[$tableName] = $this->manyTable->where($where)->get();
      }
    }
    return $rows;
  }

  public function getFirst() {
    $rows = $this->get();
    if (!empty($rows))
      return $rows[0];
    else
      return false;
  }

  public function getKeyStr($row, $key, $keyVals=array()) {
    if (!is_array($key)) {
      $key=explode(',',$key);
    }
    $where = '';
    foreach ($key as $k) {
      if (isset($keyVals[$k])) {
        $where .= "`$k`='{$keyVals[$k]}' AND ";
      } else if (isset($row[$k])) {
        $where .= "`$k`='{$row[$k]}' AND ";
      } else {
        throw new Exception("Key field ($k) not found in provided array.");
      }
    }
    $where = substr($where,0,-5);
    return $where;
  }

  /**
   * diff - Diff of one row.
   *
   * @param array $row
   * @param array or comm-separated-string $key
   * @param array $keyVals
   * @param boolean $wouldInsert
   *
   * @return array
   */
  public function diff($row,$key,$keyVals=array(),$wouldInsert=true) {
    $rec = $this->getby($row,$key,$keyVals);
    $where = $this->getKeyStr($row, $key, $keyVals);
    $diffRec['key'] = $where;
    if (empty($rec)) {
      $diffRec['diff'] = $wouldInsert ? 'Insert' : 'NotFound';
      $diffRec['O'] = array();
      $diffRec['N'] = $row;
      $diffRec['deltaCols'] = array();
    } else {
      // TODO: why not make a separate class for diffRec?
      $diffRec['diff'] = 'NoChange';
      $diffRec['O'] = $rec;
      $diffRec['N'] = $rec;
      $diffRec['deltaCols'] = array();
      foreach ($rec as $k => $v) {
        if (isset($row[$k]) && ($v != $row[$k])) {
          $diffRec['diff'] = 'Update';
          $diffRec['deltaCols'][] = $k;
          $diffRec['N'][$k] = $row[$k];
        }
      }
    }
    return $diffRec;
  }

  public function getDiffs($rows, $key, $keyVals=array(), $wouldInsert=true) {
    $diffs = array();
    foreach ($rows as $row) {
      try {
        $diff = $this->diff($row,$key,$keyVals,$wouldInsert);
      } catch(Exception $e) {
        echo $e->getMessage()."<br>";
        continue;
      }
      $diffs[] = $diff;
    }
    return $diffs;
  }

  public function getby($row,$key='id',$keyVals=array()) {
    if (!is_array($key)) {
      $key=explode(',',$key);
    }
    try {
      $where = $this->getKeyStr($row, $key, $keyVals);
    } catch (Exception $e) {
      // We could not extract a key value, so just return an empty array.
      $this->reset();
      return array();
    }
    $rows = $this->where($where)->get();
    $c = count($rows);
    switch ($c) {
      case 0:
        $result = array();
        break;
      case 1:
        $result = $rows[0];
        break;
      default:
        throw new Exception("Multiple rows found ($c) for key $where.  Zero or one expected.");
    }
    $this->reset();
    return $result;
  }

  public function distinct($col,$withCounts=false,$ignoreSpaces=true) {
    if ($withCounts) {
      $sql = "SELECT DISTINCT `$col`,count(*) as count FROM $this->tableName {$this->whereClause} GROUP BY {$col}{$this->orderBySql()}";
    } else {
      $sql = "SELECT DISTINCT `$col` FROM $this->tableName {$this->whereClause}{$this->orderBySql()}";
    }
    $sth = $this->dbh->prepare($sql);
    $this->log($sql);
    $this->beforeQuery($sth);
    $ret = $sth->execute();
    if ($ret === false) {
      throw new Exception("TabularTable distinct() query failed.  ret=$ret. errorInfo():".print_r($sth->errorInfo(),true));
    }
    $rows = $sth->fetchAll(PDO::FETCH_ASSOC);

    if ($withCounts) {
      $result = $rows;
    } else {
      $result = array();
      $uniqueKeys = array();
      foreach ($rows as $row) {
        if ($ignoreSpaces) {
          // We will ignore spaces in building the result.
          $k = str_replace(' ', '', $row[$col]);
          if (!isset($uniqueKeys[$k])) {
            $result[] = $row[$col];
            $uniqueKeys[$k] = true;
          }
        } else {
          $result[] = $row[$col];
        }
      }
    }
    $this->log($sql);
    $this->lastQuery = $sql;
    $this->log("Returned ".count($rows)." rows.");
    $this->reset();
    return $result;
  }

  /**
   * delete - Delete a row from the table, and optionally record a history of the deletion.
   *
   * @param array $row
   * @param array or comma-separated-string $key
   * @param string $cri (optional)
   * @param string $criKeyField (optional)
   *
   * @throws Exception
   */
  public function delete($row,$key) {
    $cri = '';
    if (!is_array($key)) {
      $key=explode(',',$key);
    }

    // Copy row to the deleted table:
    $deletedRow = $this->copyToDeleted($row,$key);

    // Do the deletion:
    $sql = "DELETE FROM $this->tableName WHERE ";
    $delKeys = array();
    foreach ($key as $k) {
      $sql .= "`$k`=:$k AND ";
      $delKeys[$k] = $row[$k];
    }
    $sql = substr($sql,0,-5);
    $sql .= " LIMIT 1";
    $sth = $this->dbh->prepare($sql);

    $this->beforeQuery($sth);
    if ($this->execute) {
      $ret = $sth->execute($delKeys);
    } else {
      $ret = true;
    }
    $this->log($sql);
    $this->lastQuery = $sql;
    if ($ret == false) {
      throw new Exception("TabularTable delete failed.  ret=$ret. errorInfo():".print_r($sth->errorInfo(),true));
    }
    // Return number of rows affected:
    $c = $sth->rowCount();

    // If there is a history key set, then use it in call to writeHistory:
    if (!empty($this->historyKey)) {
      $key = $this->historyKey;
      // Make sure the key is set in the row being passed to writeHistory.
      $row[$key] = $deletedRow[$key];
      $this->log("Delete using historyKey {$this->historyKey}, value: {$deletedRow[$key]}");
    }
    $this->writeHistory('delete',$cri,$key,$row);

    $this->log("DELETE affected $c rows");
    return $c;
  }


  public function makeWhere($row,$key) {
    if (!is_array($key)) {
      $key=explode(',',$key);
    }
    $w = '';
    foreach ($key as $k) {
      // TODO: SQL injection possible here?
      $w .= "`$k`='{$row[$k]}' AND ";
    }
    $w = substr($w,0,-5);
    return $w;
  }

  /**
   * Copies a row that is being deleted to a "deleted rows" table.
   *
   * @param $row
   * @param $key
   * @return $row - the row as it exists in the database table.
   * @throws Exception
   */
  protected function copyToDeleted($row,$key) {
    $delTblName = $this->tableName.'_del';
    $delTbl = new TabularTable($this->tabularDb,$delTblName,$this->userName,$this->logger, true, false);
    // Read the record
    $w = $this->makeWhere($row,$key);
    $row = $this->where($w)->getFirst();
    if ($row) {
      // Insert it into del table:
      $delTbl->insert($row);
    } else {
      // Unusual case:  the row we are deleting can't be found.  Race condition?
      // TODO: Add logging for this kind of thing.
      // For now throw exception
      throw new Exception("Could not copy row being deleted");
    }
    return $row;
  }

}