<?php
// Pull in the app environment configuration:
require_once "config/config.php";
// Pull in the Tabular Toolkit:
require_once "../TabularToolkit.php";

class TabularTableTest extends PHPUnit_Framework_TestCase {
  protected $db;

  protected function setUp() {
    $this->db = new TabularDb(HOST, DB, MYSQL_LOGIN, MYSQL_PASSWORD);
  }

  protected function tearDown() {
    unset($this->db);
  }

  /**
   *
   */
  public function testReadTable() {
    global $firephp;
    $this->assertInstanceOf('PDO',$this->db->dbh());
    // Setup a DB Table object:
    $tableName = 'alarms';
    $table = new TabularTable($this->db,$tableName,'testuser', $firephp, true);
    // TODO: Without the where clause, this could try to read too many records and PHP will
    //       generate an out of memory error.  The toolkit should detect that and generate
    //       some sort of more useful error message.
    $rows = $table->where("Edition='TEST DATA Edition 2'")->get();
    // We know there will be at least 1200 records in the table:
    $this->assertGreaterThan(1200,count($rows));
  }

  /**
   * @expectedException Exception
   */
  public function testReadOfNonExistingTable() {
    global $firephp;
    $this->assertInstanceOf('PDO',$this->db->dbh());
    // Setup a DB Table object:
    $tableName = 'sometable';
    $table = new TabularTable($this->db,$tableName,'testuser', $firephp, true);
    $rows = $table->get();
    $this->assertEmpty($rows);
  }

  /**
   * Check table names when table history is turned on.
   */
  public function testTableNamesWithHistory() {
    global $firephp;
    $tableName = 'alarms';
    $table = new TabularTable($this->db,$tableName,'testuser',$firephp,true,true);
    $names = $table->getTableNames();
    $namesArray = explode(',',$names);
    $this->assertContains($tableName,$namesArray);
    $this->assertContains($tableName."_change_log",$namesArray);
    $this->assertCount(2,$namesArray);
  }

  /**
   * Check table names when table history is turned off.
   */
  public function testTableNamesWithoutHistory() {
    global $firephp;
    $tableName = 'alarms';
    $table = new TabularTable($this->db,$tableName,'testuser',$firephp,true,false);
    $names = $table->getTableNames();
    $namesArray = explode(',',$names);
    $this->assertContains($tableName,$namesArray);
    $this->assertCount(1,$namesArray);
  }

}