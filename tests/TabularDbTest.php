<?php
// Pull in the app environment configuration:
require_once "config/config.php";
// Pull in the Tabular Toolkit:
require_once "../TabularToolkit.php";

class TabularDbTest extends PHPUnit_Framework_TestCase {

  public function testConstruction() {
    $db = new TabularDb(HOST, DB, MYSQL_LOGIN, MYSQL_PASSWORD);
    $this->assertInstanceOf('PDO',$db->dbh());
    $this->assertEquals(DB,$db->dbName());
  }


  /**
   * @expectedException PDOException
   */
  public function testBadPasswordException() {
    define('BAD_PASSWORD','foo');
    $db = new TabularDb(HOST, DB, MYSQL_LOGIN, BAD_PASSWORD);
  }

}