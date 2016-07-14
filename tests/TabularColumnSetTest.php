<?php
// Pull in the app environment configuration:
require_once "config/config.php";
// Pull in the Tabular Toolkit:
require_once "../TabularToolkit.php";
// Example columns definitions:
require_once "columns.php";

class TabularColumnSetTest extends PHPUnit_Framework_TestCase {

  public function testConstruction() {
    global $columnDefs;
    $columnSet = TabularColumnSet::make($columnDefs);
    $this->assertEquals(count($columnDefs),count($columnSet));
    $this->assertEquals(__::keys($columnDefs),__::keys($columnSet));
    return $columnSet;
  }


  /**
   * @depends testConstruction
   */
  public function testKeysAttrEqual(array $columnSet) {
    $keys = TabularSet::keysAttrEqual($columnSet,'key',true);
    $expectedKeys = array('Edition','AD_alarmID');
    $this->assertEquals($keys,$expectedKeys);
  }

}