<?php
/**
 * Class TabularColumnSet
 *
 * @author David Spears
 *
 */
class TabularColumnSet extends TabularSet {
  // Make an object for a single column:
  public static function makeCol($colName,$colDef) {
    // Allow for very simple column definition that is just the column name as a string value:
    if (!is_array($colDef)) {
      $colName = $colDef;
      $colDef = array('type'=>'text');
    }
    $col = TabularColumn::create($colName,$colDef);
    return $col;
  }

  // Make objects for a whole array of columns:
  public static function make($columnDefs) {
    $result = array();
    foreach ($columnDefs as $colName=>$colDef) {
      if (isset($result[$colName])) {
        throw new Exception("Attempt to create column that already exists: $colName");
      }
      $result[$colName] = TabularColumnSet::makeCol($colName,$colDef);
    }
    return $result;
  }
}