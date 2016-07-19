<?php
/**
 * Class TabularFilterSet
 *
 * @author David Spears
 *
 */
class TabularFilterSet extends TabularSet {
  public static function makeFilter(TabularColumn $col) {
    $filterType = $col->getAttr('filterType');
    if (empty($filterType)) {
      return new TabularFilter($col);
    } else {
      $filterClass = "Tabular{$filterType}Filter";
      if (class_exists($filterClass)) {
        FB::log("Creating $filterClass filter on {$col->name()}");
        return new $filterClass($col);
      } else {
        throw new Exception("Unknown filter type. Can't find class: $filterType, column name: {$col->name()}");
      }
    }
  }

  public static function make(array $columnSet, $subset=array()) {
    if (!empty($subset)) {
      $columnSet = __::pick($columnSet,$subset);
    }
    $result = array();
    foreach ($columnSet as $key=>$col) {
      $result[$key] = TabularFilterSet::makeFilter($col);
    }
    return $result;
  }

  public static function getWhereStr(array $filterSet) {
    $s = "";
    foreach ($filterSet as $filter) {
      $s.= $filter->getWhereStr();
    }
    $s = substr($s,4);
    return $s;
  }
}