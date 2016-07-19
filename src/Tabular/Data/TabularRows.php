<?php

namespace Tabular\Data;

/**
 * Class TabularRows - Supports various useful operations on arrays of rows.  The rows might have come from
 * a database read, or from loading a CSV file.  Some of the methods here are for generating PHP source code
 * to represent the columns in the dataset.
 *
 * @author David Spears
 *
 */
class TabularRows {
  protected $columns;
  protected $rows;

  public function __construct($rows) {
    $this->rows = $rows;
    $this->columns = array_keys($rows[0]);
  }

  public function filter($fn) {
    $newRows = array();
    foreach ($this->rows as $k=>$row) {
      if ($fn($row)) $newRows[] = $row;
    }
    $this->rows = $newRows;
    return $this;
  }

  public function getDistinctValues() {
    $colVals = array();
    foreach ($this->columns as $col) {
      $colVals[$col] = array();
      foreach ($this->rows as $row) {
        $val = $row[$col];
        //echo "col: $col val: $val<br>\n";
        if (isset($colVals[$col][$val])) {
          $colVals[$col][$val]++;
        } else {
          $colVals[$col][$val]=1;
        }
      }
    }
    return $colVals;
  }

  public function getColumnDescriptions($pulldownThreshold=0.03, $lengthThreshold=64) {
    $descriptions = array();
    $colVals = $this->getDistinctValues();
    $total = count($this->rows);
    $pulldownCutoff = $total * $pulldownThreshold;
    foreach ($colVals as $col=>$valueCounts) {
      $numBlankRows = $valueCounts[''];
      $values = array_keys($valueCounts);
      natsort($values);
      $c = count($values);
      list($min,$max) = $this->getMinAndMaxLengths($values);
      $descriptions[$col]['minlen'] = $min;
      $descriptions[$col]['maxlen'] = $max;
      if (($c > 1) && ($c <= $pulldownCutoff) && ($max<=$lengthThreshold)) {
        // Looks like a pulldown
        $type = "pulldown";
        $descriptions[$col]['values'] = $values;
      } else if ($c == $total) {
        // A unique key field or some freeform input that always differs:
        if ($this->valuesContain(' ',$values)) {
          // Since the values contain spaces, it's probably freeform text
          $type = "text";
        } else {
          // Assume a key value
          $type = "key";
        }
      } else {
        // assume an autocomplete field:
        $type = "autocomplete";
      }
      $descriptions[$col]['type'] = $type;
    }
    return $descriptions;
  }

  public function valuesContain($needle,$haystack) {
    foreach ($haystack as $val) {
      if (strpos($val,$needle) !== false) {
        return true;
      }
    }
    return false;
  }

  public function getMinAndMaxLengths($data) {
    $min = PHP_INT_MAX;
    $max = -1;

    foreach ($data as $a) {
      $length = strlen($a);
      $max = max($max, $length);
      $min = min($min, $length);
    }
    return array($min,$max);
  }

  public function generatePHPcolumns($columnDescriptions) {
    $p = "\$columnDefs=array(\n";
    foreach ($columnDescriptions as $col => $attrs) {
      $p .= "  '$col'=>array(\n";
      foreach ($attrs as $k => $v) {
        if (is_array($v)) {
          $p .= "       '$k'=>array(\n";
          foreach ($v as $k2 => $v2) {
            $p .= "             '$v2',\n";
          }
          $p .= "             ),\n";
        } else {
          $p .= "       '$k'=>'$v',\n";
        }
      }
      $p .= "       ),\n";
    }
    $p .= ");\n";
    return $p;
  }
}