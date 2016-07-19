<?php

namespace Tabular\Data;

/**
 * Class TabularCsvFile - Handles reading a CSV file, with a few special options.
 *
 * @author David Spears
 *
 */
class TabularCsvFile {
  protected $filename;
  protected $colMap;
  protected $columns;
  protected $rows;
  protected $distinctColValues;
  protected $suppressNoNameCols;
  protected $delimiter;
  protected $rowSet;

  /**
   * Constructor.
   *
   * @param $filename - Path to the CSV file.
   * @param array $colMap
   * @param bool $suppressNoNameCols
   */
  public function __construct($filename,$colMap=array(),$suppressNoNameCols=false) {
    $this->filename = $filename;
    $this->colMap = $colMap;
    $this->suppressNoNameCols = $suppressNoNameCols;
    $this->delimiter = ',';
  }

  /**
   * Get or Set the delimeter.  Pass a value to set, call with no parameter to get.
   *
   * @param string $d
   * @return $this|string
   */
  public function delimiter($d='') {
    if (empty($d)) {
      return $d;
    } else {
      $this->delimiter = $d;
      return $this;
    }
  }

  /**
   * Perform the file read, and return the resulting rows.
   * (Just a shortcut for calling both read() and getRows())
   *
   * @return mixed
   * @throws Exception
   */
  public function get() {
    $this->read();
    return $this->getRows();
  }

  /**
   * Reads all records from the given CSV file.  The first line of the CSV file is assumed to be the column names.
   *
   * The column map (if any) provided to the constructor is used as follows:
   *   The column name from the first line of the CSV file is used lookup an entry in the column map.
   *   If found, the value in the column map is used as the column name instead of the column name in the file.
   *   If not found, the column is still read in, but using the column name found in the CSV file.
   *   If an entry is found in the column map, but the value is an empty string, then the column is ignored.
   *   For example, for:
   *
   *     $colMap = array('Col1'=>'FirstColumn','Col2'=>'Col2','Col3'=>'');
   *
   *   Col1 will be renamed to FirstColumn in the array.
   *   Col2 will remain as Col2 in the array.
   *   Col3 will be ignored (not read into the array).
   *   Any other column in the CSV file will be read into the array using the column name found in the first line of the CSV.
   *
   * @throws Exception "CSV file not found".
   *
   * @return array of rows from the CSV file.
   */
  public function read() {
    $row = 1;
    $this->columns = array();
    $this->rows = array();
    if (($handle = fopen($this->filename, "r")) !== FALSE) {
      while (($data = fgetcsv($handle,0,$this->delimiter)) !== FALSE) {
        $num = count($data);
        if ($row==1) {
          for ($c=0; $c < $num; $c++) {
            $h = TABULAR::makeValidColName($data[$c]);
            $this->columns[] = $h;
          }
        }  else {
          $rec = array();
          // If we encounter a row that is completely blank, we will not add that row to the results.
          $allEmpty = true;
          for ($c=0; $c < $num; $c++) {
            if (isset($this->colMap[$this->columns[$c]])) {
              $column = $this->colMap[$this->columns[$c]];
            } else {
              $column = $this->columns[$c];
            }
            if ((!empty($column)) || !$this->suppressNoNameCols) {
              $d = trim($data[$c]);
              $d = str_replace("\x0D","",$d); // remove MS-DOS carriage returns (^M)
              $rec[$column] = $d;
              if ($d != '') {
                $allEmpty = false;
              }
            }
          }
          if (!$allEmpty) {
            $this->rows[] = $rec;
          }
        }
        $row++;
      }
      fclose($handle);
    } else {
      throw new Exception("CSV File not found: $this->filename");
    }
    // Return $this to allow method chaining.
    return $this;
  }


  protected function append($row1,$row2) {
    foreach ($row2 as $k=>$v) {
      if (!empty($row2[$k])) {
        if (!empty($row1[$k]))  $row1[$k] .= "\n";
        $row1[$k] .= $row2[$k];
      }
    }
    return $row1;
  }

  public function mergeCells($doAppendFn) {
    $newRows = array();
    $i = -1;
    foreach ($this->rows as $row) {
      if (!$doAppendFn($row)) {
        $i++;
      }
      $newRows[$i] = $this->append($newRows[$i], $row);
    }
    return $newRows;
  }

  public function getColumns() {
    return $this->columns;
  }

  public function getRows() {
    return $this->rows;
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


  public function handleMergedCells($rows,$colsToMerge) {
    $newRows = array();
    $newVal = array();
    $prevVal = array();
    foreach ($rows as $row) {
      $newRow = $row;
      foreach ($colsToMerge as $col) {
        $newVal[$col] = trim($newRow[$col]);
        if ($newVal[$col]=="") {
          $newRow[$col] = $prevVal[$col];
        } else {
          $prevVal[$col] = $newVal[$col];
        }
      }
      $newRows[] = $newRow;
    }
    return $newRows;
  }

}
