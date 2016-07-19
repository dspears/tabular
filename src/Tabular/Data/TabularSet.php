<?php

namespace Tabular\Data;

/**
 * Class TabularSet
 *
 * @author David Spears
 *
 */
class TabularSet {
  /**
   * Filters the input array by keeping only those entries that have a specified attribute name.
   * The name of the attribute is passed in the 2nd parameter.
   *
   * @param array $array - Array to filter over.
   * @param string $name - Name of attribute to look for.
   * @return array
   */
  public static function filterHasAttr($array,$name) {
    return __::filterk($array,function($o) use($name) { return $o->hasAttr($name); });
  }

  /**
   * Rejects elements of the input array, eliminating those that have a specified attribute name.
   * The name of the attribute is passed in the 2nd parameter.
   *
   * @param array $array - Array Kto filter over.
   * @param string $name - Name of attribute to look for.
   * @return array
   */
  public static function rejectHasAttr($array,$name) {
    return __::reject($array,function($o) use($name) { return $o->hasAttr($name); });
  }

  /**
   * Filters the input array by keeping only those entries with an attribute matching a specific value.
   * The name of the attribute is passed in the 2nd parameter, and the value to match on is passed in
   * the 3rd parameter.
   *
   * @param array $array - Array to filter over.
   * @param string $name - Name of attribute to look for.
   * @param string $val - Value of attribute to match on.
   * @return array - Filtered set of array entries from the original input array.
   */
  public static function filterAttrEqual($array,$name,$val) {
    return __::filterk($array,function($o) use($name,$val) { return $o->hasAttr($name) && $o->getAttr($name)==$val; } );
  }

  /**
   * Filters by attribute value, then returns an array of just the keys of entries where a match was found.
   *
   * @param $array - Array to filter over.
   * @param $name - Name of attribute to look for.
   * @param $val - Value of attribute to match on.
   * @return array - Keys of the filtered set of array entries.
   */
  public static function keysAttrEqual($array,$name,$val) {
    return __::keys(TabularSet::filterAttrEqual($array,$name,$val));
  }

  /**
   * Filters the input array by keeping only those entries with an attribute matching a specific value, except if the value
   * (in the 3rd argument) is "All", then no filtering is performed and a copy of the input array (1st parameter) is returned.
   *
   * @param array $array - Array to filter over.
   * @param string $name - Name of attribute to look for.
   * @param string $val - Value of attribute to match on.
   * @return array
   */
  public static function filterWithAll(array $array,$name,$val) {
    if (strtoupper($val)!="ALL") {
      $array = TabularSet::filterAttrEqual($array,$name,$val);
    }
    return $array;
  }

  /**
   * Filters the input array by keeping only those entries with an attribute comma separated value that includes a
   * specific value being searched for (i.e. if the attribute value is "SE,OAM", then either SE or OAM would be
   * found and therefore pass the filter).
   * The name of the attribute is passed in the 2nd parameter, and the value to match on is passed in
   * the 3rd parameter.
   *
   * @param array $array - Array to filter over.
   * @param string $name - Name of attribute to look for.
   * @param string $val - Value of attribute to match on.
   * @return array
   */
  public static function filterAttrHasVal($array,$name,$val) {
    return __::filterk($array,function($o) use($name,$val) {
      $valueList = explode(',',$o->getAttr($name));
      return $o->hasAttr($name) && in_array($val,$valueList);
    } );
  }

  /**
   * Filters the input array by keeping only those entries with an attribute matching a specific value, except if the value
   * (in the 3rd argument) is "All", then no filtering is performed and a copy of the input array (1st parameter) is returned.
   *
   * @param array $array - Array to filter over.
   * @param string $name - Name of attribute to look for.
   * @param string $val - Value of attribute to match on.
   * @return array
   */
  public static function filterHasValOrAll(array $array,$name,$val) {
    if (strtoupper($val)!="ALL") {
      $array = TabularSet::filterAttrHasVal($array,$name,$val);
    }
    return $array;
  }
}
