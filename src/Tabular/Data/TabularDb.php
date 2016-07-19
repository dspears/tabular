<?php
/**
 * Class TabularDb - Represents a connection to a MySQL database.  You can have multiple of these if you need
 * connections to more  than one DB in your app.
 *
 * This is basically a wrapper around a PHP PDO object.
 *
 *  @author: David Spears
 */
class TabularDb {
  protected $dbh;
  protected $host;
  protected $db;
  protected $user;
  protected $passwd;
  protected $charset;

  /**
   * Construct a TabularDb database connection.
   *
   * @param $host - Hostname where MySQL is running
   * @param $db - Name of the database
   * @param $user - MySQL user
   * @param $passwd - MySQL user's password
   * @param string $charset - Character set to use.  Default to latin1, which is essentially the same as cp1252,
   *                          Windows-1252 (used to be the MySQL default.  utf8 recommended for all new applications.)
   */
  public function __construct($host,$db,$user,$passwd,$charset='latin1') {
    $this->host = $host;
    $this->db = $db;
    $this->user = $user;
    $this->passwd = $passwd;
    $charset = strtolower($charset);
    if ($charset == 'utf-8') $charset = 'utf8';
    $this->charset = $charset;
    switch ($charset) {
      case 'utf8':
        $this->dbh = new PDO("mysql:host=".$host.";dbname=$db;charset=utf8", $user, $passwd, array(PDO::MYSQL_ATTR_INIT_COMMAND => "SET NAMES utf8") );
        break;
      default:
        $this->dbh = new PDO("mysql:host=".$host.";dbname=$db;charset=$charset", $user, $passwd);
    }
  }

  public function __destruct() {
    unset($this->dbh);
  }

  public function dbh() {
    return $this->dbh;
  }
  public function dbName() {
    return $this->db;
  }

  public function getCharSet() {
    return $this->charset;
  }
}