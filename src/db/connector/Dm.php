<?php
/**
 * Author: 进军的肥天鹅 <15168759021@189.cn>
 * Date: 2025/06/03
 * Time: 10:05
 */

namespace think\db\connector;

use PDO;
use think\db\PDOConnection;

/**
 *  达梦数据库驱动
 */
class Dm extends PDOConnection
{
    /**
     * 解析pdo连接的dsn信息
     * @access protected
     * @param array $config 连接信息
     * @return string
     */
    protected function parseDsn(array $config): string
    {
        $dsn = 'dm:host=' . $config['hostname'];

        if (!empty($config['hostport'])) {
            $dsn .= ':' . $config['hostport'];
        }
        return $dsn;
    }


    /**
     * 取得数据表的字段信息
     * @access public
     * @param string $tableName
     * @return array
     */
    public function getFields($tableName): array
    {
        $config = $this->getConfig();
        $sql = "select * from all_tab_columns where table_name='{$tableName}' AND OWNER='{$config['username']}'";
        $pdo    = $this->getPDOStatement($sql);
        $result = $pdo->fetchAll(PDO::FETCH_ASSOC);
//        $pdo = $this->query($sql, [], false, true);
//        $result = $pdo->fetchAll(PDO::FETCH_ASSOC);
        $info = [];

        if ($result) {
            foreach ($result as $key => $val) {
                $val = array_change_key_case($val);
                $info[$val['column_name']] = [
                    'name' => $val['column_name'],
                    'type' => $val['data_type'],
                    'notnull' => 'Y' === $val['nullable'],
                    'default' => $val['data_default'],
                    'primary' => $val['column_name'] === 'id',
                    'autoinc' => false,
                ];
            }
        }

        return $this->fieldCase($info);
    }

    /**
     * 取得数据库的表信息
     * @access   public
     * @param string $dbName
     * @return array
     */
    public function getTables(string $dbName = ''): array
    {
        $config = $this->getConfig();
        $sql = "select table_name from all_tables where OWNER='{$config['username']}'";
        $pdo = $this->getPDOStatement($sql);
        $result = $pdo->fetchAll(PDO::FETCH_ASSOC);
        $info = [];

        foreach ($result as $key => $val) {
            $info[$key] = current($val);
        }

        return $info;
    }

    /**
     * SQL性能分析
     * @access protected
     * @param string $sql
     * @return array
     */
    protected function getExplain($sql): array
    {
        return [];
    }

    protected function supportSavepoint(): bool
    {
        return true;
    }
}

