<?php
/**
 * Author: 进军的肥天鹅 <15168759021@189.cn>
 * Date: 2025/06/03
 * Time: 10:05
 */

namespace think\db\builder;

use think\db\Builder;
use think\db\Expression;
use think\db\Query;
use think\db\Raw;

/**
 * 达梦数据库驱动
 */
class Dm extends Builder
{
    /**
     * 字段和表名处理
     * @access protected
     * @param mixed $key
     * @param array $options
     * @return string
     */
    public function parseKey(Query $query, $key, bool $strict = false): string
    {
        if (is_numeric($key)) {
            return $key;
        } elseif ($key instanceof Expression) {
            return $key->getValue();
        }
        $key = trim($key);

        if (strpos($key, '.')) {

            list($table, $key) = explode('.', $key, 2);

            list($table, $key) = explode('.', $key, 2);

            $alias = $query->getOptions('alias');

            if ('__TABLE__' == $table) {
                $table = $query->getOptions('table');
                $table = is_array($table) ? array_shift($table) : $table;
            }

            if (isset($alias[$table])) {
                $table = $alias[$table];
            }
            //wth
            $table = '"' . $table . '"';
        }

        $key = str_replace('`', '', $key);
        if ('*' != $key && !preg_match('/[,\'\"\*\(\).\s]/', $key)) {
            $key = '"' . $key . '"';
        }

        //达梦的JSON查询方式，修改兼容 JSON_VALUE("字段（要么双引号，要么不要引号）", '$.JSON字段（$.一定要加）') = '值'
        //就像这样 JSON_VALUE("param", '$.param_21') = '皮带'
        if (strpos($key, '->') && false === strpos($key, '(')) {
            $key = ltrim($key,'"');
            $key = rtrim($key,'"');
            list($field, $name) = explode('->', $key, 2);
            $key = "JSON_VALUE(" . $this->parseKey($query, $field) . ", '$." . str_replace("'", "''", $name) . "')";
        }

        if (isset($table)) {
            $key = $table . '.' . $key;
        }
        return $key;
    }


    /**
     * field分析
     * @access protected
     * @param  Query     $query     查询对象
     * @param  mixed     $fields    字段名
     * @return string
     */
    protected function parseField(Query $query, $fields): string
    {
        if (is_array($fields)) {
            // 支持 'field1'=>'field2' 这样的字段别名定义
            $array = [];

            foreach ($fields as $key => $field) {
                if ($field instanceof Raw) {
                    $array[] = $this->parseRaw($query, $field);
                } elseif (!is_numeric($key)) {
                    $array[] = $this->parseKey($query, $key) . ' AS ' . $this->parseKey($query, $field, true);
                } else {
                    $array[] = $this->parseKey($query, $field);
                }
            }

            $fieldsStr = implode(',', $array);
        } else {
            $fieldsStr = '*';
        }

        return $fieldsStr;
    }


    /**
     * 随机排序
     * @access protected
     * @param Query $query 查询对象
     * @return string
     */
    protected function parseRand(Query $query): string
    {
        return 'RAND()';
    }
}
