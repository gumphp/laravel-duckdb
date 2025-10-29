<?php

namespace GumPHP\LaravelDuckDB\Query\Grammars;

use Illuminate\Database\Query\Grammars\Grammar;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\Connection;

class DuckDBGrammar extends Grammar
{
    /**
     * The keyword identifier wrapper format.
     *
     * @var string
     */
    protected $wrapper = '"%s"';

    /**
     * Create a new grammar instance.
     *
     * @param  \Illuminate\Database\Connection  $connection
     */
    public function __construct(Connection $connection)
    {
        parent::__construct($connection);
    }

    /**
     * Compile a create table command.
     *
     * @param  \Illuminate\Database\Schema\Blueprint  $blueprint
     * @param  \Illuminate\Support\Fluent  $command
     * @return string
     */
    public function compileCreate($blueprint, $command)
    {
        $columns = implode(', ', $this->getColumns($blueprint));

        $sql = 'create table '.$this->wrapTable($blueprint)." ({$columns})";

        return $sql;
    }

    /**
     * Compile an insert ignore statement into SQL.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array  $values
     * @return string
     */
    public function compileInsertOrIgnore($query, $values)
    {
        return substr_replace($this->compileInsert($query, $values), ' insert or ignore', 0, 6);
    }

    /**
     * Compile the columns for an update statement.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array  $values
     * @return string
     */
    protected function compileUpdateColumns(Builder $query, array $values)
    {
        return collect($values)->map(function ($value, $key) {
            if ($value instanceof \Illuminate\Contracts\Database\Query\Expression) {
                return $this->wrap($key).' = '.$value->getValue($this);
            }

            return $this->wrap($key).' = '.$this->parameter($value);
        })->implode(', ');
    }

    /**
     * Compile a "where null" clause.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array  $where
     * @return string
     */
    protected function whereNull($query, $where)
    {
        return $this->wrap($where['column']).' is null';
    }

    /**
     * Compile a "where not null" clause.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array  $where
     * @return string
     */
    protected function whereNotNull($query, $where)
    {
        return $this->wrap($where['column']).' is not null';
    }

    /**
     * Compile a "where date" clause.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array  $where
     * @return string
     */
    protected function whereDate($query, $where)
    {
        $value = $this->parameter($where['value']);

        return 'date('.$this->wrap($where['column']).') '.$where['operator'].' '.$value;
    }

    /**
     * Compile a "where time" clause.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array  $where
     * @return string
     */
    protected function whereTime($query, $where)
    {
        $value = $this->parameter($where['value']);

        return 'time('.$this->wrap($where['column']).') '.$where['operator'].' '.$value;
    }

    /**
     * Compile a "where day" clause.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array  $where
     * @return string
     */
    protected function whereDay($query, $where)
    {
        $value = $this->parameter($where['value']);

        return 'day('.$this->wrap($where['column']).') '.$where['operator'].' '.$value;
    }

    /**
     * Compile a "where month" clause.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array  $where
     * @return string
     */
    protected function whereMonth($query, $where)
    {
        $value = $this->parameter($where['value']);

        return 'month('.$this->wrap($where['column']).') '.$where['operator'].' '.$value;
    }

    /**
     * Compile a "where year" clause.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array  $where
     * @return string
     */
    protected function whereYear($query, $where)
    {
        $value = $this->parameter($where['value']);

        return 'year('.$this->wrap($where['column']).') '.$where['operator'].' '.$value;
    }
}