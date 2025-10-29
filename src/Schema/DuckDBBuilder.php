<?php

namespace GumPHP\LaravelDuckDB\Schema;

use Illuminate\Database\Schema\Builder;

class DuckDBBuilder extends Builder
{
    /**
     * Determine if the given table exists.
     *
     * @param  string  $table
     * @return bool
     */
    public function hasTable($table)
    {
        $table = $this->connection->getTablePrefix().$table;

        return count($this->connection->select(
            $this->grammar->compileTableExists(null, $table), [$table]
        )) > 0;
    }

    /**
     * Get the column listing for a given table.
     *
     * @param  string  $table
     * @return array
     */
    public function getColumnListing($table)
    {
        $table = $this->connection->getTablePrefix().$table;

        $results = $this->connection->select(
            $this->grammar->compileColumnListing($table), [], false
        );

        return $this->connection->getPostProcessor()->processColumnListing($results);
    }
    
    /**
     * Get the tables that belong to the database.
     *
     * @param  string|null  $schema
     * @return array
     */
    public function getTables($schema = null)
    {
        $results = $this->connection->select(
            "SELECT name, sql FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        );
        
        return array_map(function ($result) {
            // Handle both array and object results
            $name = is_object($result) ? $result->name : $result['name'];
            
            return [
                'name' => $name,
                'schema' => null, // DuckDB doesn't have schemas like other databases
                'size' => null, // Size information not readily available
                'comment' => null, // Comments not supported in the same way
                'collation' => null, // Collation not applicable
                'engine' => 'duckdb', // Database engine
            ];
        }, $results);
    }
}