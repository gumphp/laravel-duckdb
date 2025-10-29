<?php

namespace GumPHP\LaravelDuckDB;

use Illuminate\Database\Connectors\Connector;
use Illuminate\Database\Connectors\ConnectorInterface;
use Fnvoid\DuckDB\DuckDB;
use Fnvoid\DuckDB\Exception as DuckDBException;

class DuckDBConnector extends Connector implements ConnectorInterface
{
    /**
     * Establish a database connection.
     *
     * @param  array  $config
     * @return \Fnvoid\DuckDB\DuckDB
     */
    public function connect(array $config)
    {
        // Extract DuckDB-specific configuration
        $database = $config['database'] ?? ':memory:';
        $configOptions = $config['config'] ?? [];

        try {
            // Create DuckDB connection
            if (!empty($configOptions)) {
                return new DuckDB($database, $configOptions);
            } else {
                return new DuckDB($database);
            }
        } catch (DuckDBException $e) {
            throw new \PDOException($e->getMessage(), $e->getCode(), $e);
        }
    }
}