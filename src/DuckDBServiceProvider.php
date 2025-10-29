<?php

namespace GumPHP\LaravelDuckDB;

use Illuminate\Support\ServiceProvider;
use Illuminate\Support\Arr;
use Illuminate\Database\Connection;

class DuckDBServiceProvider extends ServiceProvider
{
    /**
     * Register the service provider.
     *
     * @return void
     */
    public function register()
    {
        // Register the DuckDB connector
        $this->app->bind('db.connector.duckdb', function () {
            return new DuckDBConnector();
        });

        // Extend the connection factory
        $this->app->resolving('db', function ($db) {
            $db->extend('duckdb', function ($config, $name) {
                $config = $this->parseConfig($config, $name);

                $connector = new DuckDBConnector();
                $duckdb = $connector->connect($config);

                return new DuckDBConnection($duckdb, $config['database'], $config['prefix'], $config);
            });
        });
    }

    /**
     * Parse and prepare the database configuration.
     *
     * @param  array  $config
     * @param  string  $name
     * @return array
     */
    protected function parseConfig(array $config, $name)
    {
        return Arr::add(Arr::add($config, 'prefix', ''), 'name', $name);
    }

    /**
     * Bootstrap the application events.
     *
     * @return void
     */
    public function boot()
    {
        // Register the DuckDB connection resolver
        Connection::resolverFor('duckdb', function ($connection, $database, $prefix, $config) {
            return new DuckDBConnection($connection, $database, $prefix, $config);
        });
    }
}