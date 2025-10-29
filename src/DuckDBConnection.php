<?php

namespace GumPHP\LaravelDuckDB;

use Illuminate\Database\Connection;
use Fnvoid\DuckDB\DuckDB;
use Fnvoid\DuckDB\Result;
use Fnvoid\DuckDB\Exception as DuckDBException;
use GumPHP\LaravelDuckDB\Query\Grammars\DuckDBGrammar as QueryGrammar;
use GumPHP\LaravelDuckDB\Query\Processors\DuckDBProcessor;
use GumPHP\LaravelDuckDB\Schema\DuckDBBuilder;
use GumPHP\LaravelDuckDB\Schema\Grammars\DuckDBGrammar as SchemaGrammar;

class DuckDBConnection extends Connection
{
    /**
     * The DuckDB connection instance.
     *
     * @var \Fnvoid\DuckDB\DuckDB
     */
    protected $duckdb;

    /**
     * Create a new DuckDB connection instance.
     *
     * @param  \Fnvoid\DuckDB\DuckDB  $duckdb
     * @param  string  $database
     * @param  string  $tablePrefix
     * @param  array  $config
     */
    public function __construct(DuckDB $duckdb, $database = '', $tablePrefix = '', array $config = [])
    {
        $this->duckdb = $duckdb;

        parent::__construct($this->createPdoConnection($duckdb), $database, $tablePrefix, $config);
        
        $this->useDefaultQueryGrammar();
        $this->useDefaultPostProcessor();
    }
    
    /**
     * Create a fake PDO connection for Laravel's base Connection class.
     *
     * @param  \Fnvoid\DuckDB\DuckDB  $duckdb
     * @return \PDO
     */
    protected function createPdoConnection($duckdb)
    {
        // Create a minimal PDO connection to satisfy Laravel's base Connection class
        // Since DuckDB doesn't use PDO, we'll create a minimal connection to :memory:
        return new \PDO('sqlite::memory:');
    }

    /**
     * Get the default query grammar instance.
     *
     * @return \Illuminate\Database\Query\Grammars\Grammar
     */
    protected function getDefaultQueryGrammar()
    {
        return new QueryGrammar($this);
    }

    /**
     * Get the default post processor instance.
     *
     * @return \Illuminate\Database\Query\Processors\Processor
     */
    protected function getDefaultPostProcessor()
    {
        return new DuckDBProcessor();
    }

    /**
     * Get a schema builder instance for the connection.
     *
     * @return \LaravelDuckdb\LaravelDuckdb\Schema\DuckDBBuilder
     */
    public function getSchemaBuilder()
    {
        if (is_null($this->schemaGrammar)) {
            $this->useDefaultSchemaGrammar();
        }

        return new DuckDBBuilder($this);
    }

    /**
     * Get the default schema grammar instance.
     *
     * @return \Illuminate\Database\Schema\Grammars\Grammar
     */
    protected function getDefaultSchemaGrammar()
    {
        return new SchemaGrammar($this);
    }

    /**
     * Run a select statement against the database.
     *
     * @param  string  $query
     * @param  array  $bindings
     * @param  bool  $useReadPdo
     * @return array
     */
    public function select($query, $bindings = [], $useReadPdo = true)
    {
        return $this->run($query, $bindings, function ($query, $bindings) {
            if ($this->pretending()) {
                return [];
            }

            $statement = $this->duckdb->prepare($query);
            $result = $statement->execute($bindings);

            return $result->fetchAll();
        });
    }

    /**
     * Run a select statement against the database and returns a generator.
     *
     * @param  string  $query
     * @param  array  $bindings
     * @param  bool  $useReadPdo
     * @return \Generator
     */
    public function cursor($query, $bindings = [], $useReadPdo = true)
    {
        $statement = $this->run($query, $bindings, function ($query, $bindings) {
            if ($this->pretending()) {
                return [];
            }

            $statement = $this->duckdb->prepare($query);
            return $statement->execute($bindings);
        });

        foreach ($statement->iterate() as $record) {
            yield $record;
        }
    }

    /**
     * Run an insert statement against the database.
     *
     * @param  string  $query
     * @param  array  $bindings
     * @return bool
     */
    public function insert($query, $bindings = [])
    {
        return $this->statement($query, $bindings);
    }

    /**
     * Run an update statement against the database.
     *
     * @param  string  $query
     * @param  array  $bindings
     * @return int
     */
    public function update($query, $bindings = [])
    {
        return $this->affectingStatement($query, $bindings);
    }

    /**
     * Run a delete statement against the database.
     *
     * @param  string  $query
     * @param  array  $bindings
     * @return int
     */
    public function delete($query, $bindings = [])
    {
        return $this->affectingStatement($query, $bindings);
    }

    /**
     * Execute an SQL statement and return the boolean result.
     *
     * @param  string  $query
     * @param  array  $bindings
     * @return bool
     */
    public function statement($query, $bindings = [])
    {
        return $this->run($query, $bindings, function ($query, $bindings) {
            if ($this->pretending()) {
                return true;
            }

            // Check if the query contains multiple statements separated by semicolons
            $statements = preg_split('/;\s*(?=\w)/', trim($query));
            $statements = array_filter($statements, function($stmt) {
                return !empty(trim($stmt));
            });

            // Execute each statement separately
            foreach ($statements as $stmt) {
                $stmt = trim($stmt);
                if (!empty($stmt)) {
                    $statement = $this->duckdb->prepare($stmt);
                    $statement->execute($bindings);
                }
            }

            $this->recordsHaveBeenModified();

            return true;
        });
    }

    /**
     * Run an SQL statement and get the number of rows affected.
     *
     * @param  string  $query
     * @param  array  $bindings
     * @return int
     */
    public function affectingStatement($query, $bindings = [])
    {
        return $this->run($query, $bindings, function ($query, $bindings) {
            if ($this->pretending()) {
                return 0;
            }

            $statement = $this->duckdb->prepare($query);
            $result = $statement->execute($bindings);

            // For DuckDB, we need to get the affected row count differently
            // This is a simplified approach - in practice, you might need to
            // parse the query to determine the operation and count accordingly
            $this->recordsHaveBeenModified(true);

            // Return a default value since DuckDB doesn't directly provide affected row count
            return 1;
        });
    }

    /**
     * Run a raw, unprepared query against the PDO connection.
     *
     * @param  string  $query
     * @return bool
     */
    public function unprepared($query)
    {
        return $this->run($query, [], function ($query) {
            if ($this->pretending()) {
                return true;
            }

            $this->duckdb->query($query);
            $this->recordsHaveBeenModified(true);

            return true;
        });
    }

    /**
     * Get the current PDO connection.
     *
     * @return \PDO
     */
    public function getPdo()
    {
        // Return the fake PDO connection
        return parent::getPdo();
    }

    /**
     * Get the current PDO connection used for reading.
     *
     * @return \PDO
     */
    public function getReadPdo()
    {
        // Return the fake PDO connection
        return parent::getReadPdo();
    }

    /**
     * Get the database connection name.
     *
     * @return string|null
     */
    public function getName()
    {
        return $this->getConfig('name');
    }

    /**
     * Get the DuckDB connection instance.
     *
     * @return \Fnvoid\DuckDB\DuckDB
     */
    public function getDuckDB()
    {
        return $this->duckdb;
    }

    /**
     * Get the server version for the connection.
     *
     * @return string
     */
    public function getServerVersion(): string
    {
        // DuckDB version information
        return 'DuckDB';
    }
    
    /**
     * Set the query grammar to the default implementation.
     *
     * @return void
     */
    public function useDefaultQueryGrammar()
    {
        $this->queryGrammar = $this->getDefaultQueryGrammar();
    }

    /**
     * Set the query post processor to the default implementation.
     *
     * @return void
     */
    public function useDefaultPostProcessor()
    {
        $this->postProcessor = $this->getDefaultPostProcessor();
    }
    
    /**
     * Set the schema grammar to the default implementation.
     *
     * @return void
     */
    public function useDefaultSchemaGrammar()
    {
        $this->schemaGrammar = $this->getDefaultSchemaGrammar();
    }
}