<?php

namespace GumPHP\LaravelDuckDB\Query\Processors;

use Illuminate\Database\Query\Processors\Processor;

class DuckDBProcessor extends Processor
{
    /**
     * Process the results of a column listing query.
     *
     * @param  array  $results
     * @return array
     */
    public function processColumnListing($results)
    {
        return array_map(function ($result) {
            return ((object) $result)->name;
        }, $results);
    }

    /**
     * Process the results of a column type listing query.
     *
     * @param  array  $results
     * @return array
     */
    public function processListing($results)
    {
        return array_map(function ($result) {
            return ((object) $result)->name;
        }, $results);
    }
}