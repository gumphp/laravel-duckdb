# Laravel DuckDB 驱动

这是一个用于 Laravel 的 DuckDB 数据库驱动，让你可以在 Laravel 应用中像使用 MySQL 或 PostgreSQL 一样简单地使用 DuckDB。

## 特性

- 支持 Laravel 数据库连接管理
- 支持查询构建器 (Query Builder)
- 支持 Schema 构建器 (Schema Builder)
- 支持 Eloquent ORM（有限支持）
- 与 Laravel 的数据库迁移系统集成

## 安装

1. 安装 DuckDB PHP 扩展：
   https://github.com/fnvoid64/php-duckdb

3. 安装 composer 包：
   ```bash
   composer require gumphp/laravel-duckdb
   ```

## 配置

在 `config/database.php` 中添加 DuckDB 连接配置：

```php
'duckdb' => [
    'driver' => 'duckdb',
    'database' => env('DUCKDB_DATABASE', ':memory:'),
    'prefix' => '',
    'config' => [
        'threads' => env('DUCKDB_THREADS', 4),
    ],
],
```

在 `.env` 文件中设置：

```env
DUCKDB_DATABASE=/path/to/your/database.db
DUCKDB_THREADS=4
```

## 使用

### 基本查询

```php
// 使用查询构建器
$users = DB::connection('duckdb')->table('users')->get();

// 插入数据
DB::connection('duckdb')->table('users')->insert([
    ['name' => 'John Doe', 'email' => 'john@example.com'],
    ['name' => 'Jane Smith', 'email' => 'jane@example.com']
]);

// 更新数据
DB::connection('duckdb')->table('users')
    ->where('name', 'John Doe')
    ->update(['email' => 'john.doe@example.com']);

// 删除数据
DB::connection('duckdb')->table('users')
    ->where('name', 'Jane Smith')
    ->delete();
```

### Schema 操作

```php
// 创建表
Schema::connection('duckdb')->create('users', function ($table) {
    $table->string('name');
    $table->string('email');
});

// 删除表
Schema::connection('duckdb')->dropIfExists('users');
```

## 注意事项

在生产环境中，建议将 DuckDB 数据库文件存储在持久化存储中，而不是使用内存数据库。

## 测试

运行测试命令验证驱动是否正常工作：

```bash
php artisan duckdb:test
```

## 许可证

MIT