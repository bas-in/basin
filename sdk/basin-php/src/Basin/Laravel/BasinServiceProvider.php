<?php

declare(strict_types=1);

namespace Basin\Laravel;

use Basin\Client;
use Illuminate\Support\ServiceProvider;

/**
 * Laravel service provider for Basin.
 *
 * Auto-discovered in Laravel 5.5+ via package discovery (the "extra.laravel"
 * key in composer.json). For older Laravel versions, add to config/app.php:
 *
 *   'providers' => [
 *       Basin\Laravel\BasinServiceProvider::class,
 *   ],
 *
 * Publish the config:
 *   php artisan vendor:publish --provider="Basin\Laravel\BasinServiceProvider"
 *
 * Configuration (config/basin.php):
 *   return [
 *       'url'        => env('BASIN_URL', 'https://api.basin.run'),
 *       'token'      => env('BASIN_TOKEN'),
 *       'project_id' => env('BASIN_PROJECT_ID'),
 *       'timeout'    => (float) env('BASIN_TIMEOUT', 30),
 *   ];
 *
 * Usage via the facade:
 *   use Basin\Laravel\BasinFacade as Basin;
 *
 *   $result = Basin::from('orders')->select('id,total')->execute();
 *
 * Or via dependency injection:
 *   public function __construct(private Basin\Client $basin) {}
 */
class BasinServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__ . '/config/basin.php', 'basin');

        $this->app->singleton(Client::class, function ($app) {
            /** @var array<string,mixed> $config */
            $config = $app['config']->get('basin', []);

            return new Client([
                'url'        => $config['url'] ?? throw new \RuntimeException('Basin: BASIN_URL is not set'),
                'token'      => $config['token'] ?? null,
                'project_id' => $config['project_id'] ?? null,
                'timeout'    => (float) ($config['timeout'] ?? 30.0),
            ]);
        });

        $this->app->alias(Client::class, 'basin');
    }

    public function boot(): void
    {
        if ($this->app->runningInConsole()) {
            $this->publishes([
                __DIR__ . '/config/basin.php' => $this->app->configPath('basin.php'),
            ], 'basin-config');
        }
    }
}
