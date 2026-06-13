<?php

return [
    /*
    |--------------------------------------------------------------------------
    | Basin Server URL
    |--------------------------------------------------------------------------
    |
    | The base URL of your Basin server, e.g. https://api.basin.run
    | or http://localhost:8080 for local development.
    |
    */
    'url' => env('BASIN_URL', 'https://api.basin.run'),

    /*
    |--------------------------------------------------------------------------
    | Bearer Token
    |--------------------------------------------------------------------------
    |
    | A JWT or raw API key used for Authorization: Bearer authentication.
    | This is the static fallback token used when no user session is active.
    | For user-facing auth flows, use $basin->auth->signIn() instead.
    |
    */
    'token' => env('BASIN_TOKEN'),

    /*
    |--------------------------------------------------------------------------
    | Project ID
    |--------------------------------------------------------------------------
    |
    | Your Basin project ULID (e.g. 01JXXXXXXXXXXXXXXXXXXXXXXXXX).
    | Required for public storage URLs, realtime, and auth operations.
    | When using a JWT that carries a `project_id` claim this is optional.
    |
    */
    'project_id' => env('BASIN_PROJECT_ID'),

    /*
    |--------------------------------------------------------------------------
    | Request Timeout
    |--------------------------------------------------------------------------
    |
    | Default HTTP request timeout in seconds.
    |
    */
    'timeout' => (float) env('BASIN_TIMEOUT', 30),
];
