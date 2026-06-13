<?php

declare(strict_types=1);

namespace Basin\Exception;

/**
 * Raised when the transport layer fails (connection refused, timeout, etc.)
 * before a server response is received.
 */
class BasinNetworkException extends BasinException
{
}
