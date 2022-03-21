# w8y - A circular work list item processor

## What?
`w8y` (pronounced _weighty_, or _waity_ if you prefer) is simply an executable wrapper that enriches the environment of
the executable to provide it with a work item to process via environment variables and/or command-line arguments.

## How?
`w8y` reads work items from a circular redis list until one is available to process.

Work items are popped from the left side of the redis circular list and atomically appended to the right side.
This way, no work items are ever lost or removed from the circular list by `w8y` itself. External redis clients can
freely manage the circular list by adding or removing work items at any time.

A work item is eligible to process based on the existence of a key named after the list value. If a processing key
exists for the work item then it is _not_ eligible to be processed.

If no work item is eligible after traversing the list once then `w8y` returns with exit code 0. The list's
length is queried before traversal to provide a stopping point.

To mark a work item as being processed, a processing key is created in redis and a background thread is spawned to
periodically refresh the key to prevent it from expiring. Once this key expires, the work item is eligible for
processing by the next `w8y` process that picks it up from the circular list.

A specified executable is spawned to handle the work item and `w8y` exits with the spawned process's exit code when
it completes.

**Assumptions:**
* Redis is available
* The list of work items may change over time, being appended to and removed from

## Command-line arguments
```
Usage:
  w8y [OPTIONS] executable args...

Application Options:
  -u, --redis-url=         Redis URL to connect to (default:
                           redis://localhost:6379)
  -k, --key-prefix=        Redis prefix for all keys
  -x, --key-expiry=        Redis processing key expiry in seconds (default: 5)
  -q, --quiet              Silence output of w8y to capture pure stdout,stderr
                           of spawned executable
  -f, --log-file=          Log to file
  -t, --no-log-timestamps  Disable inclusion of timestamps in log lines
  -e, --env-var=           Environment variable name to set work item to

Help Options:
  -h, --help               Show this help message

Arguments:
  args:                    Arguments to pass to executable; use {} as a
                           placeholder for work item value
```

## Spawned Executable
The `executable` argument indicates which executable to spawn to do work. The value is either the name of an executable
searched for in `$PATH` or the absolute path of an executable.

Environment variables from the `w8y` process's environment are passed to the executable.

Command-line arguments after the `executable` required argument are passed to the spawned process except that all
placeholder values of `{}` are replaced with the work item value.

## Examples

```
redis-cli::
127.0.0.1:6379> RPUSH work:list a:0 a:1 a:2 b c d e f g:0 g:1 g:2 g:3 g:4 h i j k l m
(integer) 19

bash::
$ w8y -k work -q echo hello world, {}
hello world, a:0

redis-cli::
127.0.0.1:6379> LRANGE work:list 0 -1
 1) "a:1"
 2) "a:2"
 3) "b"
 4) "c"
 5) "d"
 6) "e"
 7) "f"
 8) "g:0"
 9) "g:1"
10) "g:2"
11) "g:3"
12) "g:4"
13) "h"
14) "i"
15) "j"
16) "k"
17) "l"
18) "m"
19) "a:0"

-- notice the list has rotated "a:0" to the end
```
