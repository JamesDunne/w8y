# w8y - A circular work list item processor

## What?
`w8y` (pronounced _weighty_, or _waity_ if you prefer) was created to solve a specific problem:

On startup, provide an executable with a distinct work item to process from a list of work items via
environment variables and/or command-line arguments.

`w8y` is simply an executable wrapper that enriches the environment of a single instance of an executable to provide
it with a work item to process.

**Assumptions:**
* Redis is available and equally accessible by `w8y`
* The list of work items may change over time, being appended to and removed from

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

## Spawned Executable
The `W8Y_EXEC` environment variable indicates which executable to spawn to do work. The value is either the name of
an executable searched for in `$PATH` or the absolute path of an executable.

Environment variables from the `w8y` process's environment are passed to the executable. All environment variables
prefixed with `W8Y_` are filtered out of the environment passed to the executable.

Command-line arguments are not interpreted by `w8y` and are passed directly to the spawned process.

If the work item should be passed to the executable via command-line arguments, then the `W8Y_EXEC_ARGN` environment
variable should be set to the offset in the argument list at which to insert the work item before. Negative values
are offsets from one past the end of the arguments list.

The work item is always passed by environment variable to the spawned process using the environment variable name
specified by `W8Y_EXEC_ENVVAR`.

## Environment Variables
| Name                     | Default                | Purpose                                                                                                                        |
|--------------------------|------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| W8Y_EXEC                 |                        | Process to spawn (name in $PATH or absolute path) to process work item                                                         |
| W8Y_EXEC_ARGN            |                        | Argument offset to insert work item before (0-based, negative values are offset from end of args)                              |
| W8Y_EXEC_ENVVAR          | W8Y_WORK_ITEM          | Environment variable name to set to work item                                                                                  |
| W8Y_REDIS_URL            | redis://localhost:6379 | Redis URL to connect to                                                                                                        |
| W8Y_REDIS_KEY_PREFIX     | <none>                 | Key Prefix to prepend to all redis keys                                                                                        |
| W8Y_REDIS_EXPIRY_SECONDS | 5                      | Number of seconds to set as expiry of work item processing key                                                                 |
| W8Y_LOG_SILENT           | 0                      | If set to non-zero value, silence log output                                                                                   |
| W8Y_LOG_FD               | <none>                 | If set to non-empty value, output logs to specific file descriptor number; best when paired with shell redirection to log file |

