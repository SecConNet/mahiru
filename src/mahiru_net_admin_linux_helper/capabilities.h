#pragma once


/** Set ambient capabilities.
 *
 * This sets up ambient capabilities for use by an about-to-be execve()'d
 * program.
 *
 * @return 0 on success, -1 on failure.
 */
int set_ambient_capabilities();

