#pragma once

/** Paths to helper programs.
 *
 * net-admin-helper does not search the PATH for reasons of security, so the
 * paths to the helper applications need to be configured explicitly (and they
 * must be installed, obviously). This is done here. The given paths should work
 * in most cases (Debian, Red Hat, and derivatives).
 */
#define IP "/sbin/ip"
#define WG "/usr/bin/wg"


/** Settings for container WireGuard */

// device name prefix, use e.g. your application name
#define CWG_PREFIX "mahiru"

#define ENABLE_CWG_CREATE
#define ENABLE_CWG_CONNECT
#define ENABLE_CWG_DESTROY

