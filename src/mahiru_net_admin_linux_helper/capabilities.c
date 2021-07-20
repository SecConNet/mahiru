/* Function for manipulating capabilities. */
#include <stdio.h>
#include <stdlib.h>

#include <sys/capability.h>
#include <sys/prctl.h>

#include "capabilities.h"


/** Set ambient capabilities.
 *
 * This sets up the ambient capabilities set. Normal capabilities (in the
 * Inherited, Permitted and Effective sets) do not survive a call to execve().
 * In order to pass on some of our capabilities across an execve() call, we need
 * to add them to the Ambient set. This function does that, specifically for the
 * CAP_NET_ADMIN capability.
 *
 * Note that CAP_NET_ADMIN needs to be added to the Inherited and Permitted sets
 * for this executable using setcap (8), as we can only pass on capabilities we
 * have, not add new ones.
 *
 * @return 0 on success, -1 on failure.
 */
int set_ambient_capabilities() {
    cap_t caps;
    cap_value_t cap_list[] = {CAP_NET_ADMIN};

    caps = cap_get_proc();
    if (caps == NULL) {
        perror("Error getting capabilities");
        goto exit_0;
    }

    if (cap_set_flag(caps, CAP_EFFECTIVE, 1, cap_list, CAP_SET) != 0) {
        perror("Error setting effective capabilities");
        goto exit_0;
    }

    if (cap_set_flag(caps, CAP_INHERITABLE, 1, cap_list, CAP_SET) != 0) {
        perror("Error setting inheritable capabilities");
        goto exit_0;
    }

    if (cap_set_proc(caps) != 0) {
        perror("Error setting capabilities");
        goto exit_0;
    }

    if (cap_free(caps) != 0) {
        perror("Error freeing capabilities");
        goto exit_0;
    }

    if (prctl(PR_CAP_AMBIENT, PR_CAP_AMBIENT_RAISE, CAP_NET_ADMIN, 0, 0) != 0)
    {
        perror("Error setting ambient capabilities");
        goto exit_0;
    }

    return 0;

exit_0:
    return -1;
}

