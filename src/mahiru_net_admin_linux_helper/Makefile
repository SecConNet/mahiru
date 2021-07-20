objects = main.o capabilities.o subprocess.o

CFLAGS=-std=c11 -Wall -Wextra -pedantic -O
LDFLAGS=-lcap


.PHONY: all
all: mahiru_net_admin_linux_helper


.PHONY: setcap
setcap: mahiru_net_admin_linux_helper
	# Ensure normal users cannot modify the binary
	chown root:root ./mahiru_net_admin_linux_helper
	chmod 755 ./mahiru_net_admin_linux_helper
	# Give it CAP_NET_ADMIN
	setcap cap_net_admin=p ./mahiru_net_admin_linux_helper


.PHONY: clean
clean:
	rm -f mahiru_net_admin_linux_helper $(objects)


main.o: subprocess.h
capabilities.o: capabilities.h
subprocess.o: capabilities.h subprocess.h

mahiru_net_admin_linux_helper: $(objects)
	cc -o mahiru_net_admin_linux_helper $(objects) $(LDFLAGS)
