objects = main.o subprocess.o

CFLAGS=-std=c11 -Wall -Wextra -pedantic -O


.PHONY: all
all: mahiru_net_admin_linux_helper


.PHONY: clean
clean:
	rm -f mahiru_net_admin_linux_helper $(objects)


main.o: subprocess.h
subprocess.o: subprocess.h

mahiru_net_admin_linux_helper: $(objects)
	cc -o mahiru_net_admin_linux_helper $(objects)
