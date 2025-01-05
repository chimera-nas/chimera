#include <stdio.h>
#include <sys/time.h>

int main(int argc, char *argv[])
{
	printf("sizeof(struct timespec)=%lu\n", sizeof(struct timespec));

	return 0;
}
