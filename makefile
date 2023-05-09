EXECS=components.out

components.out: main.c
	mpicc -o components.out main.c

run: components.out
	mpirun -n 2 ${PWD}/components.out

clean:
	rm -f ${EXECS}
