EXECS=components

all: ${EXECS}

components: main.c
    mpicc -o components main.c

clean:
    rm ${EXECS}