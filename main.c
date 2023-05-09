#include <stdlib.h>
#include <time.h>
#include <mpi.h>

#include <stdio.h>

#define VERTICES 10000
#define EDGES    10000

void push(int *array, int *size, int e) {
    array[*(size)] = e;
    (*size)++;
}

int is_at(int *array, int size, int e) {
    for (int u_local = 0; u_local < size; u_local++) {
        if (array[u_local] == e) { return u_local; }
    }
    return (-1); // NOT FOUND
}

int WORLD_SIZE = -1;
int WORLD_RANK = -1;

int PARENT[VERTICES];
int PARENT_SIZE = 0;

int VALUE[VERTICES];
int VALUE_SIZE = 0;

int REPRESENTATIVES[VERTICES];
int REPRESENTATIVES_SIZE = 0;



// get index of vertex with value `value` or if no such vertex is present then push to forest
int get_local_addr(int value) {
    int u_local = is_at(VALUE, VALUE_SIZE, value);
    if (u_local == (-1)) {
        u_local = VALUE_SIZE;
        push(VALUE, &(VALUE_SIZE), value);
        push(PARENT, &(PARENT_SIZE), u_local);
    }
    return u_local;
}

int is_representative(int u_local) {
    if (u_local == PARENT[u_local]) {
        return 1;
    } else {
        return 0;
    }
}

// find the representative of the connected component
int find(int u_local) {
    int u_local_root = PARENT[u_local];
    while (is_representative(u_local_root) == 0) {
        u_local_root = PARENT[u_local_root];
    }
    return u_local_root;
}

void connect(int u_local, int v_local) {
    int u_local_root = find(u_local);
    int v_local_root = find(v_local);
    if (VALUE[u_local_root] < VALUE[v_local_root]) {
        PARENT[v_local] = u_local_root;
    } else {
        PARENT[u_local] = v_local_root;
    }
}

void add_edge(int u_value, int v_value) {
    int u_local = get_local_addr(u_value);
    int v_local = get_local_addr(v_value);
    connect(u_local, v_local);
}

int path_compress_branch(int u_local) {
    if (is_representative(u_local) == 1) {
        return u_local;
    } else {
        int root = path_compress_branch(PARENT[u_local]);
        PARENT[u_local] = root;
        return root;
    }
}

void path_compress() {
    for (int u_local = 0; u_local < PARENT_SIZE; u_local++){
        path_compress_branch(u_local);
    }
}

void find_representatives() {
    for (int u_local=0; u_local<VERTICES; u_local++){
        if (is_representative(u_local) == 1) {
            push(REPRESENTATIVES, &(REPRESENTATIVES_SIZE), u_local);
        }
    }
}

void display() {
    for (int u_local=0; u_local<VERTICES; u_local++){
        printf("%d\t", u_local);
    }
    printf("\n");
    for (int u_local=0; u_local<PARENT_SIZE; u_local++){
        printf("%d\t", PARENT[u_local]);
    }
    printf("\n");
    for (int u_local=0; u_local<VALUE_SIZE; u_local++){
        printf("%d\t", VALUE[u_local]);
    }
    printf("\n");
    printf("REPRESENTATIVES of %d are : ", WORLD_RANK);
    for (int i=0; i<REPRESENTATIVES_SIZE; i++){
        printf("%d\t", VALUE[REPRESENTATIVES[i]]);
    }
    printf("\n");
}

struct Edges {
    int size;
    int array[2 * EDGES];
};

struct Edges generate_edges() { // generates edges as a (2*n_edge) sized array on heap
    struct Edges edges;
    for (int u_local = 0; u_local < EDGES; u_local += 2) {
        int u_value = rand() % (WORLD_SIZE * VERTICES);
        int v_value = rand() % (WORLD_SIZE * VERTICES);
        edges.array[u_local] = u_value;
        edges.array[u_local + 1] = v_value;
    }
    return edges;
}

void populate(struct Edges edges) {
    for (int u_local = 0; u_local < EDGES; u_local += 2) {
        add_edge(edges.array[u_local], edges.array[u_local + 1]);
    }
}

void generate(struct Edges edges) {
    populate(edges);
    path_compress();
    find_representatives();
}

int* generate_representative_edges(){
    int edges_size = 2 * REPRESENTATIVES_SIZE;
    int* send_buffer = (int*)malloc((sizeof(int) * buffer_size) + 1);
    send_buffer[0] = edges_size;
    ++send_buffer;
    for (int i=0; i<REPRESENTATIVES_SIZE; i++){
        send_buffer[2*i] = VALUE[REPRESENTATIVES[i]];
        send_buffer[(2*i)+1] = VALUE[REPRESENTATIVES[i]];
    }
    --send_buffer;
    return send_buffer;
}

void send_edges(int* send_buffer, int to, int tag) {
//    int* send_buffer = generate_representative_edges();
    int buffer_size = send_buffer[0];
    ++send_buffer;
    MPI_Send(send_buffer, buffer_size, MPI_INT, to, tag, MPI_COMM_WORLD);
    --send_buffer;
    free(send_buffer);
}

int* receive_edges(int from, int tag) {
    int received_size;
    MPI_Status status;
    MPI_Probe(from, tag, MPI_COMM_WORLD, &status);
    MPI_Get_count(&status, MPI_INT, &received_size);
    int* received_buffer = (int*)malloc(sizeof(int) * (received_size + 1));
    received_buffer[0] = received_size;
    ++received_buffer;
    MPI_Recv(received_buffer, received_size, MPI_INT, from, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    --received_buffer;
    printf("%d/%d: %d received %d edges from %d.\n", WORLD_RANK, WORLD_SIZE, WORLD_RANK, received_size, from);
    return received_buffer;
}

int* sendrecv_representatives(int u_rank, int v_rank, int tag) {
    int big, small;
    if (u_rank < v_rank) {
        big = v_rank;
        small = u_rank;
    } else {
        big = u_rank;
        small = v_rank;
    }

    int* receive_buffer;
    if (WORLD_RANK == big) {
        send_edges(generate_representative_edges(), small, 0);
        receive_buffer = receive_edges(small, 0);
    } else if (WORLD_RANK == small){
        receive_buffer = receive_edges(big, 0);
        send_edges(generate_representative_edges(), big, 0);
    }
    return receive_buffer;
}

void sync_representatives(int u_rank, int v_rank, int tag) {
    int* receive_buffer = sendrecv_representatives(u_rank, v_rank, tag);
    int receive_buffer_size = receive_buffer[0];

    int *new_edges = malloc(2 * VERTICES * sizeof(int));
    int new_edges_size = 0;
    for (int i=0; i<receive_buffer_size; i++) {
        int u_local = is_at(VALUE, VALUE_SIZE, receive_buffer[i]);
        if (u_local != -1) {
            int u_local_root = find(u_local);
            int root_value = VALUE[u_local_root];
            push(new_edges, &new_edges_size, root_value);
            push(new_edges, &new_edges_size, receive_buffer[i]);
        } else {
            int u_local = get_local_addr(receive_buffer[i]);
        }
    }
    --receive_buffer;
    free(receive_buffer);
}

int main(int argc, char **argv) {
    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &WORLD_RANK);
    MPI_Comm_size(MPI_COMM_WORLD, &WORLD_SIZE);
    srand((294756334224345 * (WORLD_RANK+1)) % INT32_MAX);
    struct Edges edges = generate_edges();
    generate(edges);
    printf("%d/%d: %d edges generated for %d vertices.\n", WORLD_RANK, WORLD_SIZE, EDGES, VERTICES);
    MPI_Barrier(MPI_COMM_WORLD);

    sync_representatives(0, 1, 0);

    MPI_Finalize();
}