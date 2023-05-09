#include <mpi.h>
#include <stdlib.h>
#include <stdio.h>

#define GLOBAL_MAX_VALUE            100000
#define BATCH_EDGES                 1000
#define GLOBAL_MAX_ARRAY_SIZE       10000000000

int WORLD_SIZE = -1;
int WORLD_RANK = -1;

int PARENT[GLOBAL_MAX_VALUE];
int VALUE[GLOBAL_MAX_VALUE];
int NODES = 0;

int* new_sized_buffer(int max_size) {
    int* sized_buffer = malloc(sizeof(int)*(max_size+1));
    sized_buffer[0] = 0;
    return sized_buffer;
}

void push_sized_buffer(int* sized_buffer, int e) { // push to array with size at index 0
    sized_buffer[0] += 1;
    sized_buffer[sized_buffer[0]] = e;
}

int* get_array_sized_buffer(int* sized_buffer) {
    int* array = (sized_buffer+1);
    return array;
}

int get_size_sized_buffer(int* sized_buffer) {
    return sized_buffer[0];
}

int search(int value) { // returns u_local for vertex value OR returns 0 if not found
    for (int u_local = 0; u_local < NODES; u_local++) {
        if (VALUE[u_local] == value) {
            return u_local;
        }
    }
    return -1;
}

/////* get u_local of vertex with value `value` or if no such vertex is present then push to forest *///
int get_u_local(int value) {
    int u_local = search(value);
    if (u_local == -1) {
        u_local = NODES;
        VALUE[u_local] = value;
        PARENT[u_local] = u_local;
        NODES++;
    }
    return u_local;
}

int find_root(int u_local) { // returns u_local of the root containing vertex U_local
    while (u_local != PARENT[u_local]) {
        u_local = PARENT[u_local];
    }
    return u_local;
}

void connect_vertices(int u_local, int v_local) {
    int u_local_root = find_root(u_local);
    int v_local_root = find_root(v_local);
    if (VALUE[u_local_root] < VALUE[v_local_root]) {
        PARENT[v_local] = u_local_root;
    } else {
        PARENT[u_local] = v_local_root;
    }
}

void add_edge(int u_value, int v_value) {
    int u_local = get_u_local(u_value);
    int v_local = get_u_local(v_value);
    connect_vertices(u_local, v_local);
}

void display() {
    printf("INDEX:\t");
    for (int u_local = 0; u_local < NODES; u_local++) {
        printf("%d\t", u_local);
    }
    printf("\n");
    printf("PARENT:\t");
    for (int u_local = 0; u_local < NODES; u_local++) {
        printf("%d\t", PARENT[u_local]);
    }
    printf("\n");
    printf("VALUE:\t");
    for (int u_local = 0; u_local < NODES; u_local++) {
        printf("%d\t", VALUE[u_local]);
    }
    printf("\n");
}

int* generate_edges() { // generates random value pairs as edges
    int* edges_sized_array = new_sized_buffer(BATCH_EDGES * 2);                                                             ///// `edges` generated
    for (int i = 0; i < (BATCH_EDGES * 2); i += 2) {
        int u_value = rand() % GLOBAL_MAX_VALUE;
        int v_value = rand() % GLOBAL_MAX_VALUE;
        push_sized_buffer(edges_sized_array, u_value);
        push_sized_buffer(edges_sized_array, v_value);
    }
    printf("%d / %d: Generated %d edges.\n", WORLD_RANK, WORLD_SIZE, get_size_sized_buffer(edges_sized_array)/2);
    return edges_sized_array;
}

void add_edges(int* edges_sized_array) {
    int edges_size = get_size_sized_buffer(edges_sized_array);
    for (int edge_i = 0; edge_i < (2 * edges_size); edge_i += 2) {
        add_edge(get_array_sized_buffer(edges_sized_array)[edge_i], get_array_sized_buffer(edges_sized_array)[edge_i + 1]);
    }
}

void send_values(int to, int tag) {
    MPI_Send(VALUE, NODES, MPI_INT, to, tag, MPI_COMM_WORLD);
}

int* receive_values(int from, int tag) {
    int received_size;
    MPI_Status status;
    MPI_Probe(from, tag, MPI_COMM_WORLD, &status);
    MPI_Get_count(&status, MPI_INT, &received_size);

    int *received_values = new_sized_buffer(received_size);                                                     ///// `received_values` generated
    received_values[0] = received_size;
    MPI_Recv(get_array_sized_buffer(received_values), get_size_sized_buffer(received_values), MPI_INT, from, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    printf("%d/%d: %d received %d values from %d.\n", WORLD_RANK, WORLD_SIZE, WORLD_RANK, get_size_sized_buffer(received_values), from);
    return received_values;
}

int* sendrecv_values(int u_rank, int v_rank, int tag) {
    int big = (u_rank < v_rank) ? v_rank : u_rank;
    int small = (u_rank < v_rank) ? u_rank : v_rank;

    int *receive_buffer;
    if (WORLD_RANK ==
        big) {                                                                                                          ///// Bigger rank sends values first
        send_values(small, tag);
        receive_buffer = receive_values(small, tag);
    } else if (WORLD_RANK == small) {
        receive_buffer = receive_values(big,
                                        tag);                                                                         ///// Smaller rank receives values first
        send_values(big, tag);
    }
    return receive_buffer;
}

void send_edges(int* edges_sized_array, int to, int tag) {
    int buffer_size = get_size_sized_buffer(edges_sized_array);
    int* buffer_array = get_array_sized_buffer(edges_sized_array);
    MPI_Send(buffer_array, buffer_size, MPI_INT, to, tag, MPI_COMM_WORLD);
}

int* receive_edges(int from, int tag) {
    int received_size;
    MPI_Status status;
    MPI_Probe(from, tag, MPI_COMM_WORLD, &status);
    MPI_Get_count(&status, MPI_INT, &received_size);

    int *received_buffer = new_sized_buffer(received_size);                                                     ///// `received_edges` generated
    received_buffer[0] = received_size;
    MPI_Recv(get_array_sized_buffer(received_buffer), get_size_sized_buffer(received_buffer), MPI_INT, from, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    printf("%d/%d: %d received %d edges from %d.\n", WORLD_RANK, WORLD_SIZE, WORLD_RANK, get_size_sized_buffer(received_buffer)/2, from);
    return received_buffer;
}

int* sendrecv_common_edges(int* edges_sized_array, int u_rank, int v_rank, int tag) {
    int big = (u_rank < v_rank) ? v_rank : u_rank;
    int small = (u_rank < v_rank) ? u_rank : v_rank;

    int *receive_buffer;
    if (WORLD_RANK ==
        big) {                                                                                                          ///// Bigger rank sends values first
        send_edges(edges_sized_array, small, 0);
        receive_buffer = receive_edges(small, 0);
    } else if (WORLD_RANK == small) {
        receive_buffer = receive_edges(big,
                                        0);                                                                         ///// Smaller rank receives values first
        send_edges(edges_sized_array, big, 0);
    }
    return receive_buffer;
}

int* generate_common_edges(int* received_edges_sized_buffer) {                                                          ///// TODO: VERIFY
    int* common_edges_sized_array = new_sized_buffer(get_size_sized_buffer(received_edges_sized_buffer) * 2);    ///// `common_edges` generated
    for (int i=0; i< get_size_sized_buffer(received_edges_sized_buffer); i++) {
        int u_local = search(get_array_sized_buffer(received_edges_sized_buffer)[i]);
        if (u_local != -1) {
            push_sized_buffer(common_edges_sized_array, VALUE[find_root(u_local)]);
            push_sized_buffer(common_edges_sized_array, VALUE[u_local]);
        }
    }
    printf("%d/%d: %d values common.\n", WORLD_RANK, WORLD_SIZE, get_size_sized_buffer(common_edges_sized_array)/2);
    return common_edges_sized_array;
}

int main(int argc, char **argv) {
    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &WORLD_RANK);
    MPI_Comm_size(MPI_COMM_WORLD, &WORLD_SIZE);
    srand(WORLD_RANK + 1);

    if (WORLD_RANK == 0) {
        printf("WORLD_SIZE = %d\n", WORLD_SIZE);
        printf("MAX_VALUE  = %d\n", GLOBAL_MAX_VALUE);
    }

    ///////////////////////////////////// FORMING INITIAL FOREST ///////////////////////////////////////////////////////
    int* edges_sized_array = generate_edges();
    add_edges(edges_sized_array);
    free(edges_sized_array);

    ///////////////////////////////////// INITIAL FOREST FORMED ////////////////////////////////////////////////////////
    if (WORLD_RANK == 0) { display(); MPI_Barrier(MPI_COMM_WORLD); }
    else if (WORLD_RANK == 1) { MPI_Barrier(MPI_COMM_WORLD); display(); }

    MPI_Barrier(MPI_COMM_WORLD);
    ///////////////////////////////////// SYNCHRONIZING COMMON EDGES ///////////////////////////////////////////////////
    int* received_values_sized_buffer = sendrecv_values(0, 1, 0);
    int* common_edges_sized_array = generate_common_edges(received_values_sized_buffer);
    free(received_values_sized_buffer);
    int* received_common_edges_sized_buffer = sendrecv_common_edges(common_edges_sized_array, 0, 1, 0);
    free(common_edges_sized_array);
    add_edges(received_common_edges_sized_buffer);
    free(received_common_edges_sized_buffer);
    ///////////////////////////////////// COMMON EDGES SYNCHRONIZED ////////////////////////////////////////////////////
    MPI_Barrier(MPI_COMM_WORLD);

    if (WORLD_RANK == 0) { display(); MPI_Barrier(MPI_COMM_WORLD); }
    else if (WORLD_RANK == 1) { MPI_Barrier(MPI_COMM_WORLD); display(); }

    MPI_Finalize();
}
