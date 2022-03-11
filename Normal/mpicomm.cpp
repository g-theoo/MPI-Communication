#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <fstream>
#include <vector>

#include "mpi.h"

using namespace std;

#define COORD0 0
#define COORD1 1
#define COORD2 2
#define ACK 100

#define PARENTTAG 101

// function that print the topology form each process
void printTopology(int rank , int* topology0, int* topology1, int* topology2, int size0, int size1, int size2) {
    printf("%d -> ", rank);
    printf("%d:", topology0[0]);
    for(int i = 1; i < size0; i ++) {
        if(i == size0 - 1) {
            printf("%d ", topology0[i]);
        } else {
            printf("%d,", topology0[i]);
        }
    }
    printf("%d:", topology1[0]);
    for(int i = 1; i < size1; i ++) {
        if(i == size1 - 1) {
            printf("%d ", topology1[i]);
        } else {
            printf("%d,", topology1[i]);
        }
    }
    printf("%d:", topology2[0]);
    for(int i = 1; i < size2; i ++) {
        if(i == size2 - 1) {
            printf("%d ", topology2[i]);
        } else {
            printf("%d,", topology2[i]);
        }
    }
    printf("\n");
}


int main (int argc, char *argv[]) {
    int numtasks, rank, len;
    char hostname[MPI_MAX_PROCESSOR_NAME];
    int workerParent;
    string line;
    MPI_Status status;

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Get_processor_name(hostname, &len);
    if (rank == COORD0) {
        vector<int> connections;
        connections.push_back(COORD1);
        connections.push_back(COORD2);

        // read the file
        ifstream myFile("cluster0.txt");
        if(myFile.is_open()) {
            getline(myFile, line);
            int numberOfWorkers = stoi(line);
            while(getline(myFile, line)) {
                int worker = stoi(line);
                connections.push_back(worker);
            }
        }

        // compute cluster's topology
        int sizeCOORD0 = connections.size() - 1;
        int topologyCOORD0[sizeCOORD0];
        topologyCOORD0[0] = rank;
        for(int i = 2; i < connections.size(); i ++) {
            topologyCOORD0[i-1] = connections[i];
        }

        // send cluster topology to the other coordinators
        MPI_Send(&sizeCOORD0, 1, MPI_INT, COORD1, 1, MPI_COMM_WORLD);
        printf("M(%d,%d)\n", rank, COORD1);
        MPI_Send(&topologyCOORD0, sizeCOORD0, MPI_INT, COORD1, 1, MPI_COMM_WORLD);
        printf("M(%d,%d)\n", rank, COORD1);

        MPI_Send(&sizeCOORD0, 1, MPI_INT, COORD2, 1, MPI_COMM_WORLD);
        printf("M(%d,%d)\n", rank, COORD2);
        MPI_Send(&topologyCOORD0, sizeCOORD0, MPI_INT, COORD2, 1, MPI_COMM_WORLD);
        printf("M(%d,%d)\n", rank, COORD2);

        // receive the topologies from other coordinators
        int sizeCOORD1, sizeCOORD2;
        MPI_Recv(&sizeCOORD1, 1, MPI_INT, COORD1, 1, MPI_COMM_WORLD, &status);
        int topologyCOORD1[sizeCOORD1];
        MPI_Recv(&topologyCOORD1, sizeCOORD1, MPI_INT, COORD1, 1, MPI_COMM_WORLD, &status);

        MPI_Recv(&sizeCOORD2, 1, MPI_INT, COORD2, 1, MPI_COMM_WORLD, &status);
        int topologyCOORD2[sizeCOORD2];
        MPI_Recv(&topologyCOORD2, sizeCOORD2, MPI_INT, COORD2, 1, MPI_COMM_WORLD, &status);

        // print the final topology
        printTopology(rank, topologyCOORD0, topologyCOORD1, topologyCOORD2, sizeCOORD0, sizeCOORD1, sizeCOORD2);

        // send data to workers
        for(int i = 2; i < connections.size(); i ++) {
            // send the parent process to workers
            MPI_Send(&rank, 1, MPI_INT, connections[i], PARENTTAG, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, connections[i]);

            // send the final topology to workers
            MPI_Send(&sizeCOORD0, 1, MPI_INT, connections[i], 1, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, connections[i]);
            MPI_Send(&topologyCOORD0, sizeCOORD0, MPI_INT, connections[i], 1, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, connections[i]);

            MPI_Send(&sizeCOORD1, 1, MPI_INT, connections[i], 1, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, connections[i]);
            MPI_Send(&topologyCOORD1, sizeCOORD1, MPI_INT, connections[i], 1, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, connections[i]);

            MPI_Send(&sizeCOORD2, 1, MPI_INT, connections[i], 1, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, connections[i]);
            MPI_Send(&topologyCOORD2, sizeCOORD2, MPI_INT, connections[i], 1, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, connections[i]);
        }

        // compute the vector
        int vectorSize = atoi(argv[1]);
        int vector[vectorSize];

        for(int i = 0; i < vectorSize; i++) {
            vector[i] = i;
        }

        int numberOfWorkers = sizeCOORD0 + sizeCOORD1 + sizeCOORD2 - 3;
        int tasksPerWorker;
        if(vectorSize % numberOfWorkers == 0) {
            tasksPerWorker = vectorSize / numberOfWorkers;
        } else {
            tasksPerWorker = vectorSize / numberOfWorkers + 1;
        }

        int tasks[numberOfWorkers][tasksPerWorker];
        int worker = 0;
        int task = 0;

        // split the tasks equally for each worker
        for(int i = 0; i < vectorSize; i ++) {
            tasks[worker][task] = vector[i];
            worker++;
            if(worker == numberOfWorkers) {
                worker = 0;
                task++;
            }
        }

        worker = 0;

        // send tasks to his workers and receive the final results for each part
        for(int i = 2; i < connections.size(); i ++){
            MPI_Send(&tasksPerWorker, 1, MPI_INT, connections[i], 1, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, connections[i]);

            MPI_Send(&tasks[worker], tasksPerWorker, MPI_INT, connections[i], 1, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, connections[i]);

            MPI_Recv(&tasks[worker], tasksPerWorker, MPI_INT, connections[i], 1, MPI_COMM_WORLD, &status);

            worker++;
        }

        // send tasks to the second coordinator and receive the final results for each part
        MPI_Send(&tasksPerWorker, 1, MPI_INT, COORD1, 1, MPI_COMM_WORLD);
        printf("M(%d,%d)\n", rank, COORD1);

        for(int i = 1; i < sizeCOORD1; i ++) {
            MPI_Send(&tasks[worker], tasksPerWorker, MPI_INT, COORD1, 1, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, COORD1);

            MPI_Recv(&tasks[worker], tasksPerWorker, MPI_INT, COORD1, 1, MPI_COMM_WORLD, &status);

            worker++;
        }

        // send tasks to the third coordinator and receive the final results for each part
        MPI_Send(&tasksPerWorker, 1, MPI_INT, COORD2, 1, MPI_COMM_WORLD);
        printf("M(%d,%d)\n", rank, COORD2);

        for(int i = 1; i < sizeCOORD2; i ++) {
            MPI_Send(&tasks[worker], tasksPerWorker, MPI_INT, COORD2, 1, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, COORD2);

            MPI_Recv(&tasks[worker], tasksPerWorker, MPI_INT, COORD2, 1, MPI_COMM_WORLD, &status);

            worker++;
        }

        worker = 0;
        task = 0;

        // print the final result
        printf("Rezultat: ");
        for(int i = 0; i < vectorSize; i++) {
            if(i == vectorSize - 1) {
                printf("%d\n", tasks[worker][task]);
            } else {
                printf("%d ", tasks[worker][task]);
            }
            worker++;
            if(worker == numberOfWorkers) {
                worker = 0;
                task ++;
            }
        }
    }
    else if(rank == COORD1) {
        vector<int> connections;
        connections.push_back(COORD0);
        connections.push_back(COORD2);

        // read the file
        ifstream myFile("cluster1.txt");
        if(myFile.is_open()) {
            getline(myFile, line);
            int numberOfWorkers = stoi(line);
            while(getline(myFile, line)) {
                int worker = stoi(line);
                connections.push_back(worker);
            }
        }

        // compute cluster's topology
        int sizeCOORD1 = connections.size() - 1;
        int topologyCOORD1[sizeCOORD1];
        topologyCOORD1[0] = rank;
        for(int i = 2; i < connections.size(); i ++) {
            topologyCOORD1[i-1] = connections[i];
        }

        // send cluster topology to the other coordinators
        MPI_Send(&sizeCOORD1, 1, MPI_INT, COORD0, 1, MPI_COMM_WORLD);
        printf("M(%d,%d)\n", rank, COORD0);
        MPI_Send(&topologyCOORD1, sizeCOORD1, MPI_INT, COORD0, 1, MPI_COMM_WORLD);
        printf("M(%d,%d)\n", rank, COORD0);

        MPI_Send(&sizeCOORD1, 1, MPI_INT, COORD2, 1, MPI_COMM_WORLD);
        printf("M(%d,%d)\n", rank, COORD2);
        MPI_Send(&topologyCOORD1, sizeCOORD1, MPI_INT, COORD2, 1, MPI_COMM_WORLD);
        printf("M(%d,%d)\n", rank, COORD2);

        // receive the topologies from other coordinators
        int sizeCOORD0, sizeCOORD2;
        MPI_Recv(&sizeCOORD0, 1, MPI_INT, COORD0, 1, MPI_COMM_WORLD, &status);
        int topologyCOORD0[sizeCOORD0];
        MPI_Recv(&topologyCOORD0, sizeCOORD0, MPI_INT, COORD0, 1, MPI_COMM_WORLD, &status);

        MPI_Recv(&sizeCOORD2, 1, MPI_INT, COORD2, 1, MPI_COMM_WORLD, &status);
        int topologyCOORD2[sizeCOORD2];
        MPI_Recv(&topologyCOORD2, sizeCOORD2, MPI_INT, COORD2, 1, MPI_COMM_WORLD, &status);

        // print the final topology
        printTopology(rank, topologyCOORD0, topologyCOORD1, topologyCOORD2, sizeCOORD0, sizeCOORD1, sizeCOORD2);

        // send data to workers
        for(int i = 2; i < connections.size(); i ++) {
            // send the parent process to workers
            MPI_Send(&rank, 1, MPI_INT, connections[i], PARENTTAG, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, connections[i]);

            // send the final topology to workers
            MPI_Send(&sizeCOORD0, 1, MPI_INT, connections[i], 1, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, connections[i]);
            MPI_Send(&topologyCOORD0, sizeCOORD0, MPI_INT, connections[i], 1, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, connections[i]);

            MPI_Send(&sizeCOORD1, 1, MPI_INT, connections[i], 1, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, connections[i]);
            MPI_Send(&topologyCOORD1, sizeCOORD1, MPI_INT, connections[i], 1, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, connections[i]);

            MPI_Send(&sizeCOORD2, 1, MPI_INT, connections[i], 1, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, connections[i]);
            MPI_Send(&topologyCOORD2, sizeCOORD2, MPI_INT, connections[i], 1, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, connections[i]);
        }


        int numberOfTasks;
        MPI_Recv(&numberOfTasks, 1, MPI_INT, COORD0, 1, MPI_COMM_WORLD, &status);
        int tasks[numberOfTasks];

        // send tasks to workers, receive the final results for each part and
        // send the final results to first coordinator
        for(int i = 1; i < sizeCOORD1; i ++) {
            MPI_Recv(&tasks, numberOfTasks, MPI_INT, COORD0, 1, MPI_COMM_WORLD, &status);

            MPI_Send(&numberOfTasks, 1, MPI_INT, topologyCOORD1[i], 1, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, topologyCOORD1[i]);

            MPI_Send(&tasks, numberOfTasks, MPI_INT, topologyCOORD1[i], 1, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, topologyCOORD1[i]);

            MPI_Recv(&tasks, numberOfTasks, MPI_INT, topologyCOORD1[i], 1, MPI_COMM_WORLD, &status);

            MPI_Send(&tasks, numberOfTasks, MPI_INT, COORD0, 1, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, COORD0);
        }

    } else if(rank == COORD2) {
        vector<int> connections;
        connections.push_back(COORD0);
        connections.push_back(COORD1);

        // read the file
        ifstream myFile("cluster2.txt");
        if(myFile.is_open()) {
            getline(myFile, line);
            int numberOfWorkers = stoi(line);
            while(getline(myFile, line)) {
                int worker = stoi(line);
                connections.push_back(worker);
            }
        }

        // compute cluster's topology
        int sizeCOORD2 = connections.size() - 1;
        int topologyCOORD2[sizeCOORD2];
        topologyCOORD2[0] = rank;
        for(int i = 2; i < connections.size(); i ++) {
            topologyCOORD2[i-1] = connections[i];
        }
        // send cluster topology to the other coordinators
        MPI_Send(&sizeCOORD2, 1, MPI_INT, COORD0, 1, MPI_COMM_WORLD);
        printf("M(%d,%d)\n", rank, COORD0);
        MPI_Send(&topologyCOORD2, sizeCOORD2, MPI_INT, COORD0, 1, MPI_COMM_WORLD);
        printf("M(%d,%d)\n", rank, COORD0);

        MPI_Send(&sizeCOORD2, 1, MPI_INT, COORD1, 1, MPI_COMM_WORLD);
        printf("M(%d,%d)\n", rank, COORD1);
        MPI_Send(&topologyCOORD2, sizeCOORD2, MPI_INT, COORD1, 1, MPI_COMM_WORLD);
        printf("M(%d,%d)\n", rank, COORD1);

        // receive the topologies from other coordinators
        int sizeCOORD0, sizeCOORD1;
        MPI_Recv(&sizeCOORD0, 1, MPI_INT, COORD0, 1, MPI_COMM_WORLD, &status);
        int topologyCOORD0[sizeCOORD0];
        MPI_Recv(&topologyCOORD0, sizeCOORD0, MPI_INT, COORD0, 1, MPI_COMM_WORLD, &status);

        MPI_Recv(&sizeCOORD1, 1, MPI_INT, COORD1, 1, MPI_COMM_WORLD, &status);
        int topologyCOORD1[sizeCOORD1];
        MPI_Recv(&topologyCOORD1, sizeCOORD1, MPI_INT, COORD1, 1, MPI_COMM_WORLD, &status);

        // print the final topology
        printTopology(rank, topologyCOORD0, topologyCOORD1, topologyCOORD2, sizeCOORD0, sizeCOORD1, sizeCOORD2);

        // send data to workers
        for(int i = 2; i < connections.size(); i ++) {
            // send the parent process to workers
            MPI_Send(&rank, 1, MPI_INT, connections[i], PARENTTAG, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, connections[i]);

            // send the final topology to workers
            MPI_Send(&sizeCOORD0, 1, MPI_INT, connections[i], 1, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, connections[i]);
            MPI_Send(&topologyCOORD0, sizeCOORD0, MPI_INT, connections[i], 1, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, connections[i]);

            MPI_Send(&sizeCOORD1, 1, MPI_INT, connections[i], 1, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, connections[i]);
            MPI_Send(&topologyCOORD1, sizeCOORD1, MPI_INT, connections[i], 1, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, connections[i]);

            MPI_Send(&sizeCOORD2, 1, MPI_INT, connections[i], 1, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, connections[i]);
            MPI_Send(&topologyCOORD2, sizeCOORD2, MPI_INT, connections[i], 1, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, connections[i]);
        }


        int numberOfTasks;
        MPI_Recv(&numberOfTasks, 1, MPI_INT, COORD0, 1, MPI_COMM_WORLD, &status);
        int tasks[numberOfTasks];

        // send tasks to workers, receive the final results for each part and
        // send the final results to first coordinator
        for(int i = 1; i < sizeCOORD2; i ++) {
            MPI_Recv(&tasks, numberOfTasks, MPI_INT, COORD0, 1, MPI_COMM_WORLD, &status);

            MPI_Send(&numberOfTasks, 1, MPI_INT, topologyCOORD2[i], 1, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, topologyCOORD2[i]);

            MPI_Send(&tasks, numberOfTasks, MPI_INT, topologyCOORD2[i], 1, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, topologyCOORD2[i]);

            MPI_Recv(&tasks, numberOfTasks, MPI_INT, topologyCOORD2[i], 1, MPI_COMM_WORLD, &status);

            MPI_Send(&tasks, numberOfTasks, MPI_INT, COORD0, 1, MPI_COMM_WORLD);
            printf("M(%d,%d)\n", rank, COORD0);
        }

    } else {
        int workerParent;
        int sizeCOORD0, sizeCOORD1, sizeCOORD2;

        // receive the parent process
        MPI_Recv(&workerParent, 1, MPI_INT, MPI_ANY_SOURCE, PARENTTAG, MPI_COMM_WORLD, &status);

        // receive the topologies
        MPI_Recv(&sizeCOORD0, 1, MPI_INT, workerParent, 1, MPI_COMM_WORLD, &status);
        int topologyCOORD0[sizeCOORD0];
        MPI_Recv(&topologyCOORD0, sizeCOORD0, MPI_INT, workerParent, 1, MPI_COMM_WORLD, &status);

        MPI_Recv(&sizeCOORD1, 1, MPI_INT, workerParent, 1, MPI_COMM_WORLD, &status);
        int topologyCOORD1[sizeCOORD1];
        MPI_Recv(&topologyCOORD1, sizeCOORD1, MPI_INT, workerParent, 1, MPI_COMM_WORLD, &status);

        MPI_Recv(&sizeCOORD2, 1, MPI_INT, workerParent, 1, MPI_COMM_WORLD, &status);
        int topologyCOORD2[sizeCOORD2];
        MPI_Recv(&topologyCOORD2, sizeCOORD2, MPI_INT, workerParent, 1, MPI_COMM_WORLD, &status);

        // print the final topology
        printTopology(rank, topologyCOORD0, topologyCOORD1, topologyCOORD2, sizeCOORD0, sizeCOORD1, sizeCOORD2);

        int numberOfTasks;

        // receive the tasks
        MPI_Recv(&numberOfTasks, 1, MPI_INT, workerParent, 1, MPI_COMM_WORLD, &status);
        int tasks[numberOfTasks];

        MPI_Recv(&tasks, numberOfTasks, MPI_INT, workerParent, 1, MPI_COMM_WORLD, &status);

        // compute the final results
        for (int i = 0; i < numberOfTasks; i++) {
            tasks[i] *= 2;
        }

        // send the final results to the coordinator process
        MPI_Send(&tasks, numberOfTasks, MPI_INT, workerParent, 1, MPI_COMM_WORLD);
        printf("M(%d,%d)\n", rank, workerParent);
    }

    MPI_Finalize();
}