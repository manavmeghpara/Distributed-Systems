#include <iostream>
#include <cstdio>
#include <mpi.h>
#include "core/utils.h"
#include "core/graph.h"

#ifdef USE_INT
#define INIT_PAGE_RANK 100000
#define EPSILON 1000
#define PAGE_RANK(x) (15000 + (5 * x) / 6)
#define CHANGE_IN_PAGE_RANK(x, y) std::abs(x - y)
#define PAGERANK_MPI_TYPE MPI_LONG
#define PR_FMT "%ld"
typedef int64_t PageRankType;
#else
#define INIT_PAGE_RANK 1.0
#define EPSILON 0.01
#define DAMPING 0.85
#define PAGE_RANK(x) (1 - DAMPING + DAMPING * x)
#define CHANGE_IN_PAGE_RANK(x, y) std::fabs(x - y)
#define PAGERANK_MPI_TYPE MPI_FLOAT
#define PR_FMT "%f"
#define ROOT_PROCESS (0) 
#define ROOT_PROCESSOR (0) 

typedef float PageRankType;
#endif

void pageRankSerial(Graph &g, int max_iters)
{
    uintV n = g.n_;
    double time_taken;
    timer t1;
    PageRankType *pr_curr = new PageRankType[n];
    PageRankType *pr_next = new PageRankType[n];

    t1.start();
    for (uintV i = 0; i < n; i++)
    {
        pr_curr[i] = INIT_PAGE_RANK;
        pr_next[i] = 0.0;
    }

    // Push based pagerank
    // -------------------------------------------------------------------
    for (int iter = 0; iter < max_iters; iter++)
    {
        // for each vertex 'u', process all its outNeighbors 'v'
        for (uintV u = 0; u < n; u++)
        {
            uintE out_degree = g.vertices_[u].getOutDegree();
            for (uintE i = 0; i < out_degree; i++)
            {
                uintV v = g.vertices_[u].getOutNeighbor(i);
                pr_next[v] += (pr_curr[u] / out_degree);
            }
        }
        for (uintV v = 0; v < n; v++)
        {
            pr_next[v] = PAGE_RANK(pr_next[v]);

            // reset pr_curr for the next iteration
            pr_curr[v] = pr_next[v];
            pr_next[v] = 0.0;
        }
    }
    // -------------------------------------------------------------------

    // For every thread, print the following statistics:
    // rank, num_edges, communication_time
    // 0, 344968860, 1.297778
    // 1, 344968860, 1.247763
    // 2, 344968860, 0.956243
    // 3, 344968880, 0.467028

    PageRankType sum_of_page_ranks = 0;
    for (uintV u = 0; u < n; u++)
    {
        sum_of_page_ranks += pr_curr[u];
    }
    time_taken = t1.stop();
    std::printf("Sum of page rank : " PR_FMT "\n", sum_of_page_ranks);
    std::printf("Time taken (in seconds) : %f\n", time_taken);
    delete[] pr_curr;
    delete[] pr_next;
}

void pageRankParallel(Graph &g, int max_iters, int world_rank, int world_size){
    PageRankType *pr_curr = new PageRankType[g.n_];
    PageRankType *pr_next = new PageRankType[g.n_];
    PageRankType *buffer;
    uintV *size_partition = new uintV[world_size];
    long edge_processed = 0;
    double communication_time;
    timer t, c;

    for (uintV i = 0; i < g.n_; i++) {
        pr_curr[i] = INIT_PAGE_RANK;
        pr_next[i] = 0.0;
    }

    if(world_rank == ROOT_PROCESS){
        t.start();
    }

    // --- Edge Based Partition of graph ---
    uintV start_vertex(0), end_vertex(0);
    for(int i=0; i<world_size; i++){
        start_vertex=end_vertex;
        long count = 0;
        while (end_vertex < g.n_)
        {
            // add vertices until we reach m/P edges.
            count += g.vertices_[end_vertex].getOutDegree();
            end_vertex += 1;
            if (count >= g.m_/world_size)
                break;
        }
        size_partition[i] = start_vertex;
    }

    if(world_rank == ROOT_PROCESS){
        start_vertex = 0;
        end_vertex = size_partition[1];
        buffer = new PageRankType[g.n_];
    }
    else{
        if(world_rank < world_size-1){
            start_vertex = size_partition[world_rank];
            end_vertex = size_partition[world_rank+1];
        }
        else{
            start_vertex = size_partition[world_rank];
            end_vertex = g.n_;
        }
    }


    for (int iter = 0; iter < max_iters; iter++)
    {
        // for each vertex 'u', process all its outNeighbors 'v'
        for (uintV u = start_vertex; u < end_vertex; u++)
        {
            uintE out_degree = g.vertices_[u].getOutDegree();
            edge_processed += out_degree;
            for (uintE i = 0; i < out_degree; i++)
            {
                uintV v = g.vertices_[u].getOutNeighbor(i);
                pr_next[v] += (pr_curr[u] / out_degree);
            }
        }

        // --- synchronization phase 1 start ---
        c.start();
        if(world_rank == ROOT_PROCESS){
            for (int i = 1; i < world_size; i++){

                MPI_Recv(buffer, g.n_, PAGERANK_MPI_TYPE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                for(int j=0; j<g.n_; j++)
                    pr_next[j] += buffer[j];
                       
            }
            for (int i = 1; i < world_size; i++){
                int count=0;
                if(i < world_size - 1)
                    count = size_partition[i+1] - size_partition[i];
                else
                    count = g.n_ - size_partition[i];
                
                MPI_Send(&pr_next[size_partition[i]], count,
                            PAGERANK_MPI_TYPE, i, 0, MPI_COMM_WORLD);                    
            }
        }
        else{
            MPI_Send(pr_next, g.n_, PAGERANK_MPI_TYPE, ROOT_PROCESS, 0, MPI_COMM_WORLD);

            MPI_Recv(&pr_next[size_partition[world_rank]], (end_vertex-start_vertex), PAGERANK_MPI_TYPE, ROOT_PROCESS, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }
        communication_time += c.stop();
        // --- synchronization phase 1 ends ---

        for (uintV v = 0; v < g.n_; v++)
        {

            pr_curr[v] = PAGE_RANK(pr_next[v]);
            // reset pr_curr for the next iteration
            pr_next[v] = 0.0;
        }
    }

    PageRankType local_sum = 0;
    PageRankType total_sum = 0;
    for(uintV u = start_vertex; u<end_vertex; u++){
        local_sum += pr_curr[u];
    }


    if(world_rank == ROOT_PROCESS){
        total_sum = local_sum;

        for (int i = 1; i<world_size; i++){
            PageRankType local_sum_per_process;
            MPI_Recv(&local_sum_per_process, 1, PAGERANK_MPI_TYPE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            
            total_sum += local_sum_per_process;
        }
    }
    else{
        MPI_Send(&local_sum, 1, PAGERANK_MPI_TYPE, ROOT_PROCESS, 0, MPI_COMM_WORLD);
    }
    
    std::string to_print =  std::to_string(world_rank) + ", "+
                            std::to_string(edge_processed) + ", " +
                            std::to_string(communication_time) +"\n";
    std::cout << to_print;

    if(world_rank == ROOT_PROCESS){
        double total_time = t.stop();
        std::cout<<"Sum of page rank : "+ std::to_string(total_sum)+ "\n" ;
        std::printf("Time taken (in seconds) : %g\n", total_time);
        delete[]buffer;
    }
    delete[]size_partition;

    delete[]pr_curr;
    delete[]pr_next;


}


int main(int argc, char *argv[])
{
    cxxopts::Options options("page_rank_push", "Calculate page_rank using serial and parallel execution");
    options.add_options("", {
                                {"nIterations", "Maximum number of iterations", cxxopts::value<uint>()->default_value(DEFAULT_MAX_ITER)},
                                {"strategy", "Strategy to be used", cxxopts::value<uint>()->default_value(DEFAULT_STRATEGY)},
                                {"inputFile", "Input graph file path", cxxopts::value<std::string>()->default_value("/scratch/input_graphs/roadNet-CA")},
                            });

    auto cl_options = options.parse(argc, argv);
    uint strategy = cl_options["strategy"].as<uint>();
    uint max_iterations = cl_options["nIterations"].as<uint>();
    std::string input_file_path = cl_options["inputFile"].as<std::string>();



    if(strategy == 0){
        #ifdef USE_INT
            std::printf("Using INT\n");
        #else
            std::printf("Using FLOAT\n");
        #endif
        Graph g;
        g.readGraphFromBinary<int>(input_file_path);
        std::printf("Communication strategy : %d\n", strategy);
        std::printf("Iterations : %d\n", max_iterations);

        pageRankSerial(g, max_iterations);
        return 0;
    }
    // Else strategy 1 use multiple processor.

    MPI_Init(NULL, NULL);

    // Get the number of processes
    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    // Get the rank of the process
    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    // Get the name of the processor
    char processor_name[MPI_MAX_PROCESSOR_NAME];
    int name_len;
    MPI_Get_processor_name(processor_name, &name_len);

    // Get the world size and print it out here
    if(world_rank == 0){
        #ifdef USE_INT
            std::printf("Using INT\n");
        #else
            std::printf("Using FLOAT\n");
        #endif
        std::printf("World size : %d\n", world_size);
        std::printf("Communication strategy : %d\n", strategy);
        std::printf("Iterations : %d\n", max_iterations);
        std::printf("rank, num_edges, communication_time\n");
    }

    Graph g;
    g.readGraphFromBinary<int>(input_file_path);

    pageRankParallel(g, max_iterations, world_rank, world_size);

    // Finalize the MPI environment.
    MPI_Finalize();
    return 0;
}
