#include <iostream>
#include <cstdio>
#include <mpi.h>
#include "core/utils.h"
#include "core/graph.h"

#define ROOT_PROCESS (0) 

long countTriangles(uintV *array1, uintE len1, uintV *array2, uintE len2,
                     uintV u, uintV v) {
  uintE i = 0, j = 0; // indexes for array1 and array2
  long count = 0;

  if (u == v)
    return count;

  while ((i < len1) && (j < len2)) {
    if (array1[i] == array2[j]) {
      if ((array1[i] != u) && (array1[i] != v)) {
        count++;
      } else {
        // triangle with self-referential edge -> ignore
      }
      i++;
      j++;
    } else if (array1[i] < array2[j]) {
      i++;
    } else {
      j++;
    }
  }
  return count;
}


void triangleCountSerial(Graph &g)
{
    uintV n = g.n_;
    long triangle_count = 0;
    double time_taken;
    timer t1;
    t1.start();
    for (uintV u = 0; u < n; u++)
    {
        uintE out_degree = g.vertices_[u].getOutDegree();
        for (uintE i = 0; i < out_degree; i++)
        {
            uintV v = g.vertices_[u].getOutNeighbor(i);
            triangle_count += countTriangles(g.vertices_[u].getInNeighbors(),
                                             g.vertices_[u].getInDegree(),
                                             g.vertices_[v].getOutNeighbors(),
                                             g.vertices_[v].getOutDegree(),
                                             u,
                                             v);
        }
    }

    // For every thread, print out the following statistics:
    // rank, edges, triangle_count, communication_time
    // 0, 17248443, 144441858, 0.000074
    // 1, 17248443, 152103585, 0.000020
    // 2, 17248443, 225182666, 0.000034
    // 3, 17248444, 185596640, 0.000022

    time_taken = t1.stop();

    // Print out overall statistics
    std::printf("Number of triangles : %ld\n", triangle_count);
    std::printf("Number of unique triangles : %ld\n", triangle_count / 3);
    std::printf("Time taken (in seconds) : %f\n", time_taken);
}

void  triangleCountParallel(Graph &g, int world_rank, int world_size ){
  double communication_time;
  timer t, c;
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
      if(i == world_rank)
          break;
  }

   // --- Triangle count start ---
  long global_count = 0;
  long local_count = 0;
  long edge_processed = 0;
  for (uintV u = start_vertex; u < end_vertex; u++) {
    uintE out_degree = g.vertices_[u].getOutDegree();
    edge_processed += out_degree;
    for (uintE i = 0; i < out_degree; i++) {
      uintV v = g.vertices_[u].getOutNeighbor(i);
      local_count += countTriangles(g.vertices_[u].getInNeighbors(),
                                        g.vertices_[u].getInDegree(),
                                        g.vertices_[v].getOutNeighbors(),
                                        g.vertices_[v].getOutDegree(), u, v);
    }
  }

  // --- synchronization phase start ---
  c.start();
  if(world_rank == ROOT_PROCESS){
    global_count = local_count;

    for (int i = 1; i<world_size; i++){
      long local_count_per_process;
      MPI_Recv(&local_count_per_process, 1, MPI_LONG, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      
      global_count += local_count_per_process;
    }
  }
  else{
    MPI_Send(&local_count, 1, MPI_LONG, ROOT_PROCESS, 0, MPI_COMM_WORLD);
  }
  communication_time = c.stop();

  // ------- Print Stats ---------
  std::string to_print = std::to_string(world_rank)+", "+
                         std::to_string(edge_processed)+", "+
                         std::to_string(local_count)+", "+
                         std::to_string(communication_time)+"\n";
  std::cout<<to_print;

  //----------- Only Print for Root process --------
  if(world_rank == ROOT_PROCESS){
    double total_time = t.stop();
    std::printf("Number of triangles : %ld\n", global_count);
    global_count /= 3;
    std::printf("Number of unique triangles : %ld\n", global_count);
    std::printf("Time taken (in seconds) : %g\n", total_time);
  }
}

int main(int argc, char *argv[])
{
    cxxopts::Options options("triangle_counting_serial", "Count the number of triangles using serial and parallel execution");
    options.add_options("custom", {
                                      {"strategy", "Strategy to be used", cxxopts::value<uint>()->default_value(DEFAULT_STRATEGY)},
                                      {"inputFile", "Input graph file path", cxxopts::value<std::string>()->default_value("/scratch/input_graphs/roadNet-CA")},
                                  });

    auto cl_options = options.parse(argc, argv);
    uint strategy = cl_options["strategy"].as<uint>();
    std::string input_file_path = cl_options["inputFile"].as<std::string>();

    if(strategy == 0){
      Graph g;
      g.readGraphFromBinary<int>(input_file_path);
      std::printf("Communication strategy : %d\n", strategy);
      triangleCountSerial(g);
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
        std::printf("World size : %d\n", world_size);
        std::printf("Communication strategy : %d\n", strategy);
        std::printf("rank, edges, triangle_count, communication_time\n");
    }

    Graph g;
    g.readGraphFromBinary<int>(input_file_path);

    triangleCountParallel(g, world_rank, world_size);

    // Finalize the MPI environment.
    MPI_Finalize();
    return 0;
}

