#include "core/graph.h"
#include "core/utils.h"
#include <future>
#include <iomanip>
#include <iostream>
#include <stdlib.h>
#include <thread>

struct ThreadStats {
  uint thread_id;
  uint triangle_count;
  double time_taken;
};

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

void threadCountTriangle (Graph &g, uintV start, uintV end, ThreadStats &t_stat){
  uint triangle_count = 0;
  timer t1;

  t1.start();
  // Process each edge <u,v>
  for (uintV u = start; u < end; u++) {
    // For each outNeighbor v, find the intersection of inNeighbor(u) and
    // outNeighbor(v)
    uintE out_degree = g.vertices_[u].getOutDegree();
    for (uintE i = 0; i < out_degree; i++) {
      uintV v = g.vertices_[u].getOutNeighbor(i);
      triangle_count += countTriangles(g.vertices_[u].getInNeighbors(),
                                       g.vertices_[u].getInDegree(),
                                       g.vertices_[v].getOutNeighbors(),
                                       g.vertices_[v].getOutDegree(), u, v);
    }
  }
  t_stat.triangle_count = triangle_count;
  t_stat.time_taken = t1.stop();
}

void triangleCountParallel(Graph &g, uint workers) {
  uintV n = g.n_;
  long triangle_count = 0;
  double time_taken = 0.0;
  timer t1;
  
  t1.start();
  // The outNghs and inNghs for a given vertex are already sorted

  // Create threads and distribute the work across T threads
  // -------------------------------------------------------------------
  uint num_threads = workers;

  std::vector<std::thread> threads(num_threads);
  std::vector<ThreadStats> stats(num_threads);

  uintV subset = 0;
  uintV remain = n%num_threads;
  uintV extra = 0;

  t1.start();

  for (uint i = 0; i<num_threads; i++) {
      if(remain>0){
        extra = 1;
        remain--;
      }
      stats[i].thread_id = i;
      threads[i] = std::thread(threadCountTriangle, std::ref(g), subset, subset+(n/num_threads)+extra, std::ref(stats[i]));
      subset += ((n/num_threads)+extra);
      extra = 0;
  }

  for (uint i = 0; i < num_threads; i++) {
    threads[i].join();
  }

  uint sum = 0;
  for (uint i = 0; i < num_threads; i++) {
      sum += stats[i].triangle_count;
  }
  time_taken = t1.stop();
  // -------------------------------------------------------------------
  // Here, you can just print the number of non-unique triangles counted by each
  // thread std::cout << "thread_id, triangle_count, time_taken\n"; Print the
  // above statistics for each thread Example output for 2 threads: thread_id,
  // triangle_count, time_taken 1, 102, 0.12 0, 100, 0.12
  std::cout << "thread_id, triangle_count, time_taken\n";
  for (uint i = 0; i < num_threads; i++) {
    std::cout << stats[i].thread_id<< ", "
              << stats[i].triangle_count<< ", "
              << stats[i].time_taken<< "\n";
  }
  // Print the overall statistics
  std::cout << "Number of triangles : " << sum << "\n";
  std::cout << "Number of unique triangles : " << sum / 3 << "\n";
  std::cout << "Time taken (in seconds) : " << std::setprecision(TIME_PRECISION)
            << time_taken << "\n";
            
}

int main(int argc, char *argv[]) {
  cxxopts::Options options(
      "triangle_counting_serial",
      "Count the number of triangles using serial and parallel execution");
  options.add_options(
      "custom",
      {
          {"nWorkers", "Number of workers",
           cxxopts::value<uint>()->default_value(DEFAULT_NUMBER_OF_WORKERS)},
          {"inputFile", "Input graph file path",
           cxxopts::value<std::string>()->default_value(
               "/scratch/input_graphs/roadNet-CA")},
      });

  auto cl_options = options.parse(argc, argv);
  uint n_workers = cl_options["nWorkers"].as<uint>();
  std::string input_file_path = cl_options["inputFile"].as<std::string>();
  std::cout << std::fixed;
  std::cout << "Number of workers : " << n_workers << "\n";

  Graph g;
  std::cout << "Reading graph\n";
  g.readGraphFromBinary<int>(input_file_path);
  std::cout << "Created graph\n";

  triangleCountParallel(g, n_workers);

  return 0;
}
