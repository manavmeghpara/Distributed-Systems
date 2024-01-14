#include "core/graph.h"
#include "core/utils.h"
#include <iomanip>
#include <iostream>
#include <stdlib.h>
#include <thread>

 
using namespace std::literals;
struct ThreadStats {
  uintV start;
  uintV end;
  uint thread_id;
  uint triangle_count;
  uint vertex_count;
  uint edge_count;
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

void threadCountTriangle (Graph &g, uintV start, uintV end, ThreadStats &t_stat){
  uint triangle_count = 0;
  uint num_vertex = 0;
  uint num_edge = 0;
  timer t1;

  t1.start();
  // Process each edge <u,v>
  for (uintV u = start; u < end; u++) {
    // For each outNeighbor v, find the intersection of inNeighbor(u) and
    // outNeighbor(v)
    num_vertex++;
    uintE out_degree = g.vertices_[u].getOutDegree();
    num_edge += out_degree;
    for (uintE i = 0; i < out_degree; i++) {
      uintV v = g.vertices_[u].getOutNeighbor(i);
      triangle_count += countTriangles(g.vertices_[u].getInNeighbors(),
                                       g.vertices_[u].getInDegree(),
                                       g.vertices_[v].getOutNeighbors(),
                                       g.vertices_[v].getOutDegree(), u, v);
    }
  }
  t_stat.edge_count = num_edge;
  t_stat.vertex_count = num_vertex;
  t_stat.triangle_count = triangle_count;
  t_stat.time_taken = t1.stop();
}

void triangleCountParallelVertexBased(Graph &g, uint workers) {
  uintV n = g.n_;
  long triangle_count = 0;
  double time_taken = 0.0;
  timer t1;
  timer p;

  t1.start();
  // The outNghs and inNghs for a given vertex are already sorted

  // Create threads and distribute the work across T threads
  // -------------------------------------------------------------------
  uint num_threads = workers;

  std::vector<std::thread> threads(num_threads);
  std::vector<ThreadStats> stats(num_threads);

  p.start();
  uintV subset = 0;
  uintV remain = n%num_threads;
  uintV extra = 0;
  for (uint i = 0; i<num_threads; i++) {
    if(remain>0){
      extra = 1;
      remain--;
    }
    stats[i].start = subset;
    stats[i].end = subset + (n/num_threads) + extra;
    subset += ((n/num_threads)+extra);
    extra = 0;
  }
  double partition_time = p.stop();

  for (uint i = 0; i<num_threads; i++) {
      stats[i].thread_id = i;
      threads[i] = std::thread(threadCountTriangle, std::ref(g), stats[i].start, stats[i].end, std::ref(stats[i]));
  }

  for (uint i = 0; i < num_threads; i++)
    threads[i].join();

  uint sum = 0;
  for (uint i = 0; i < num_threads; i++)
      sum += stats[i].triangle_count;
  time_taken = t1.stop();
  // -------------------------------------------------------------------
  // Here, you can just print the number of non-unique triangles counted by each
  // thread std::cout << "thread_id, triangle_count, time_taken\n"; Print the
  // above statistics for each thread Example output for 2 threads: thread_id,
  // triangle_count, time_taken 1, 102, 0.12 0, 100, 0.12
  std::cout << "thread_id, num_vertices, num_edges, triangle_count, time_taken\n";
  for (uint i = 0; i < num_threads; i++) {
    std::cout << stats[i].thread_id<< ", "
              << stats[i].vertex_count<< ", "
              << stats[i].edge_count<< ", "
              << stats[i].triangle_count<< ", "
              << stats[i].time_taken<< "\n";
  }
  // Print the overall statistics
  std::cout << "Number of triangles : " << sum << "\n";
  std::cout << "Number of unique triangles : " << sum / 3 << "\n";
  std::cout << "Partitioning time (in seconds) : " << partition_time << "\n";
  std::cout << "Time taken (in seconds) : " << std::setprecision(TIME_PRECISION)
            << time_taken << "\n";
            
}


void triangleCountParallelEdgeBased(Graph &g, uint workers) {
  uintV n = g.n_;
  uintE m = g.m_;
  long triangle_count = 0;
  double time_taken = 0.0;
  timer t1;
  timer p;
  
  t1.start();
  // The outNghs and inNghs for a given vertex are already sorted

  // Create threads and distribute the work across T threads
  // -------------------------------------------------------------------
  uint num_threads = workers;

  std::vector<std::thread> threads(num_threads);
  std::vector<ThreadStats> stats(num_threads);

  uintV vertexSubset = 0;
  uintE thresholdEdge = m/num_threads;
  double partition_time = 0.0;

  p.start();
  for (uint i = 0; i<num_threads; i++) {
    uintE edgeSubset =0;
    stats[i].start = vertexSubset;
    while(vertexSubset < n) {
      edgeSubset += g.vertices_[vertexSubset].getOutDegree();
      vertexSubset++;
      if(edgeSubset>=thresholdEdge)
        break;
    }
    stats[i].end = vertexSubset;
  }

  partition_time = p.stop();

  for (uint i = 0; i<num_threads; i++) {
      stats[i].thread_id = i;
      threads[i] = std::thread(threadCountTriangle, std::ref(g), stats[i].start, stats[i].end, std::ref(stats[i]));
  }

  for (uint i = 0; i < num_threads; i++)
    threads[i].join();

  uint sum = 0;
  for (uint i = 0; i < num_threads; i++)
      sum += stats[i].triangle_count;
  time_taken = t1.stop();
  // -------------------------------------------------------------------
  // Here, you can just print the number of non-unique triangles counted by each
  // thread std::cout << "thread_id, triangle_count, time_taken\n"; Print the
  // above statistics for each thread Example output for 2 threads: thread_id,
  // triangle_count, time_taken 1, 102, 0.12 0, 100, 0.12
  std::cout << "thread_id, num_vertices, num_edges, triangle_count, time_taken\n";
  for (uint i = 0; i < num_threads; i++) {
    std::cout << stats[i].thread_id<< ", "
              << "0"<< ", "
              << stats[i].edge_count<< ", "
              << stats[i].triangle_count<< ", "
              << stats[i].time_taken<< "\n";
  }
  // Print the overall statistics
  std::cout << "Number of triangles : " << sum << "\n";
  std::cout << "Number of unique triangles : " << sum / 3 << "\n";
  std::cout << "Partitioning time (in seconds) : " << partition_time << "\n";
  std::cout << "Time taken (in seconds) : " << std::setprecision(TIME_PRECISION)
            << time_taken << "\n";
}

void dynamicCount (Graph &g, std::atomic<uintV> &nextVertex, ThreadStats &t_stat){
  uint triangle_count = 0;
  uint num_vertex = 0;
  uint num_edge = 0;
  timer t1;

  t1.start();

  while(true){
      uintV u = nextVertex.fetch_add(1);
      if(u>=g.n_)
        break;
      uintE out_degree = g.vertices_[u].getOutDegree();
      num_edge += out_degree;
      num_vertex++;
      for (uintE i = 0; i < out_degree; i++) {
        uintV v = g.vertices_[u].getOutNeighbor(i);
        triangle_count += countTriangles(g.vertices_[u].getInNeighbors(),
                                        g.vertices_[u].getInDegree(),
                                        g.vertices_[v].getOutNeighbors(),
                                        g.vertices_[v].getOutDegree(), u, v);
      }
  }
  t_stat.edge_count = num_edge;
  t_stat.vertex_count = num_vertex;
  t_stat.triangle_count = triangle_count;
  t_stat.time_taken = t1.stop();
}

void triangleCountParallelDynamic(Graph &g, uint workers) {
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
  std::atomic<uintV> vertex_distribution (0);

  for (uint i = 0; i<num_threads; i++) {
      stats[i].thread_id = i;
      threads[i] = std::thread(dynamicCount, std::ref(g), std::ref(vertex_distribution), std::ref(stats[i]));
  }

  for (uint i = 0; i < num_threads; i++)
    threads[i].join();

  uint sum = 0;
  for (uint i = 0; i < num_threads; i++)
      sum += stats[i].triangle_count;
  time_taken = t1.stop();
  // -------------------------------------------------------------------
  // Here, you can just print the number of non-unique triangles counted by each
  // thread std::cout << "thread_id, triangle_count, time_taken\n"; Print the
  // above statistics for each thread Example output for 2 threads: thread_id,
  // triangle_count, time_taken 1, 102, 0.12 0, 100, 0.12
  std::cout << "thread_id, num_vertices, num_edges, triangle_count, time_taken\n";
  for (uint i = 0; i < num_threads; i++) {
    std::cout << stats[i].thread_id<< ", "
              << stats[i].vertex_count<< ", "
              << stats[i].edge_count<< ", "
              << stats[i].triangle_count<< ", "
              << stats[i].time_taken<< "\n";
  }
  // Print the overall statistics
  std::cout << "Number of triangles : " << sum << "\n";
  std::cout << "Number of unique triangles : " << sum / 3 << "\n";
  std::cout << "Partitioning time (in seconds) : " << "0" << "\n";
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
          {"strategy", "Strategy to be used",
           cxxopts::value<uint>()->default_value(DEFAULT_STRATEGY)},
          {"inputFile", "Input graph file path",
           cxxopts::value<std::string>()->default_value(
               "/scratch/input_graphs/roadNet-CA")},
      });

  auto cl_options = options.parse(argc, argv);
  uint n_workers = cl_options["nWorkers"].as<uint>();
  uint strategy = cl_options["strategy"].as<uint>();
  std::string input_file_path = cl_options["inputFile"].as<std::string>();
  std::cout << std::fixed;
  std::cout << "Number of workers : " << n_workers << "\n";
  std::cout << "Task decomposition strategy : " << strategy << "\n";

  Graph g;
  std::cout << "Reading graph\n";
  g.readGraphFromBinary<int>(input_file_path);
  std::cout << "Created graph\n";

  switch (strategy) {
  case 1:
    std::cout << "\nVertex-based work partitioning\n";
    triangleCountParallelVertexBased(g, n_workers);
    break;
  case 2:
    std::cout << "\nEdge-based work partitioning\n";
    triangleCountParallelEdgeBased(g, n_workers);
    break;
  case 3:
    std::cout << "\nDynamic task mapping\n";
    triangleCountParallelDynamic(g, n_workers);
    break;
  default:
    break;
  }

  return 0;
}
