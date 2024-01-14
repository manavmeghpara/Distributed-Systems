#include "core/graph.h"
#include "core/utils.h"
#include <iomanip>
#include <iostream>
#include <stdlib.h>
#include <thread>

#ifdef USE_INT
#define INIT_PAGE_RANK 100000
#define EPSILON 1000
#define PAGE_RANK(x) (15000 + (5 * x) / 6)
#define CHANGE_IN_PAGE_RANK(x, y) std::abs(x - y)
typedef int64_t PageRankType;
#else
#define INIT_PAGE_RANK 1.0
#define EPSILON 0.01
#define DAMPING 0.85
#define PAGE_RANK(x) (1 - DAMPING + DAMPING * x)
#define CHANGE_IN_PAGE_RANK(x, y) std::fabs(x - y)
typedef float PageRankType;
#endif

struct ThreadStats {
  uint thread_id;
  uint vertex_count;
  uint edge_count;
  uintV start;
  uintV end;
  double barrier1_time;
  double barrier2_time;
  double getNextVertex_time;
  double time_taken;
};

void workForThreads(Graph &g, int max_iters ,uintV start, uintV end, PageRankType* curr, std::atomic<PageRankType> *next, CustomBarrier &my_barrier, ThreadStats &t_stat){
  timer t1;
  timer b1;
  timer b2;
  uint num_vertex = 0;
  uint num_edge = 0;
  t1.start();
  for (int iter = 0; iter < max_iters; iter++) {
    for (uintV u = start; u < end; u++) {
      uintE out_degree = g.vertices_[u].getOutDegree();
      num_edge += out_degree;
      for (uintE i = 0; i < out_degree; i++) {
        uintV v = g.vertices_[u].getOutNeighbor(i);
        PageRankType current = next[v];
        while(!next[v].compare_exchange_weak(current, current+(curr[u] / out_degree)));
      }
    }
    b1.start();
    my_barrier.wait();
    t_stat.barrier1_time = b1.stop();

    for (uintV v = start; v < end; v++) {
      num_vertex++;
      next[v] = PAGE_RANK(next[v]);

      // reset pr_curr for the next iteration
      curr[v] = next[v];
      next[v] = 0.0;
    }

    b2.start();
    my_barrier.wait();
    t_stat.barrier2_time = b2.stop();
  }
  t_stat.edge_count = num_edge;
  t_stat.vertex_count = num_vertex;
  t_stat.time_taken = t1.stop();
}

void pageRankParallelVertexBased(Graph &g, int max_iters, uint num_threads) {
  uintV n = g.n_;

  PageRankType *pr_curr = new PageRankType[n];
  std::atomic<PageRankType> *pr_next = new std::atomic<PageRankType> [n];

  for (uintV i = 0; i < n; i++) {
    pr_curr[i] = INIT_PAGE_RANK;
    pr_next[i] = 0.0;
  }

  // Push based pagerank
  timer t1;
  timer p;
  double time_taken = 0.0;
  // Create threads and distribute the work across T threads
  // -------------------------------------------------------------------
  std::vector<std::thread> threads(num_threads);
  std::vector<ThreadStats> stats(num_threads);
  CustomBarrier my_barrier(num_threads);

  t1.start();

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
      stats[i].getNextVertex_time = 0.0;
      threads[i] = std::thread(workForThreads, std::ref(g), max_iters, stats[i].start, stats[i].end, pr_curr, pr_next, std::ref(my_barrier), std::ref(stats[i]));
  }

  for (uint i = 0; i < num_threads; i++)
    threads[i].join();
  
  time_taken = t1.stop();
  // -------------------------------------------------------------------
  // std::cout << "thread_id, time_taken\n";
  // Print the above statistics for each thread
  // Example output for 2 threads:
  // thread_id, time_taken
  // 0, 0.12
  // 1, 0.12
  std::cout << "thread_id, num_vertices, num_edges, barrier1_time, barrier2_time, getNextVertex_time, total_time\n";
  for (uint i = 0; i < num_threads; i++) {
    std::cout << stats[i].thread_id<< ", "
              << stats[i].vertex_count<< ", "
              << stats[i].edge_count<< ", "
              << stats[i].barrier1_time<< ", "
              << stats[i].barrier2_time<< ", "
              << stats[i].getNextVertex_time<< ", "
              << stats[i].time_taken<< "\n";
  }

  PageRankType sum_of_page_ranks = 0;
  for (uintV u = 0; u < n; u++) {
    sum_of_page_ranks += pr_curr[u];
  }
  std::cout << "Sum of page rank : " << sum_of_page_ranks << "\n";
  std::cout << "Partitioning time (in seconds) : " << partition_time << "\n";
  std::cout << "Time taken (in seconds) : " << time_taken << "\n";
  delete[] pr_curr;
  delete[] pr_next;
}

void pageRankParallelEdgeBased(Graph &g, int max_iters, uint num_threads) {
  uintV n = g.n_;
  uintE m = g.m_;

  PageRankType *pr_curr = new PageRankType[n];
  std::atomic<PageRankType> *pr_next = new std::atomic<PageRankType> [n];

  for (uintV i = 0; i < n; i++) {
    pr_curr[i] = INIT_PAGE_RANK;
    pr_next[i] = 0.0;
  }

  // Push based pagerank
  timer t1;
  timer p;
  double time_taken = 0.0;
  // Create threads and distribute the work across T threads
  // -------------------------------------------------------------------
  std::vector<std::thread> threads(num_threads);
  std::vector<ThreadStats> stats(num_threads);
  CustomBarrier my_barrier(num_threads);

  t1.start();

  uintV vertexSubset = 0;
  uintE thresholdEdge = m/num_threads;

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
  double partition_time = p.stop();

  for (uint i = 0; i<num_threads; i++) {
      stats[i].thread_id = i;
      stats[i].getNextVertex_time = 0.0;
      threads[i] = std::thread(workForThreads, std::ref(g), max_iters, stats[i].start, stats[i].end, pr_curr, pr_next, std::ref(my_barrier), std::ref(stats[i]));
  }

  for (uint i = 0; i < num_threads; i++)
    threads[i].join();
  
  time_taken = t1.stop();
  // -------------------------------------------------------------------
  // std::cout << "thread_id, time_taken\n";
  // Print the above statistics for each thread
  // Example output for 2 threads:
  // thread_id, time_taken
  // 0, 0.12
  // 1, 0.12
  std::cout << "thread_id, num_vertices, num_edges, barrier1_time, barrier2_time, getNextVertex_time, total_time\n";
  for (uint i = 0; i < num_threads; i++) {
    std::cout << stats[i].thread_id<< ", "
              << stats[i].vertex_count<< ", "
              << stats[i].edge_count<< ", "
              << stats[i].barrier1_time<< ", "
              << stats[i].barrier2_time<< ", "
              << stats[i].getNextVertex_time<< ", "
              << stats[i].time_taken<< "\n";
  }

  PageRankType sum_of_page_ranks = 0;
  for (uintV u = 0; u < n; u++) {
    sum_of_page_ranks += pr_curr[u];
  }
  std::cout << "Sum of page rank : " << sum_of_page_ranks << "\n";
  std::cout << "Partitioning time (in seconds) : " << partition_time << "\n";
  std::cout << "Time taken (in seconds) : " << time_taken << "\n";
  delete[] pr_curr;
  delete[] pr_next;
}

void dynamicWorkAllocation (Graph &g, int max_iters, PageRankType* curr, std::atomic<PageRankType> *next, CustomBarrier &my_barrier, ThreadStats &t_stat, std::atomic<uintV>& vertex_distribution_for_barrier1, std::atomic<uintV>& vertex_distribution_for_barrier2){
  timer t1;
  timer b1;
  timer b2;
  timer getNext;
  uint num_vertex = 0;
  uint num_edge = 0;
  t1.start();
  for (int iter = 0; iter < max_iters; iter++) {
    while (true) {
      getNext.start();
      uintV u = vertex_distribution_for_barrier1.fetch_add(1);
      t_stat.getNextVertex_time += getNext.stop();
      if(u>=g.n_) 
        break;
      uintE out_degree = g.vertices_[u].getOutDegree();
      num_edge += out_degree;
      for (uintE i = 0; i < out_degree; i++) {
        uintV v = g.vertices_[u].getOutNeighbor(i);
        PageRankType current = next[v];
        while(!next[v].compare_exchange_weak(current, current+(curr[u] / out_degree)));
      }
    }
    vertex_distribution_for_barrier2 = 0;
    b1.start();
    my_barrier.wait();
    t_stat.barrier1_time += b1.stop();

    while (true) {
      getNext.start();
      uintV v = vertex_distribution_for_barrier2.fetch_add(1);
      t_stat.getNextVertex_time += getNext.stop();
      if(v>=g.n_) 
        break;
      num_vertex++;
      next[v] = PAGE_RANK(next[v]);

      // reset pr_curr for the next iteration
      curr[v] = next[v];
      next[v] = 0.0;
    }
    vertex_distribution_for_barrier1 = 0;
    b2.start();
    my_barrier.wait();
    t_stat.barrier2_time += b2.stop();
  }
  t_stat.edge_count = num_edge;
  t_stat.vertex_count = num_vertex;
  t_stat.time_taken = t1.stop();
}

void dynamicWorkAllocationWithGranularity (Graph &g, int max_iters, int granularity, PageRankType* curr, std::atomic<PageRankType> *next, CustomBarrier &my_barrier, ThreadStats &t_stat, std::atomic<uintV>& vertex_distribution_for_barrier1, std::atomic<uintV>& vertex_distribution_for_barrier2){
  timer t1;
  timer b1;
  timer b2;
  timer getNext;
  uint num_vertex = 0;
  uint num_edge = 0;
  t1.start();
  for (int iter = 0; iter < max_iters; iter++) {
    while (true) {
      getNext.start();
      uintV u = vertex_distribution_for_barrier1.fetch_add(granularity);
      t_stat.getNextVertex_time += getNext.stop();
      if(u>=g.n_) break;
      for(int j=0; j<granularity; j++){
        uintE out_degree = g.vertices_[u].getOutDegree();
        num_edge += out_degree;
        for (uintE i = 0; i < out_degree; i++) {
          uintV v = g.vertices_[u].getOutNeighbor(i);
          PageRankType current = next[v];
          while(!next[v].compare_exchange_weak(current, current+(curr[u] / out_degree)));
        }
        u++;
        if(u >= g.n_) break;
      }
    }
    vertex_distribution_for_barrier2 = 0;
    b1.start();
    my_barrier.wait();
    t_stat.barrier1_time += b1.stop();

    while (true) {
      getNext.start();
      uintV v = vertex_distribution_for_barrier2.fetch_add(granularity);
      t_stat.getNextVertex_time += getNext.stop();
      if(v>=g.n_) break;
      for(int j=0; j<granularity; j++){
        num_vertex++;
        next[v] = PAGE_RANK(next[v]);

        // reset pr_curr for the next iteration
        curr[v] = next[v];
        next[v] = 0.0;
        v++;
        if(v >= g.n_) break;
      }
    }

    vertex_distribution_for_barrier1 = 0;
    b2.start();
    my_barrier.wait();
    t_stat.barrier2_time += b2.stop();
  }
  t_stat.edge_count = num_edge;
  t_stat.vertex_count = num_vertex;
  t_stat.time_taken = t1.stop();
}

void pageRankParallelDynamic(Graph &g, int max_iters, uint num_threads, int granularity) {
  uintV n = g.n_;

  PageRankType *pr_curr = new PageRankType[n];
  std::atomic<PageRankType> *pr_next = new std::atomic<PageRankType> [n];

  for (uintV i = 0; i < n; i++) {
    pr_curr[i] = INIT_PAGE_RANK;
    pr_next[i] = 0.0;
  }

  // Push based pagerank
  timer t1;
  timer p;
  double time_taken = 0.0;
  // Create threads and distribute the work across T threads
  // -------------------------------------------------------------------
  std::vector<std::thread> threads(num_threads);
  std::vector<ThreadStats> stats(num_threads);
  CustomBarrier my_barrier(num_threads);
  std::atomic<uintV> vertex_distribution_for_barrier1 (0);
  std::atomic<uintV> vertex_distribution_for_barrier2 (0);

  t1.start();

  if(granularity == 1){
    for (uint i = 0; i<num_threads; i++) {
        stats[i].thread_id = i;
        threads[i] = std::thread(dynamicWorkAllocation, std::ref(g), max_iters, pr_curr, pr_next, std::ref(my_barrier), std::ref(stats[i]), 
                                std::ref(vertex_distribution_for_barrier1), std::ref(vertex_distribution_for_barrier2));
    }
  }
  else {
    for (uint i = 0; i<num_threads; i++) {
        stats[i].thread_id = i;
        threads[i] = std::thread(dynamicWorkAllocationWithGranularity, std::ref(g), max_iters, granularity, pr_curr, pr_next, std::ref(my_barrier), std::ref(stats[i]), 
                                std::ref(vertex_distribution_for_barrier1), std::ref(vertex_distribution_for_barrier2));
    }
  }

  for (uint i = 0; i < num_threads; i++)
    threads[i].join();
  
  time_taken = t1.stop();
  // -------------------------------------------------------------------
  // std::cout << "thread_id, time_taken\n";
  // Print the above statistics for each thread
  // Example output for 2 threads:
  // thread_id, time_taken
  // 0, 0.12
  // 1, 0.12
  std::cout << "thread_id, num_vertices, num_edges, barrier1_time, barrier2_time, getNextVertex_time, total_time\n";
  for (uint i = 0; i < num_threads; i++) {
    std::cout << stats[i].thread_id<< ", "
              << stats[i].vertex_count<< ", "
              << stats[i].edge_count<< ", "
              << stats[i].barrier1_time<< ", "
              << stats[i].barrier2_time<< ", "
              << stats[i].getNextVertex_time<< ", "
              << stats[i].time_taken<< "\n";
  }

  PageRankType sum_of_page_ranks = 0;
  for (uintV u = 0; u < n; u++) {
    sum_of_page_ranks += pr_curr[u];
  }
  std::cout << "Sum of page rank : " << sum_of_page_ranks << "\n";
  std::cout << "Partitioning time (in seconds) : " << "0" << "\n";
  std::cout << "Time taken (in seconds) : " << time_taken << "\n";
  delete[] pr_curr;
  delete[] pr_next;
}

int main(int argc, char *argv[]) {
  cxxopts::Options options(
      "page_rank_push",
      "Calculate page_rank using serial and parallel execution");
  options.add_options(
      "",
      {
          {"nWorkers", "Number of workers",
           cxxopts::value<uint>()->default_value(DEFAULT_NUMBER_OF_WORKERS)},
          {"nIterations", "Maximum number of iterations",
           cxxopts::value<uint>()->default_value(DEFAULT_MAX_ITER)},
          {"strategy", "Strategy to be used",
           cxxopts::value<uint>()->default_value(DEFAULT_STRATEGY)},
          {"granularity", "Granularity to be used",
           cxxopts::value<uint>()->default_value(DEFAULT_GRANULARITY)},
          {"inputFile", "Input graph file path",
           cxxopts::value<std::string>()->default_value(
               "/scratch/input_graphs/roadNet-CA")},
      });

  auto cl_options = options.parse(argc, argv);
  uint n_workers = cl_options["nWorkers"].as<uint>();
  uint strategy = cl_options["strategy"].as<uint>();
  uint max_iterations = cl_options["nIterations"].as<uint>();
  uint granularity = cl_options["granularity"].as<uint>();
  std::string input_file_path = cl_options["inputFile"].as<std::string>();

#ifdef USE_INT
  std::cout << "Using INT\n";
#else
  std::cout << "Using FLOAT\n";
#endif
  std::cout << std::fixed;
  std::cout << "Number of workers : " << n_workers << "\n";
  std::cout << "Task decomposition strategy : " << strategy << "\n";
  std::cout << "Iterations : " << max_iterations << "\n";
  std::cout << "Granularity : " << granularity << "\n";

  Graph g;
  std::cout << "Reading graph\n";
  g.readGraphFromBinary<int>(input_file_path);
  std::cout << "Created graph\n";
  switch (strategy) {
  case 1:
    std::cout << "\nVertex-based work partitioning\n";
    pageRankParallelVertexBased(g, max_iterations, n_workers);
    break;
  case 2:
    std::cout << "\nEdge-based work partitioning\n";
    pageRankParallelEdgeBased(g, max_iterations, n_workers);
    break;
  case 3:
    std::cout << "\nDynamic task mapping\n";
    pageRankParallelDynamic(g, max_iterations, n_workers, granularity);
    break;
  default:
    break;
  }

  return 0;
}
