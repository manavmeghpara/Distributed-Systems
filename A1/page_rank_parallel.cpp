#include "core/graph.h"
#include "core/utils.h"
#include <iomanip>
#include <iostream>
#include <stdlib.h>
#include <thread>
#include <mutex>

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
  double time_taken;
};

void workForThreads(Graph &g, int max_iters ,uintV start, uintV end, PageRankType* curr, PageRankType* next, CustomBarrier &my_barrier, std::vector<std::mutex> &pr_mutex, ThreadStats &t_stat){
  timer t1;
  t1.start();
  for (int iter = 0; iter < max_iters; iter++) {
    for (uintV u = start; u < end; u++) {
      uintE out_degree = g.vertices_[u].getOutDegree();
      for (uintE i = 0; i < out_degree; i++) {
        uintV v = g.vertices_[u].getOutNeighbor(i);
        std::lock_guard<std::mutex> lock(pr_mutex[v]);
        next[v] += (curr[u] / out_degree);
      }
    }

    my_barrier.wait();

    for (uintV v = start; v < end; v++) {
      next[v] = PAGE_RANK(next[v]);

      // reset pr_curr for the next iteration
      curr[v] = next[v];
      next[v] = 0.0;
    }
    my_barrier.wait();
  }
  t_stat.time_taken = t1.stop();
}

void pageRankParallel(Graph &g, int max_iters, uint num_threads) {
  uintV n = g.n_;

  PageRankType *pr_curr = new PageRankType[n];
  PageRankType *pr_next = new PageRankType[n];

  for (uintV i = 0; i < n; i++) {
    pr_curr[i] = INIT_PAGE_RANK;
    pr_next[i] = 0.0;
  }

  // Push based pagerank
  timer t1;
  double time_taken = 0.0;
  // Create threads and distribute the work across T threads
  // -------------------------------------------------------------------
  std::vector<std::thread> threads(num_threads);
  std::vector<ThreadStats> stats(num_threads);
  CustomBarrier my_barrier(num_threads);
  std::vector<std::mutex> vec_mutex(n); 
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
      threads[i] = std::thread(workForThreads, std::ref(g), max_iters, subset, subset+(n/num_threads)+extra, pr_curr, pr_next, std::ref(my_barrier), std::ref(vec_mutex), std::ref(stats[i]));
      subset += ((n/num_threads)+extra);
      extra = 0;
  }

  for (uint i = 0; i < num_threads; i++) {
    threads[i].join();
  }
  time_taken = t1.stop();
  // -------------------------------------------------------------------
  // std::cout << "thread_id, time_taken\n";
  // Print the above statistics for each thread
  // Example output for 2 threads:
  // thread_id, time_taken
  // 0, 0.12
  // 1, 0.12
  std::cout << "thread_id, time_taken\n";
  for (uint i = 0; i < num_threads; i++) {
    std::cout << stats[i].thread_id<< ", "
              << stats[i].time_taken<< "\n";
  }

  PageRankType sum_of_page_ranks = 0;
  for (uintV u = 0; u < n; u++) {
    sum_of_page_ranks += pr_curr[u];
  }
  std::cout << "Sum of page rank : " << sum_of_page_ranks << "\n";
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
          {"inputFile", "Input graph file path",
           cxxopts::value<std::string>()->default_value(
               "/scratch/input_graphs/roadNet-CA")},
      });

  auto cl_options = options.parse(argc, argv);
  uint n_workers = cl_options["nWorkers"].as<uint>();
  uint max_iterations = cl_options["nIterations"].as<uint>();
  std::string input_file_path = cl_options["inputFile"].as<std::string>();

#ifdef USE_INT
  std::cout << "Using INT\n";
#else
  std::cout << "Using FLOAT\n";
#endif
  std::cout << std::fixed;
  std::cout << "Number of workers : " << n_workers << "\n";

  Graph g;
  std::cout << "Reading graph\n";
  g.readGraphFromBinary<int>(input_file_path);
  std::cout << "Created graph\n";
  pageRankParallel(g, max_iterations, n_workers);

  return 0;
}
