#include "core/utils.h"
#include <iomanip>
#include <iostream>
#include <stdlib.h>
#include <atomic>
#include <thread>
#include <vector>
#include <ctime>

#define sqr(x) ((x) * (x))
#define DEFAULT_NUMBER_OF_POINTS "12345678"

struct ThreadStats {
  uint thread_id;
  uint points_generated;
  uint circle_points;
  double time_taken;
};

uint c_const = (uint)RAND_MAX + (uint)1;
inline double get_random_coordinate(uint *random_seed) {
  return ((double)rand_r(random_seed)) / c_const;
}

void get_points_in_circle(uint n, uint random_seed, ThreadStats &t_stat) {
  timer t;
  t.start();
  uint local_circle_count = 0;
  t_stat.circle_points = 0;
  t_stat.points_generated = 0;
  t_stat.time_taken = 0;
  double x_coord, y_coord;
  for (uint i = 0; i < n; i++) {
    x_coord = (2.0 * get_random_coordinate(&random_seed)) - 1.0;
    y_coord = (2.0 * get_random_coordinate(&random_seed)) - 1.0;
    if ((sqr(x_coord) + sqr(y_coord)) <= 1.0)
      local_circle_count++;

  }
  t_stat.points_generated = n;
  t_stat.circle_points = local_circle_count;
  t_stat.time_taken = t.stop();
}

void piCalculation(uint n, uint workers) {
  timer serial_timer;
  double time_taken = 0.0;

  serial_timer.start();
  // Create threads and distribute the work across T threads
  // -------------------------------------------------------------------
  uint num_threads = workers;

  std::vector<std::thread> threads(num_threads);
  std::vector<ThreadStats> stats(num_threads);

  srand(time(0));
  uint seed_arr[num_threads];
  for(uint i=0; i<num_threads; i++)
    seed_arr[i] = random();
  
  uint range = n / num_threads;
  uint remain = n%num_threads;
  uint extra = 0;

  for (uint i = 0; i < num_threads; i++) {
      if(remain>0){
        extra = 1;
        remain--;
      }
      stats[i].thread_id = i;
      threads[i] = std::thread(get_points_in_circle, range+extra, seed_arr[i], std::ref(stats[i]));
      extra = 0;
  }

  for (uint i = 0; i < num_threads; i++) {
      threads[i].join();
  }

  uint sum = 0;
  for (uint i = 0; i < num_threads; i++) {
      sum += stats[i].circle_points;
  }
  double pi_value = 4.0 * (double)sum / (double)n;
  // -------------------------------------------------------------------
  time_taken = serial_timer.stop();

  std::cout << "thread_id, points_generated, circle_points, time_taken\n";
  // Print the above statistics for each thread
  // Example output for 2 threads:
  // thread_id, points_generated, circle_points, time_taken
  // 1, 100, 90, 0.12
  // 0, 100, 89, 0.12
  for (uint i = 0; i < num_threads; i++) {
    std::cout << stats[i].thread_id<< ", "
              << stats[i].points_generated<< ", "
              << stats[i].circle_points<< ", "
              << stats[i].time_taken<< "\n";
  }
  // Print the overall statistics
  std::cout << "Total points generated : " << n << "\n";
  std::cout << "Total points in circle : " << sum << "\n";
  std::cout << "Result : " << std::setprecision(VAL_PRECISION) << pi_value
            << "\n";
  std::cout << "Time taken (in seconds) : " << std::setprecision(TIME_PRECISION)
            << time_taken << "\n";

}

int main(int argc, char *argv[]) {
  // Initialize command line arguments
  cxxopts::Options options("pi_calculation",
                           "Calculate pi using serial and parallel execution");
  options.add_options(
      "custom",
      {
          {"nPoints", "Number of points",
           cxxopts::value<uint>()->default_value(DEFAULT_NUMBER_OF_POINTS)},
          {"nWorkers", "Number of workers",
           cxxopts::value<uint>()->default_value(DEFAULT_NUMBER_OF_WORKERS)},
      });

  auto cl_options = options.parse(argc, argv);
  uint n_points = cl_options["nPoints"].as<uint>();
  uint n_workers = cl_options["nWorkers"].as<uint>();
  std::cout << std::fixed;
  std::cout << "Number of points : " << n_points << "\n";
  std::cout << "Number of workers : " << n_workers << "\n";

  piCalculation(n_points, n_workers);

  return 0;
}
