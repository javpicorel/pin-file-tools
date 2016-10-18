#include <stdio.h>
#include <fstream>
#include <stdint.h>
#include <cstdlib>
#include <zlib.h>
#include <string>
#include <cstring>

#include "ranges.h"
#include "workloads.h"
#include "scale.h"
#include "op.h"

//#define DEBUG 0

#define DEBUG_RECORDS 1000000

// Compressed record type
struct compressedRecord{   
  char op;
  uint64_t addr;
  uint32_t idx;
};

int main(int argc, char *argv[])
{
  gzFile input; // Input file (compressed)
  gzFile output; // Output file (compressed)

  int status;
  uint64_t i= 0;
  int workload, size, op;
  char str_workload[80];
  char path[1024];
  char pinatrace[1024];
  char filtered[1024];
  bool print = false;
  uint64_t start = 0, end = 0;
  uint64_t mmap_s = 0, mmap_e = 0;
  uint64_t mmap1_s = 0, mmap1_e = 0;
  uint64_t mmap2_s = 0, mmap2_e = 0;
  uint64_t mmap3_s = 0, mmap3_e = 0;
  uint64_t mmap4_s = 0, mmap4_e = 0;
  uint64_t mmap5_s = 0, mmap5_e = 0;
  uint64_t mmap6_s = 0, mmap6_e = 0;
  uint64_t mmap7_s = 0, mmap7_e = 0;
  uint64_t mmap8_s = 0, mmap8_e = 0;
  uint64_t mmap9_s = 0, mmap9_e = 0;
  uint64_t mmap10_s = 0, mmap10_e = 0;

  //Wrong number of argument
  if (argc != 5){
    printf("===============================================\n");
    printf("You need to specify a workload number to filter\n");
    printf("===============================================\n");
    printf("0 - Memcached\n");
    printf("1 - RocksDB\n");
    printf("2 - MySQL\n");
    printf("3 - TPC-H\n");
    printf("4 - TPC-DS\n");
    printf("5 - CassandraA\n");
    printf("6 - Neo4j\n");
    printf("7 - Test\n");
    printf("===============================================\n");
    printf("You need to specify the size of the dataset\n");
    printf("===============================================\n");
    printf("8 - 8GB\n");
    printf("16 - 16GB\n");
    printf("32 - 32GB\n");
    printf("64 - 64GB\n");
    printf("===============================================\n");
    printf("You need to specify a path to locate the traces\n");
    printf("===============================================\n");
    printf("===============================================\n");
    printf("You need to specify an operation to perform    \n");
    printf("===============================================\n");
    printf("0 - Count\n");
    printf("1 - Filter\n");
    exit(-1);
  }

  //Read the workload number
  sscanf(argv[1], "%d", &workload);
  //Read the size of dataset
  sscanf(argv[2], "%d", &size);
  //Read the path 
  sscanf(argv[3], "%s", path);
  //Read the operation number
  sscanf(argv[4], "%d", &op);

  strcpy(pinatrace, path);
  strcat(pinatrace, "pinatrace-");

  strcpy(filtered, pinatrace);
  strcat(filtered, "filtered-");

  switch(workload){
    case ROCKSDB: 
      strcat(pinatrace, "rocksdb-");      
      strcat(filtered, "rocksdb-");      

      strcpy(str_workload, "RocksDB");
      str_workload[strlen("RocksDB")] = '\0';

      switch (size){
        case S8GB:
          start = ROCKSDB_8GB_RANGE_START;
          end = ROCKSDB_8GB_RANGE_END;
          mmap_s = ROCKSDB_8GB_RANGE_MMAP_START;
          mmap_e = ROCKSDB_8GB_RANGE_MMAP_END;
          break;
        case S16GB:
          start = ROCKSDB_16GB_RANGE_START;
          end = ROCKSDB_16GB_RANGE_END;
          mmap_s = ROCKSDB_16GB_RANGE_MMAP_START;
          mmap_e = ROCKSDB_16GB_RANGE_MMAP_END;
          break;
        case S32GB:
          start = ROCKSDB_32GB_RANGE_START;
          end = ROCKSDB_32GB_RANGE_END;
          mmap_s = ROCKSDB_32GB_RANGE_MMAP_START;
          mmap_e = ROCKSDB_32GB_RANGE_MMAP_END;
          break;
        case S64GB:
          start = ROCKSDB_64GB_RANGE_START;
          end = ROCKSDB_64GB_RANGE_END;
          mmap_s = ROCKSDB_64GB_RANGE_MMAP_START;
          mmap_e = ROCKSDB_64GB_RANGE_MMAP_END;
          break;

        default:
          printf("Error size not properly selected, exiting...\n");
          return 0;
      }

      break;
    case TPCH:
      strcat(pinatrace, "tpch-");      
      strcat(filtered, "tpch-");      

      strcpy(str_workload, "TPC-H");
      str_workload[strlen("TPC-H")] = '\0';

      switch (size){
        case S8GB:
          start = TPCH_8GB_RANGE_START;
          end = TPCH_8GB_RANGE_END;
          mmap_s = TPCH_8GB_RANGE_MMAP_START;
          mmap_e = TPCH_8GB_RANGE_MMAP_END;
          mmap1_s = TPCH_8GB_RANGE_MMAP1_START;
          mmap1_e = TPCH_8GB_RANGE_MMAP1_END;
          mmap2_s = TPCH_8GB_RANGE_MMAP2_START;
          mmap2_e = TPCH_8GB_RANGE_MMAP2_END;
          break;
        case S16GB:
          start = TPCH_16GB_RANGE_START;
          end = TPCH_16GB_RANGE_END;
          mmap_s = TPCH_16GB_RANGE_MMAP_START;
          mmap_e = TPCH_16GB_RANGE_MMAP_END;
          mmap1_s = TPCH_16GB_RANGE_MMAP1_START;
          mmap1_e = TPCH_16GB_RANGE_MMAP1_END;
          break;
        case S32GB:
          start = TPCH_32GB_RANGE_START;
          end = TPCH_32GB_RANGE_END;
          mmap_s = TPCH_32GB_RANGE_MMAP_START;
          mmap_e = TPCH_32GB_RANGE_MMAP_END;
          mmap1_s = TPCH_32GB_RANGE_MMAP1_START;
          mmap1_e = TPCH_32GB_RANGE_MMAP1_END;
          mmap2_s = TPCH_32GB_RANGE_MMAP2_START;
          mmap2_e = TPCH_32GB_RANGE_MMAP2_END;
          mmap3_s = TPCH_32GB_RANGE_MMAP3_START;
          mmap3_e = TPCH_32GB_RANGE_MMAP3_END;
          break;
        default:
          printf("Error size not properly selected, exiting...\n");
          return 0;
      }    

      break;
    case TPCDS:
      strcat(pinatrace, "tpcds-");      
      strcat(filtered, "tpcds-");      

      strcpy(str_workload, "TPC-DS");
      str_workload[strlen("TPC-DS")] = '\0';

      switch (size){
        case S8GB:
          start = TPCDS_8GB_RANGE_START;
          end = TPCDS_8GB_RANGE_END;
          mmap_s = TPCDS_8GB_RANGE_MMAP_START;
          mmap_e = TPCDS_8GB_RANGE_MMAP_END;
          mmap1_s = TPCDS_8GB_RANGE_MMAP1_START;
          mmap1_e = TPCDS_8GB_RANGE_MMAP1_END;
          mmap2_s = TPCDS_8GB_RANGE_MMAP2_START;
          mmap2_e = TPCDS_8GB_RANGE_MMAP2_END;
          break;
        case S16GB:
          start = TPCDS_16GB_RANGE_START;
          end = TPCDS_16GB_RANGE_END;
          mmap_s = TPCDS_16GB_RANGE_MMAP_START;
          mmap_e = TPCDS_16GB_RANGE_MMAP_END;
          mmap1_s = TPCDS_16GB_RANGE_MMAP1_START;
          mmap1_e = TPCDS_16GB_RANGE_MMAP1_END;
          mmap2_s = TPCDS_16GB_RANGE_MMAP2_START;
          mmap2_e = TPCDS_16GB_RANGE_MMAP2_END;
          mmap3_s = TPCDS_16GB_RANGE_MMAP3_START;
          mmap3_e = TPCDS_16GB_RANGE_MMAP3_END;
          break;
        case S32GB:
          start = TPCDS_32GB_RANGE_START;
          end = TPCDS_32GB_RANGE_END;
          mmap_s = TPCDS_32GB_RANGE_MMAP_START;
          mmap_e = TPCDS_32GB_RANGE_MMAP_END;
          mmap1_s = TPCDS_32GB_RANGE_MMAP1_START;
          mmap1_e = TPCDS_32GB_RANGE_MMAP1_END;
          mmap2_s = TPCDS_32GB_RANGE_MMAP2_START;
          mmap2_e = TPCDS_32GB_RANGE_MMAP2_END;
          mmap3_s = TPCDS_32GB_RANGE_MMAP3_START;
          mmap3_e = TPCDS_32GB_RANGE_MMAP3_END;
          break;
        case S64GB:
          break;
        default:
          printf("Error size not properly selected, exiting...\n");
          return 0;
      }

      break;
    case MEMCACHED:
      strcat(pinatrace, "memcached-");      
      strcat(filtered, "memcached-");

      strcpy(str_workload, "Memcached");
      str_workload[strlen("Memcached")] = '\0';

      switch (size){
        case S8GB:
          start = MEMCACHED_8GB_RANGE_START;
          end = MEMCACHED_8GB_RANGE_END;
          break;
        case S16GB:
          start = MEMCACHED_16GB_RANGE_START;
          end = MEMCACHED_16GB_RANGE_END;
          break;
        case S32GB:
          start = MEMCACHED_32GB_RANGE_START;
          end = MEMCACHED_32GB_RANGE_END;
          break;
        case S64GB:
          start = MEMCACHED_64GB_RANGE_START;
          end = MEMCACHED_64GB_RANGE_END;
          break;

        default:
          printf("Error size not properly selected, exiting...\n");
          return 0;
      }

      break;
    case CASSANDRA:
      strcat(pinatrace, "cassandra-");      
      strcat(filtered, "cassandra-");

      strcpy(str_workload, "Cassandra");
      str_workload[strlen("Cassandra")] = '\0';

      switch (size){
        case S8GB:
          start = CASSANDRA_8GB_RANGE_START;
          end = CASSANDRA_8GB_RANGE_END;
          mmap_s = CASSANDRA_8GB_RANGE_MMAP_START;
          mmap_e = CASSANDRA_8GB_RANGE_MMAP_END;
          mmap1_s = CASSANDRA_8GB_RANGE_MMAP1_START;
          mmap1_e = CASSANDRA_8GB_RANGE_MMAP1_END;
          mmap2_s = CASSANDRA_8GB_RANGE_MMAP2_START;
          mmap2_e = CASSANDRA_8GB_RANGE_MMAP2_END;
          mmap3_s = CASSANDRA_8GB_RANGE_MMAP3_START;
          mmap3_e = CASSANDRA_8GB_RANGE_MMAP3_END;
          mmap4_s = CASSANDRA_8GB_RANGE_MMAP4_START;
          mmap4_e = CASSANDRA_8GB_RANGE_MMAP4_END;
          mmap5_s = CASSANDRA_8GB_RANGE_MMAP5_START;
          mmap5_e = CASSANDRA_8GB_RANGE_MMAP5_END;
          mmap6_s = CASSANDRA_8GB_RANGE_MMAP6_START;
          mmap6_e = CASSANDRA_8GB_RANGE_MMAP6_END;
          mmap7_s = CASSANDRA_8GB_RANGE_MMAP7_START;
          mmap7_e = CASSANDRA_8GB_RANGE_MMAP7_END;
          mmap8_s = CASSANDRA_8GB_RANGE_MMAP8_START;
          mmap8_e = CASSANDRA_8GB_RANGE_MMAP8_END;
          mmap9_s = CASSANDRA_8GB_RANGE_MMAP9_START;
          mmap9_e = CASSANDRA_8GB_RANGE_MMAP9_END;
          break;
       case S16GB:
          start = CASSANDRA_16GB_RANGE_START;
          end = CASSANDRA_16GB_RANGE_END;
          mmap_s = CASSANDRA_16GB_RANGE_MMAP_START;
          mmap_e = CASSANDRA_16GB_RANGE_MMAP_END;
          mmap1_s = CASSANDRA_16GB_RANGE_MMAP1_START;
          mmap1_e = CASSANDRA_16GB_RANGE_MMAP1_END;
          mmap2_s = CASSANDRA_16GB_RANGE_MMAP2_START;
          mmap2_e = CASSANDRA_16GB_RANGE_MMAP2_END;
          mmap3_s = CASSANDRA_16GB_RANGE_MMAP3_START;
          mmap3_e = CASSANDRA_16GB_RANGE_MMAP3_END;
          mmap4_s = CASSANDRA_16GB_RANGE_MMAP4_START;
          mmap4_e = CASSANDRA_16GB_RANGE_MMAP4_END;
          mmap5_s = CASSANDRA_16GB_RANGE_MMAP5_START;
          mmap5_e = CASSANDRA_16GB_RANGE_MMAP5_END;
          mmap6_s = CASSANDRA_16GB_RANGE_MMAP6_START;
          mmap6_e = CASSANDRA_16GB_RANGE_MMAP6_END;
          mmap7_s = CASSANDRA_16GB_RANGE_MMAP7_START;
          mmap7_e = CASSANDRA_16GB_RANGE_MMAP7_END;
          mmap8_s = CASSANDRA_16GB_RANGE_MMAP8_START;
          mmap8_e = CASSANDRA_16GB_RANGE_MMAP8_END;
          mmap9_s = CASSANDRA_16GB_RANGE_MMAP9_START;
          mmap9_e = CASSANDRA_16GB_RANGE_MMAP9_END;
          mmap10_s = CASSANDRA_16GB_RANGE_MMAP10_START;
          mmap10_e = CASSANDRA_16GB_RANGE_MMAP10_END;
         break;
       case S32GB:
          start = CASSANDRA_32GB_RANGE_START;
          end = CASSANDRA_32GB_RANGE_END;
          mmap_s = CASSANDRA_32GB_RANGE_MMAP_START;
          mmap_e = CASSANDRA_32GB_RANGE_MMAP_END;
          mmap1_s = CASSANDRA_32GB_RANGE_MMAP1_START;
          mmap1_e = CASSANDRA_32GB_RANGE_MMAP1_END;
          mmap2_s = CASSANDRA_32GB_RANGE_MMAP2_START;
          mmap2_e = CASSANDRA_32GB_RANGE_MMAP2_END;
          mmap3_s = CASSANDRA_32GB_RANGE_MMAP3_START;
          mmap3_e = CASSANDRA_32GB_RANGE_MMAP3_END;
          mmap4_s = CASSANDRA_32GB_RANGE_MMAP4_START;
          mmap4_e = CASSANDRA_32GB_RANGE_MMAP4_END;
          mmap5_s = CASSANDRA_32GB_RANGE_MMAP5_START;
          mmap5_e = CASSANDRA_32GB_RANGE_MMAP5_END;
          mmap6_s = CASSANDRA_32GB_RANGE_MMAP6_START;
          mmap6_e = CASSANDRA_32GB_RANGE_MMAP6_END;
         break;
       default:
          printf("Error size not properly selected, exiting...\n");
          return 0; 
      }
   
      break;
    case NEO4J:
      strcat(pinatrace, "neo4j-");      
      strcat(filtered, "neo4j-");

      strcpy(str_workload, "Neo4j");
      str_workload[strlen("Neo4j")] = '\0';

      start = NEO4J_8GB_RANGE_START;
      end = NEO4J_8GB_RANGE_END;
      mmap_s = NEO4J_8GB_RANGE_MMAP_START;
      mmap_e = NEO4J_8GB_RANGE_MMAP_END;
      mmap1_s = NEO4J_8GB_RANGE_MMAP1_START;
      mmap1_e = NEO4J_8GB_RANGE_MMAP1_END;
      mmap2_s = NEO4J_8GB_RANGE_MMAP2_START;
      mmap2_e = NEO4J_8GB_RANGE_MMAP2_END;
      mmap3_s = NEO4J_8GB_RANGE_MMAP3_START;
      mmap3_e = NEO4J_8GB_RANGE_MMAP3_END;
      mmap4_s = NEO4J_8GB_RANGE_MMAP4_START;
      mmap4_e = NEO4J_8GB_RANGE_MMAP4_END;
      mmap5_s = NEO4J_8GB_RANGE_MMAP5_START;
      mmap5_e = NEO4J_8GB_RANGE_MMAP5_END;
      mmap6_s = NEO4J_8GB_RANGE_MMAP6_START;
      mmap6_e = NEO4J_8GB_RANGE_MMAP6_END;
      mmap7_s = NEO4J_8GB_RANGE_MMAP7_START;
      mmap7_e = NEO4J_8GB_RANGE_MMAP7_END;
      mmap8_s = NEO4J_8GB_RANGE_MMAP8_START;
      mmap8_e = NEO4J_8GB_RANGE_MMAP8_END;
      mmap9_s = NEO4J_8GB_RANGE_MMAP9_START;
      mmap9_e = NEO4J_8GB_RANGE_MMAP9_END;
      break;
    case MYSQL:
      strcat(pinatrace, "mysql-");      
      strcat(filtered, "mysq-");

      strcpy(str_workload, "MySQL");
      str_workload[strlen("MySQL")] = '\0';

      start = MYSQL_8GB_RANGE_START;
      end = MYSQL_8GB_RANGE_END;
      mmap_s = MYSQL_8GB_RANGE_MMAP_START;
      mmap_e = MYSQL_8GB_RANGE_MMAP_END;
      mmap1_s = MYSQL_8GB_RANGE_MMAP1_START;
      mmap1_e = MYSQL_8GB_RANGE_MMAP1_END;
      mmap2_s = MYSQL_8GB_RANGE_MMAP2_START;
      mmap2_e = MYSQL_8GB_RANGE_MMAP2_END;

      break;
    default: 
      strcat(pinatrace, "test-");      
      strcat(filtered, "test-");

      strcpy(str_workload, "Test");
      str_workload[strlen("Test")] = '\0';
  }

  switch (size){
      case S8GB:
        strcat(pinatrace, "8gb");
        strcat(filtered, "8gb");
        break;
      case S16GB:
        strcat(pinatrace, "16gb");
        strcat(filtered, "16gb");
        break;
      case S32GB:
        strcat(pinatrace, "32gb");
        strcat(filtered, "32gb");
        break;
      case S64GB:
        strcat(pinatrace, "64gb");
        strcat(filtered, "64gb");
        break;

      default:
        printf("Error size not properly selected, exiting...\n");
        return 0;
  }

  strcat(pinatrace, ".out");
  strcat(filtered, ".out");

  input = gzopen(pinatrace, "rb");
  if (op != COUNT){
    output = gzopen(filtered, "ab9f");
  }

  printf("Pinatrace: %s\n", pinatrace); 
  if (op != COUNT){
    printf("Filtered: %s\n", filtered); 
  }

  //exit(-1);
  
  if (input == Z_NULL){
      printf("Error: Cannot open trace file for %s.\n", str_workload);
      gzclose(input);
      if (op != COUNT){
        gzclose(output);
      }
      exit(-1);
  }

  if (op != COUNT){
    if (output == Z_NULL){
      printf("Error: Cannot create output file for %s.\n", str_workload);
      gzclose(input);
      gzclose(output);
      exit(-1);
    }
  }

  while (!gzeof(input)){
    int status;
    compressedRecord msg;

    status=gzread(input, &msg, sizeof(compressedRecord));

    if (!status){
      printf("Error: gzread did not return zero!\n");
      exit(-1);;
    }

    if (!(i % 10000000)){
      printf("Filtering status: %lld records processed so far..., record:[%llp,%c,%d]\n", i, msg.addr, msg.op, msg.idx);
    }

    i++;


    #ifdef DEBUG
    if (i > DEBUG_RECORDS){
      printf("Filtering completed: %lld records processed in total.\n", i);
      return 0;
    }
    #endif

    if ((msg.addr >= start) && (msg.addr < end))
      print = true;
    else if ((msg.addr >= mmap_s) && (msg.addr < mmap_e))
      print = true;
    else if ((msg.addr >= mmap1_s) && (msg.addr < mmap1_e))
      print = true;
    else if ((msg.addr >= mmap2_s) && (msg.addr < mmap2_e))
      print = true;
    else if ((msg.addr >= mmap3_s) && (msg.addr < mmap3_e))
      print = true;
    else if ((msg.addr >= mmap4_s) && (msg.addr < mmap4_e))
      print = true;
    else{
      #ifdef DEBUG
        printf("Not part of segment: %c %p\n", msg.op, msg.addr);
      #endif
    }

    if (print){
    #ifdef DEBUG
      printf("%c %p\n", msg.op, msg.addr);
    #endif
      if (op != COUNT){
        gzwrite(output, &msg, sizeof(compressedRecord));
      }
      print = false;
    }
      
  }

  printf("Filtering completed: %lld records processed in total.\n", i);

  gzclose(input);
  if (op != COUNT){
    gzclose(output);
  }

  return 0;
}
