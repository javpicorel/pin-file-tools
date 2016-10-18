#include <stdio.h>
#include <fstream>
#include <stdint.h>
#include <cstdlib>
#include <zlib.h>
#include <string>
#include <cstring>

//==================== ROCKSDB ==================//
#define ROCKSDB_8GB_RANGE_START 0x400000
#define ROCKSDB_8GB_RANGE_END 0x2B1C000
#define ROCKSDB_8GB_RANGE_MMAP_START 0x7FEF24B97000
#define ROCKSDB_8GB_RANGE_MMAP_END 0x7FF1068A6000

#define ROCKSDB_16GB_RANGE_START 0x400000
#define ROCKSDB_16GB_RANGE_END 0x616C000
#define ROCKSDB_16GB_RANGE_MMAP_START 0x7F026CD60000
#define ROCKSDB_16GB_RANGE_MMAP_END 0x7F0625962000

#define ROCKSDB_32GB_RANGE_START 0x400000
#define ROCKSDB_32GB_RANGE_END 0x57F0000
#define ROCKSDB_32GB_RANGE_MMAP_START 0x7F02593CB000 
#define ROCKSDB_32GB_RANGE_MMAP_END 0x7F09D251D000

#define ROCKSDB_64GB_RANGE_START 0x400000
#define ROCKSDB_64GB_RANGE_END 0x62A4000
#define ROCKSDB_64GB_RANGE_MMAP_START 0x7F6206CF0000
#define ROCKSDB_64GB_RANGE_MMAP_END 0x7F70E1B0E000
//===============================================//

//==================== MEMCACHED ==================//
#define MEMCACHED_8GB_RANGE_START 0x400000
#define MEMCACHED_8GB_RANGE_END 0x1E4C6E000

#define MEMCACHED_16GB_RANGE_START 0x400000
#define MEMCACHED_16GB_RANGE_END 0x3C71C8000

#define MEMCACHED_32GB_RANGE_START 0x400000
#define MEMCACHED_32GB_RANGE_END 0x78E8F2000

#define MEMCACHED_64GB_RANGE_START 0x400000
#define MEMCACHED_64GB_RANGE_END 0xF1A9EC000
//=================================================//

//==================== CASSANDRA ==================//
#define CASSANDRA_8GB_RANGE_START 0x400000
#define CASSANDRA_8GB_RANGE_END 0x800000000

#define CASSANDRA_8GB_RANGE_MMAP_START 0x7f7258c02000
#define CASSANDRA_8GB_RANGE_MMAP_END 0x7f72a4355000

#define CASSANDRA_8GB_RANGE_MMAP1_START 0x7f72a5c65000
#define CASSANDRA_8GB_RANGE_MMAP1_END 0x7f72a5c65000

#define CASSANDRA_8GB_RANGE_MMAP2_START 0x7f72a5850000
#define CASSANDRA_8GB_RANGE_MMAP2_END 0x7f72a5a96000
//=================================================//

//==================== NEO4J ======================//
#define NEO4J_RANGE_START 0x400000
#define NEO4J_RANGE_END 0x800000000

#define NEO4J_RANGE_MMAP_START 0x7f6c60000000
#define NEO4J_RANGE_MMAP_END 0x7f6da26f1000

#define NEO4J_RANGE_MMAP1_START 0x7f6da84bc000
#define NEO4J_RANGE_MMAP1_END 0x7f6db0955000

#define NEO4J_RANGE_MMAP2_START 0x7f6db21e2000
#define NEO4J_RANGE_MMAP2_END 0x7f6dd8000000

#define NEO4J_RANGE_MMAP3_START 0x7f6dd8071000
#define NEO4J_RANGE_MMAP3_END 0x7f6df0000000

#define NEO4J_RANGE_MMAP4_START 0x7f6df00cf000
#define NEO4J_RANGE_MMAP4_END 0x7f6df154c000
//=================================================//

//=================== MONETDB =====================//
#define MONETDB_RANGE_START 0x400000
#define MONETDB_RANGE_END 0x1be60000

#define MONETDB_RANGE_MMAP_START 0x7f3738f05000
#define MONETDB_RANGE_MMAP_END 0x7f3973d1f000
//=================================================//

//==================== MYSQL ======================//
#define MYSQL_RANGE_START 0x400000
#define MYSQL_RANGE_END 0x3A318000

//#define MYSQL_RANGE_MMAP_START 0x7f3f16225000
//#define MYSQL_RANGE_MMAP_END 0x7f40e1429000
//=================================================//

#define MEMCACHED 0
#define ROCKSDB 1
#define MYSQL 2
#define TPCH 3
#define TPCDS 4
#define CASSANDRA 5
#define NEO4J 6
#define TEST 7

#define S8GB 8
#define S16GB 16
#define S32GB 32
#define S64GB 64

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

  int status;
  uint64_t i= 0;
  int workload, size;
  char str_workload[80];
  char path[1024];
  char pinatrace[1024];
  bool print = false;
  uint64_t start = 0, end = 0;
  uint64_t mmap_s = 0, mmap_e = 0;
  uint64_t mmap1_s = 0, mmap1_e = 0;
  uint64_t mmap2_s = 0, mmap2_e = 0;
  uint64_t mmap3_s = 0, mmap3_e = 0;
  uint64_t mmap4_s = 0, mmap4_e = 0;

  //Wrong number of argument
  if (argc != 4){
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
    exit(-1);
  }

  //Read the workload number
  sscanf(argv[1], "%d", &workload);
  //Read the size of dataset
  sscanf(argv[2], "%d", &size);
  //Read the path 
  sscanf(argv[3], "%s", path);

  strcpy(pinatrace, path);
  strcat(pinatrace, "pinatrace-filtered-");

  switch(workload){
    case ROCKSDB: 
      strcat(pinatrace, "rocksdb-");      

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

      //start = ROCKSDB_RANGE_START;
      //end = ROCKSDB_RANGE_END;
      //mmap_s = ROCKSDB_RANGE_MMAP_START;
      //mmap_e = ROCKSDB_RANGE_MMAP_END;
      break;
    case TPCH:
      strcat(pinatrace, "tpch-");      

      strcpy(str_workload, "TPC-H");
      str_workload[strlen("TPC-H")] = '\0';

      //start = MONETDB_RANGE_START;
      //end = MONETDB_RANGE_END;
      break;
    case TPCDS:
      strcat(pinatrace, "tpcds-");      

      strcpy(str_workload, "TPC-DS");
      str_workload[strlen("TPC-DS")] = '\0';

      //start = MONETDB_RANGE_START;
      //end = MONETDB_RANGE_END;
      break;
    case MEMCACHED:
      strcat(pinatrace, "memcached-");      

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

      //start = MEMCACHED_RANGE_START;
      //end = MEMCACHED_RANGE_END;
      break;
    case CASSANDRA:
      strcat(pinatrace, "cassandra-");      

      strcpy(str_workload, "Cassandra");
      str_workload[strlen("Cassandra")] = '\0';

      //start = CASSANDRA_RANGE_START;
      //end = CASSANDRA_RANGE_END;
      //mmap_s = CASSANDRA_RANGE_MMAP_START;
      //mmap_e = CASSANDRA_RANGE_MMAP_END;
      break;
    case NEO4J:
      strcat(pinatrace, "neo4j-");      

      strcpy(str_workload, "Neo4j");
      str_workload[strlen("Neo4j")] = '\0';

      //start = NEO4J_RANGE_START;
      //end = NEO4J_RANGE_END;
      //mmap_s = NEO4J_RANGE_MMAP_START;
      //mmap_e = NEO4J_RANGE_MMAP_END;
      //mmap1_s = NEO4J_RANGE_MMAP1_START;
      //mmap1_e = NEO4J_RANGE_MMAP1_END;
      //mmap2_s = NEO4J_RANGE_MMAP2_START;
      //mmap2_e = NEO4J_RANGE_MMAP2_END;
      //mmap3_s = NEO4J_RANGE_MMAP3_START;
      //mmap3_e = NEO4J_RANGE_MMAP3_END;
      //mmap4_s = NEO4J_RANGE_MMAP4_START;
      //mmap4_e = NEO4J_RANGE_MMAP4_END;
      break;
    case MYSQL:
      strcat(pinatrace, "mysql-");      

      strcpy(str_workload, "MySQL");
      str_workload[strlen("MySQL")] = '\0';

      //start = MYSQL_RANGE_START;
      //end = MYSQL_RANGE_END;
      //mmap_s = MYSQL_RANGE_MMAP_START;
      //mmap_e = MYSQL_RANGE_MMAP_END;
      break;
    default: 
      strcat(pinatrace, "test-");      

      strcpy(str_workload, "Test");
      str_workload[strlen("Test")] = '\0';
  }

  switch (size){
      case S8GB:
        strcat(pinatrace, "8gb");
        break;
      case S16GB:
        strcat(pinatrace, "16gb");
        break;
      case S32GB:
        strcat(pinatrace, "32gb");
        break;
      case S64GB:
        strcat(pinatrace, "64gb");
        break;

      default:
        printf("Error size not properly selected, exiting...\n");
        return 0;
  }

  strcat(pinatrace, ".out");

  input = gzopen(pinatrace, "rb");

  printf("Pinatrace: %s\n", pinatrace); 

  //exit(-1);
  
  if (input == Z_NULL){
      printf("Error: Cannot open trace file for %s.\n", str_workload);
      gzclose(input);
      exit(-1);
  }

  while (!gzeof(input)){
    int status;
    compressedRecord msg;

    status=gzread(input, &msg, sizeof(compressedRecord));

    if (!status){
      printf("Error: gzread did not return zero!\n");
      gzclose(input);
      exit(-1);;
    }

    if (!(i % 10000000)){
      printf("Filtering status: %lld records processed so far..., record:[%llp,%c,%d]\n", i, msg.addr, msg.op, msg.idx);
    }

    i++;


    #ifdef DEBUG
    if (i > DEBUG_RECORDS){
      printf("Filtering completed: %lld records processed in total.\n", i);
      gzclose(input);
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
      print = false;
    }
      
  }

  printf("Filtering completed: %lld records processed in total.\n", i);

  gzclose(input);

  return 0;
}
