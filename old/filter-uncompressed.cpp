#include <stdio.h>
#include <fstream>
#include <stdint.h>
#include <cstdlib>
#include <zlib.h>
#include <string>
#include <cstring>

//==================== ROCKSDB ==================//
#define ROCKSDB_8GB_RANGE_START 0x400000
#define ROCKSDB_8GB_RANGE_END 0x648A000
#define ROCKSDB_8GB_RANGE_MMAP_START 0x7FBF51E73000
#define ROCKSDB_8GB_RANGE_MMAP_END 0x7FC130575000

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
#define MEMCACHED_16GB_RANGE_END 0x3C8B54000

#define MEMCACHED_32GB_RANGE_START 0x400000
#define MEMCACHED_32GB_RANGE_END 0x78E8F2000

#define MEMCACHED_64GB_RANGE_START 0x400000
#define MEMCACHED_64GB_RANGE_END 0xF1A9EC000
//=================================================//

//==================== CASSANDRA ==================//
/*
#define CASSANDRA_8GB_RANGE_START 0x400000
#define CASSANDRA_8GB_RANGE_END 0x800000000

#define CASSANDRA_8GB_RANGE_MMAP_START 0x7f7258c02000
#define CASSANDRA_8GB_RANGE_MMAP_END 0x7f72a4355000

#define CASSANDRA_8GB_RANGE_MMAP1_START 0x7f72a5c65000
#define CASSANDRA_8GB_RANGE_MMAP1_END 0x7f72a5c65000

#define CASSANDRA_8GB_RANGE_MMAP2_START 0x7f72a5850000
#define CASSANDRA_8GB_RANGE_MMAP2_END 0x7f72a5a96000
*/

#define CASSANDRA_8GB_RANGE_START 0x400000
#define CASSANDRA_8GB_RANGE_END 0x39D5000
#define CASSANDRA_8GB_RANGE_MMAP_START 0x304B67000
#define CASSANDRA_8GB_RANGE_MMAP_END 0x7C03A5000
#define CASSANDRA_8GB_RANGE_MMAP1_START 0x7FDDFBEA4000
#define CASSANDRA_8GB_RANGE_MMAP1_END 0x7FE2B89B7000
#define CASSANDRA_8GB_RANGE_MMAP2_START 0x7FE2B8BF7000
#define CASSANDRA_8GB_RANGE_MMAP2_END 0x7FE2BA2FF000
#define CASSANDRA_8GB_RANGE_MMAP3_START 0x7FE2BA580000
#define CASSANDRA_8GB_RANGE_MMAP3_END 0x7FE2BC723000
#define CASSANDRA_8GB_RANGE_MMAP4_START 0x7FE2C07E8000
#define CASSANDRA_8GB_RANGE_MMAP4_END 0x7FE2C0E73000
#define CASSANDRA_8GB_RANGE_MMAP5_START 0x7FE2C248F000
#define CASSANDRA_8GB_RANGE_MMAP5_END 0x7FE2C34DF000
#define CASSANDRA_8GB_RANGE_MMAP6_START 0x7FE2C98E4000
#define CASSANDRA_8GB_RANGE_MMAP6_END 0x7FE2DC000000
#define CASSANDRA_8GB_RANGE_MMAP7_START 0x7FE2DC092000
#define CASSANDRA_8GB_RANGE_MMAP7_END 0x7FE2E4000000
#define CASSANDRA_8GB_RANGE_MMAP8_START 0x7FE2E4025000
#define CASSANDRA_8GB_RANGE_MMAP8_END 0x7FE2F8000000
#define CASSANDRA_8GB_RANGE_MMAP9_START 0x7FE2F8013000
#define CASSANDRA_8GB_RANGE_MMAP9_END 0x7FE2F8329000

#define CASSANDRA_16GB_RANGE_START 0x400000
#define CASSANDRA_16GB_RANGE_END 0x3F35000
#define CASSANDRA_16GB_RANGE_MMAP_START 0x304b67000
#define CASSANDRA_16GB_RANGE_MMAP_END 0x800000000
#define CASSANDRA_16GB_RANGE_MMAP1_START 0x7F4958E4D000
#define CASSANDRA_16GB_RANGE_MMAP1_END 0x7F52E5543000
#define CASSANDRA_16GB_RANGE_MMAP2_START 0x7F52E6fA1000
#define CASSANDRA_16GB_RANGE_MMAP2_END 0x7F52E9BDF000
#define CASSANDRA_16GB_RANGE_MMAP3_START 0x7F52EA189000
#define CASSANDRA_16GB_RANGE_MMAP3_END 0x7F52EB57A000
#define CASSANDRA_16GB_RANGE_MMAP4_START 0x7F52ED19A000
#define CASSANDRA_16GB_RANGE_MMAP4_END 0x7F52ED862000
#define CASSANDRA_16GB_RANGE_MMAP5_START 0x7F52EEDFD000
#define CASSANDRA_16GB_RANGE_MMAP5_END 0x7F52EEE1A000
#define CASSANDRA_16GB_RANGE_MMAP6_START 0x7F52EEEA2000
#define CASSANDRA_16GB_RANGE_MMAP6_END 0x7F52EFEF2000
#define CASSANDRA_16GB_RANGE_MMAP7_START 0x7f52f01cb000
#define CASSANDRA_16GB_RANGE_MMAP7_END 0x7F5308000000
#define CASSANDRA_16GB_RANGE_MMAP8_START 0x7f5308012000
#define CASSANDRA_16GB_RANGE_MMAP8_END 0x7F5310000000
#define CASSANDRA_16GB_RANGE_MMAP9_START 0x7f5310025000
#define CASSANDRA_16GB_RANGE_MMAP9_END 0x7F5324000000
#define CASSANDRA_16GB_RANGE_MMAP10_START 0x7F532402A000
#define CASSANDRA_16GB_RANGE_MMAP10_END 0x7F5325051000

#define CASSANDRA_32GB_RANGE_START 0x400000
#define CASSANDRA_32GB_RANGE_END 0x800000000
#define CASSANDRA_32GB_RANGE_MMAP_START 0x7FB28AADF000
#define CASSANDRA_32GB_RANGE_MMAP_END 0x7FBFF252D000
#define CASSANDRA_32GB_RANGE_MMAP1_START 0x7FBFF3E90000
#define CASSANDRA_32GB_RANGE_MMAP1_END 0x7FBFFA7F4000
#define CASSANDRA_32GB_RANGE_MMAP2_START 0x7FBFFC029000
#define CASSANDRA_32GB_RANGE_MMAP2_END 0x7FBFFD07B800
#define CASSANDRA_32GB_RANGE_MMAP3_START 0x7FBFFD32A000
#define CASSANDRA_32GB_RANGE_MMAP3_END 0x7FC014001000
#define CASSANDRA_32GB_RANGE_MMAP4_START 0x7FC01405B000
#define CASSANDRA_32GB_RANGE_MMAP4_END 0x7FC01C000000
#define CASSANDRA_32GB_RANGE_MMAP5_START 0x7FC01C02E000
#define CASSANDRA_32GB_RANGE_MMAP5_END 0x7FC030000000
#define CASSANDRA_32GB_RANGE_MMAP6_START 0x7FC030013000
#define CASSANDRA_32GB_RANGE_MMAP6_END 0x7FC0321F0000
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

//=================== TPCH =====================// #FIXME: Fix range
#define TPCH_8GB_RANGE_START 0x400000
#define TPCH_8GB_RANGE_END 0x1be60000
#define TPCH_8GB_RANGE_MMAP_START 0x7f3738f05000
#define TPCH_8GB_RANGE_MMAP_END 0x7f3973d1f000

#define TPCH_32GB_RANGE_START 0x400000
#define TPCH_32GB_RANGE_END 0x177F0000
#define TPCH_32GB_RANGE_MMAP_START 0x7EF3DAECF000
#define TPCH_32GB_RANGE_MMAP_END 0x7EFE3387B000
#define TPCH_32GB_RANGE_MMAP1_START 0x7EFE33C0B000
#define TPCH_32GB_RANGE_MMAP1_END 0x7EFE39745000
#define TPCH_32GB_RANGE_MMAP2_START 0x7EFE39CBA000
#define TPCH_32GB_RANGE_MMAP2_END 0x7EFE3D44B000
#define TPCH_32GB_RANGE_MMAP3_START 0x7EFE3D4CF000
#define TPCH_32GB_RANGE_MMAP3_END 0x7EFE41E43000

//=================================================//
//
//=================== TPCDS =====================//
#define TPCDS_8GB_RANGE_START 0x400000
#define TPCDS_8GB_RANGE_END 0x18E20000
#define TPCDS_8GB_RANGE_MMAP_START 0x7FDC9764D000
#define TPCDS_8GB_RANGE_MMAP_END 0x7FDD8125D000
#define TPCDS_8GB_RANGE_MMAP1_START 0x7FDD8269D000
#define TPCDS_8GB_RANGE_MMAP1_END 0x7FDDA921D000
#define TPCDS_8GB_RANGE_MMAP2_START 0x7FDDA948D000
#define TPCDS_8GB_RANGE_MMAP2_END 0x7FDE6C291000

#define TPCDS_16GB_RANGE_START 0x400000
#define TPCDS_16GB_RANGE_END 0x19DBE000
#define TPCDS_16GB_RANGE_MMAP_START 0x7F6743B8A000
#define TPCDS_16GB_RANGE_MMAP_END 0x7F67885B9000
#define TPCDS_16GB_RANGE_MMAP1_START 0x7F67890ED000
#define TPCDS_16GB_RANGE_MMAP1_END 0x7F67F2BCD000
#define TPCDS_16GB_RANGE_MMAP2_START 0x7F67F2E1D000
#define TPCDS_16GB_RANGE_MMAP2_END 0x7F693861D000
#define TPCDS_16GB_RANGE_MMAP3_START 0x7F693883D000
#define TPCDS_16GB_RANGE_MMAP3_END 0x7F6A0FE72000

#define TPCDS_32GB_RANGE_START 0x400000
#define TPCDS_32GB_RANGE_END 0x1ABA8000
#define TPCDS_32GB_RANGE_MMAP_START 0x7F59F6CE7000
#define TPCDS_32GB_RANGE_MMAP_END 0x7F5DC6F34000
#define TPCDS_32GB_RANGE_MMAP1_START 0x7F5DC72C8000
#define TPCDS_32GB_RANGE_MMAP1_END 0x7F5DC7E48000
#define TPCDS_32GB_RANGE_MMAP2_START 0x7F5DC8818000
#define TPCDS_32GB_RANGE_MMAP2_END 0x7F5DD5EA8000
#define TPCDS_32GB_RANGE_MMAP3_START 0x7F5DD61A8000
#define TPCDS_32GB_RANGE_MMAP3_END 0x7F5FA710D000
//=================================================//

//==================== MYSQL ======================//
#define MYSQL_RANGE_START 0x400000
#define MYSQL_RANGE_END 0x3A318000

#define MYSQL_RANGE_MMAP_START 0x7f3f16225000
#define MYSQL_RANGE_MMAP_END 0x7f40e1429000
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
  FILE *input; // Input file (compressed)
  FILE *output; // Output file (compressed)

  int status;
  uint64_t i= 0;
  int workload, size;
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

      //start = ROCKSDB_RANGE_START;
      //end = ROCKSDB_RANGE_END;
      //mmap_s = ROCKSDB_RANGE_MMAP_START;
      //mmap_e = ROCKSDB_RANGE_MMAP_END;
      break;
    case TPCH:
      strcat(pinatrace, "tpch-");      
      strcat(filtered, "tpch-");      

      strcpy(str_workload, "TPC-H");
      str_workload[strlen("TPC-H")] = '\0';

      //start = MONETDB_RANGE_START;
      //end = MONETDB_RANGE_END;
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
      strcat(filtered, "mysq-");

      strcpy(str_workload, "MySQL");
      str_workload[strlen("MySQL")] = '\0';

      //start = MYSQL_RANGE_START;
      //end = MYSQL_RANGE_END;
      //mmap_s = MYSQL_RANGE_MMAP_START;
      //mmap_e = MYSQL_RANGE_MMAP_END;
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

  input = fopen(pinatrace, "rb");
  output = fopen(filtered, "w");

  printf("Pinatrace: %s\n", pinatrace); 
  printf("Filtered: %s\n", filtered); 

  //exit(-1);
  
  if (input == NULL){
      printf("Error: Cannot open trace file for %s.\n", str_workload);
      fclose(input);
      fclose(output);
      exit(-1);
  }

  if (output == NULL){
      printf("Error: Cannot create output file for %s.\n", str_workload);
      fclose(input);
      fclose(output);
      exit(-1);
  }

  while (!feof(input)){
    int status;
    compressedRecord msg;

    status=fread(&msg, sizeof(compressedRecord), 1, input);

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
    else if ((msg.addr >= mmap5_s) && (msg.addr < mmap5_e))
      print = true;
    else if ((msg.addr >= mmap6_s) && (msg.addr < mmap6_e))
      print = true;
    else if ((msg.addr >= mmap7_s) && (msg.addr < mmap7_e))
      print = true;
    else if ((msg.addr >= mmap8_s) && (msg.addr < mmap8_e))
      print = true;
    else if ((msg.addr >= mmap9_s) && (msg.addr < mmap9_e))
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
      fwrite(&msg, sizeof(compressedRecord), 1, output);
      print = false;
    }
      
  }

  printf("Filtering completed: %lld records processed in total.\n", i);

  fclose(input);
  fclose(output);

  return 0;
}
