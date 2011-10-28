#include <stdio.h>
#include <unistd.h>
//#define _GNU_SOURCE             /* See feature_test_macros(7) */
#include <sched.h>
#include <errno.h>
#include <caml/mlvalues.h>

CAMLprim value setcore(value which) {
  int numCPU = sysconf( _SC_NPROCESSORS_ONLN );
  int w = Int_val(which) % numCPU; // stay in the space of existing cores
  cpu_set_t cpus;   
  int retcode; 
  fprintf(stderr,"pinning to cpu %d out of %d\n",w,numCPU);
  CPU_ZERO(&cpus); 
  CPU_SET (which,&cpus);
  retcode = sched_setaffinity(getpid(), sizeof(cpu_set_t), &cpus);
  if(retcode != 0) {
    fprintf(stderr,"error in pinning to cpu %d",w); 
  }
  return Val_unit;
}
