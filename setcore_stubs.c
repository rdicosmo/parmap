#include <stdio.h>
#include <unistd.h>
//#define _GNU_SOURCE             /* See feature_test_macros(7) */
#include <sched.h>
#include <errno.h>
#include <caml/mlvalues.h>
#include "config.h"

CAMLprim value setcore(value which) {
  int numcores = sysconf( _SC_NPROCESSORS_ONLN );
  int w = Int_val(which) % numcores; // stay in the space of existing cores
  cpu_set_t cpus;   
  int retcode;
  int finished=0;
  while (finished==0)
    {
      CPU_ZERO(&cpus); 
      CPU_SET (w,&cpus);
      //fprintf(stderr,"Trying to pin to cpu %d out of %d reported by the system\n",w,numcores);
#ifdef HAVE_DECL_SCHED_SETAFFINITY
      retcode = sched_setaffinity(getpid(), sizeof(cpu_set_t), &cpus);
      if(retcode != 0) {
	fprintf(stderr,"Failed pinning to cpu %d, trying %d/2\n",w, w); 
	w=w/2;
      }
      else 
	{ //fprintf(stderr,"Succeeded pinning to cpu %d\n",w); 
	  finished=1;
	}
#else
      finished=1;
#endif
    }
  return Val_unit;
}
